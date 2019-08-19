package org.openplacereviews.osm.service;

import static org.openplacereviews.opendb.ops.OpBlockchainRules.F_TYPE;
import static org.openplacereviews.osm.util.ObjectGenerator.generateCreateOpObject;
import static org.openplacereviews.osm.util.ObjectGenerator.generateEditOpForBotObject;
import static org.openplacereviews.osm.util.ObjectGenerator.generateEditOpObject;
import static org.openplacereviews.osm.util.ObjectGenerator.generateHashAndSign;
import static org.openplacereviews.osm.util.ObjectGenerator.initOpOperation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.ops.PerformanceMetrics;
import org.openplacereviews.opendb.ops.PerformanceMetrics.Metric;
import org.openplacereviews.opendb.ops.PerformanceMetrics.PerformanceMetric;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.IOpenDBBot;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.osm.model.DiffEntity;
import org.openplacereviews.osm.model.Entity;
import org.openplacereviews.osm.parser.OsmParser;
import org.openplacereviews.osm.util.ObjectGenerator;
import org.openplacereviews.osm.util.OprUtil;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class OsmSyncBot implements IOpenDBBot<OsmSyncBot> {

	private static final Log LOGGER = LogFactory.getLog(OsmSyncBot.class);
	
	private static final PerformanceMetric mOverpassQuery = PerformanceMetrics.i().getMetric("opr.osm-sync.overpass");
	private static final PerformanceMetric mPublish = PerformanceMetrics.i().getMetric("opr.osm-sync.publish");
	private static final PerformanceMetric mProcDiff = PerformanceMetrics.i().getMetric("opr.osm-sync.proc-diff");
	private static final PerformanceMetric mProcEntity = PerformanceMetrics.i().getMetric("opr.osm-sync.proc-e");
	private static final PerformanceMetric mBlock = PerformanceMetrics.i().getMetric("opr.osm-sync.block");
	private static final PerformanceMetric mOpAdd = PerformanceMetrics.i().getMetric("opr.osm-sync.opadd");

	public static final String ATTR_SOURCE = "source";
	public static final String ATTR_OLD_OSM_IDS = "old-osm-ids";
	public static final String ATTR_OSM = "osm";
	public static final String ATTR_TAGS = "tags";
	public static final String ATTR_SET = "set";
	public static final String ATTR_SOURCE_OSM_TAGS = "source.osm[0].tags.";

	
	public static final String F_CONFIG = "config";
	public static final String F_BLOCKCHAIN_CONFIG = "blockchain-config";
	public static final String F_BOT_STATE = "bot-state";
	public static final String F_OSM_TAGS = "osm-tags";
	public static final String F_DATE = "date";
	
	public static final String F_URL = "url";
	public static final String F_TIMESTAMP = "overpass_timestamp";
	public static final String F_OVERPASS = "overpass";
	public static final String F_THREADS = "threads";
	
	public static final String F_PLACES_PER_OPERATION = "places_per_operation";
	public static final String F_OPERATIONS_PER_BLOCK = "operations_per_block";

	private static final long WAIT_HOURS_TIMEOUT = 4;
	private static final long SPLIT_QUERY_LIMIT = 10000;
	
	private long placesPerOperation = 250;
	private long operationsPerBlock = 16;
	private String opType;
	private OpObject botObject;
	
	@Autowired
	private BlocksManager blocksManager;

	private PlaceManager placeManager;

	ThreadPoolExecutor service;

	public OsmSyncBot(OpObject botObject) {
		this.botObject = botObject;
	}
	
	public static class SyncRequest {
		public String date;
		public String key;
		public List<String> values = new ArrayList<String>();
		public List<String> type = new ArrayList<String>();
		public String coordinates;
		
		List<String> nvalues = new ArrayList<String>();
		List<String> ntype = new ArrayList<String>();
		
		public SyncRequest state;
		public String name;
		public boolean empty;
	}
	

	public String generateRequestString(String overpassURL,
			Collection<SyncRequest> req, String bboxParam, boolean diff, boolean cnt) throws UnsupportedEncodingException {
		// check if works 'out meta; >; out geom;' vs 'out geom meta;';
		String queryType = diff ? "diff" : "date";
		String requestTemplate = "[out:xml][timeout:1800][maxsize:1000000000][%s:%s]; %s out geom meta;";
		if(cnt) {
			requestTemplate = "[out:csv(::count;false)][timeout:1800][maxsize:1000000000][%s:%s]; %s out count;";
		}
		String subTagRequest = "%s[\"%s\"~\"%s\"]%s;";
		StringBuilder ts = new StringBuilder();
		String timestamp = null;
		for (SyncRequest tag : req) {
			String ntimestamp = "\"" + (diff ? (tag.state.date + "," + tag.date) : tag.date) + "\"";
			if(timestamp == null) {
				timestamp = ntimestamp;
			} else if(OUtils.equals(ntimestamp, timestamp)) {
				throw new IllegalStateException(String.format("Timestmap %s != %s", ntimestamp, timestamp));
			}
			List<String> tps = diff ? tag.type: tag.ntype;
			List<String> values = diff ? tag.values: tag.nvalues;
			for (String type : tps) {
				StringBuilder tagsValues = new StringBuilder();
				for (String strTag : values) {
					if (tagsValues.length() == 0) {
						tagsValues.append(strTag);
					} else {
						tagsValues.append("|").append(strTag);
					}
				}
				String bbox = tag.coordinates;
				if(tag.coordinates == null && bboxParam == null) {
					bbox = "";
				} else if(bboxParam != null){
					bbox = "(" + bboxParam +")";
				} else {
					bbox = "(" + tag.coordinates +")";
				}
				ts.append(String.format(subTagRequest, type, tag.key, tagsValues, bbox));
			}
		}
		String request = String.format(requestTemplate, queryType, timestamp, ts.toString());
		// LOGGER.info(String.format("Overpass query: %s", request));
		request = URLEncoder.encode(request, StandardCharsets.UTF_8.toString());
		request = overpassURL+ "?data=" + request;
		return request;
	}
	
	private String alignTimestamp(String timestamp) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
			sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			Date dt = sdf.parse(timestamp);
			Calendar i = Calendar.getInstance();
			i.setTime(dt);
			i.set(Calendar.SECOND, 0);
			i.set(Calendar.MINUTE, 0);
			return sdf.format(i.getTime());
		} catch (ParseException e) {
			return timestamp;
		}
	}
	
	
	@SuppressWarnings("unchecked")
	private Map<String, Object> getMap(String field1, String field2) {
		Map<String, Object> mp = botObject.getStringObjMap(field1);
		if(mp == null) {
			return Collections.emptyMap();
		}
		mp  = (Map<String, Object>) mp.get(field2);
		if(mp == null) {
			return Collections.emptyMap();
		}
		return mp;
	}
	
	private List<SyncRequest> calculateRequests(String timestamp, Map<String, Object> schema, Map<String, Object> state) {
		List<SyncRequest> r = new ArrayList<OsmSyncBot.SyncRequest>();
		Iterator<Entry<String, Object>> i = schema.entrySet().iterator();
		while (i.hasNext()) {
			Entry<String, Object> e = i.next();
			SyncRequest cfg = ObjectGenerator.parseSyncRequest(null, e.getValue());
			cfg.name = e.getKey();
			cfg.date = timestamp;
			
			SyncRequest cstate = ObjectGenerator.parseSyncRequest(cfg, state.get(cfg.name));
			cstate.name = e.getKey();
			cfg.state = cstate;
			
			// calculate diff to retrieve new objects
			if(!OUtils.equalsStringValue(cfg.key, cstate.key)) {
				throw new UnsupportedOperationException("Change sync key is not supported");
			}
			if(!OUtils.equalsStringValue(cfg.coordinates, cstate.coordinates)) {
				throw new UnsupportedOperationException("Change bbox is not supported");
			}
			if (cstate.type.isEmpty() && cstate.values.isEmpty()) {
				cfg.nvalues.addAll(cfg.values);
				cfg.ntype.addAll(cfg.type);
			} else {
				for (String v : cfg.type) {
					if (!cstate.type.contains(v)) {
						cfg.ntype.add(v);
					}
				}
				for (String v : cfg.values) {
					if (!cstate.values.contains(v)) {
						if (!cfg.ntype.isEmpty()) {
							throw new UnsupportedOperationException("Can't change type and values at the same time");
						}
						cfg.nvalues.add(v);
					}
				}
				if (!cfg.ntype.isEmpty()) {
					cfg.nvalues.addAll(cfg.values);
				} else if (!cfg.nvalues.isEmpty()) {
					cfg.ntype.addAll(cfg.type);
				}
			}
			r.add(cfg);
		}
		return r;
	}
	
	private void submitTask(String msg, Publisher task, Deque<Future<Long>> futures)
			throws InterruptedException, ExecutionException, TimeoutException {
		LOGGER.info(msg);
		Future<Long> f = service.submit(task);
		f.get(WAIT_HOURS_TIMEOUT, TimeUnit.HOURS);
		int i = 0;
		while (!futures.isEmpty()) {
			Long ops = futures.pop().get(WAIT_HOURS_TIMEOUT, TimeUnit.HOURS);
			LOGGER.info(String.format("%d / %d: added %d ops", i++, futures.size(), ops));
		}
	}
	

	@Override
	public OsmSyncBot call() throws Exception {
		try {
			Map<String, Object> urls = getMap(F_CONFIG, F_URL);
			this.opType = botObject.getStringMap(F_BLOCKCHAIN_CONFIG).get(F_TYPE);
			this.placeManager = new PlaceManager(opType, blocksManager, botObject);
			this.placesPerOperation = botObject.getField(placesPerOperation, F_BLOCKCHAIN_CONFIG,
					F_PLACES_PER_OPERATION);
			this.operationsPerBlock = botObject.getField(operationsPerBlock, F_BLOCKCHAIN_CONFIG,
					F_OPERATIONS_PER_BLOCK);

			String overpassURL = urls.get(F_OVERPASS).toString();
			ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
					.setNameFormat(Thread.currentThread().getName() + "-%d").build();
			this.service = (ThreadPoolExecutor) Executors.newFixedThreadPool(botObject.getIntValue(F_THREADS, 0),
					namedThreadFactory);
			String timestamp = OprUtil.downloadString(urls.get(F_TIMESTAMP).toString(),
					"Download current OSM timestamp");
			timestamp = alignTimestamp(timestamp);
			LOGGER.info(String.format("Start synchronizing: %s", timestamp));

			Map<String, Object> schema = getMap(F_CONFIG, F_OSM_TAGS);
			Map<String, Object> state = getMap(F_BOT_STATE, F_OSM_TAGS);

			Deque<Future<Long>> futures = new ConcurrentLinkedDeque<Future<Long>>();
			List<SyncRequest> requests = calculateRequests(timestamp, schema, state);
			for (SyncRequest r : requests) {
				if (!r.ntype.isEmpty() && !r.nvalues.isEmpty()) {
					Publisher task = new Publisher(futures, overpassURL, r, null, false);
					submitTask(String.format("> Start synchronizing %s new tag/values [%s] [%s] - %s", r.name,
							r.nvalues, r.ntype, r.date), task, futures);
					OpOperation op = generateEditOpForBotObject(r, botObject, blocksManager);
					blocksManager.addOperation(op);
				}
				if (!OUtils.equals(r.date, r.state.date)) {
					Publisher task = new Publisher(futures, overpassURL, r, null, true);
					submitTask(String.format("> Start synchronizing diff %s [%s]->[%s]", r.name, r.date, r.state.date),
							task, futures);
					OpOperation op = generateEditOpForBotObject(r.name, r.state.date, r.date, botObject, blocksManager);
					blocksManager.addOperation(op);
				}
			}
			LOGGER.info("Synchronization is finished");
		} catch (Exception e) {
			LOGGER.info("Synchronization has failed: " + e.getMessage(), e);
			throw e;
		} finally {
			if (this.service != null) {
				this.service.shutdown();
				this.service = null;
			}
		}
		return this;
	}

	@Override
	public String getTaskDescription() {
		return "Synchronising with OpenStreetMap";
	}

	@Override
	public String getTaskName() {
		return "osm-sync";
	}

	@Override
	public int taskCount() {
		return 1;
	}

	@Override
	public int total() {
		return (int) service.getTaskCount();
	}

	@Override
	public int progress() {
		return (int) service.getCompletedTaskCount();
	}

	private class Publisher implements Callable<Long> {

		private long opCounter;
		private SyncRequest request;
		private boolean diff;
		private String overpassURL;
		private Deque<Future<Long>> futures;
		private String bbox;

		public Publisher(Deque<Future<Long>> futures, String overpassURL, SyncRequest request, 
				String bbox, boolean diff) {
			this.futures = futures;
			this.overpassURL = overpassURL;
			this.request = request;
			this.bbox = bbox;
			this.diff = diff;
		}

		@Override
		public Long call() throws IOException {
			try {
				if (!diff && request.coordinates == null && bbox == null) {
					// calculate bbox to process in parallel
					double xd = 360 / 8;
					double yd = 180 / 4;
					int split = 3;
					for (double tx = -180; tx + xd <= 180; tx += xd) {
						for (double ty = -90; ty + yd <= 90; ty += yd) {
							String bbox = String.format("%f,%f,%f,%f", ty, tx, ty + yd, tx + xd);
							String reqUrl = generateRequestString(overpassURL, Collections.singletonList(request), bbox,
									false, true);
							BufferedReader r = OprUtil.downloadGzipReader(reqUrl, String.format("Check size of (%s)", bbox));
							String c = r.readLine();
							Long cnt = c == null ? null : Long.parseLong(c);
							r.close();
							LOGGER.info(String.format("Size (%s) %s", bbox, c));
							if (cnt != null && cnt < SPLIT_QUERY_LIMIT) {
								if (cnt > 0) {
									Publisher task = new Publisher(futures, overpassURL, request, bbox, false);
									futures.add(service.submit(task));
								}
							} else {
								double sxd = xd / split;
								double syd = xd / split;
								for (double ttx = tx; ttx + sxd <= tx + xd; ttx += sxd) {
									for (double tty = ty; tty + syd <= ty + yd; tty += sxd) {
										String nbbox = String.format("%f,%f,%f,%f", tty, ttx, tty + syd, tx + sxd);
										Publisher task = new Publisher(futures, overpassURL, request, nbbox, false);
										futures.add(service.submit(task));
									}
								}
							}

						}
					}
					return 0l;
				} else {
					String reqUrl = generateRequestString(overpassURL, Collections.singletonList(request), bbox, diff,
							false);

					Metric m = mOverpassQuery.start();
					Reader file = OprUtil.downloadGzipReader(reqUrl,
							String.format("Download overpass data (%s)", bbox == null ? "" : bbox));
					OsmParser osmParser = null;
					osmParser = new OsmParser(file);
					m.capture();

					m = mPublish.start();
					publish(osmParser);
					m.capture();
					file.close();

					return opCounter;
				}
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
				throw new IllegalStateException(e.getMessage(), e);
			}
		}

		private void publish(OsmParser osmParser) {
			while (osmParser.hasNext()) {
				try {
					OpOperation opOperation = initOpOperation(opType, blocksManager);
					if (!diff) {
						List<Entity> newEntities = osmParser.parseNextCoordinatePlaces(placesPerOperation, Entity.class);
						for(Entity e : newEntities) {
							Metric m = mProcEntity.start();
							processEntity(opOperation, e);
							m.capture();
						}
					} else {
						List<DiffEntity> places = osmParser.parseNextCoordinatePlaces(placesPerOperation, DiffEntity.class);
						for(DiffEntity e : places) {
							Metric m = mProcDiff.start();
							processDiffEntity(opOperation, e);
							m.capture();
						}
					}
					if(opOperation.hasCreated() || opOperation.hasEdited()) {
						opCounter++;
						Metric m = mOpAdd.start();
						blocksManager.addOperation(generateHashAndSign(opOperation, blocksManager));
						m.capture();
					}
					if (blocksManager.getBlockchain().getQueueOperations().size() >= operationsPerBlock) {
						Metric m = mBlock.start();
						blocksManager.createBlock();
						m.capture();
					}
				} catch (Exception e) {
					LOGGER.error("Error while publishing", e);
					break;
				}
			}
		}

		private void processEntity(OpOperation opOperation, Entity obj) {
			OpObject loadedObj = placeManager.getObjectByExtId(obj.getId());
			if (loadedObj == null) {
				OpObject newObj = generateCreateOpObject(obj);
				opOperation.addCreated(newObj);
			} else {
				OpObject newObj = generateCreateOpObject(obj);
				String matchId = placeManager.generateMatchIdFromOpObject(newObj);
				String oldMatchId = placeManager.generateMatchIdFromOpObject(loadedObj);
				if (!Objects.equals(matchId, oldMatchId)) {
					opOperation.addDeleted(loadedObj.getId());
					opOperation.addCreated(newObj);
				}
			}
		}

		private void processDiffEntity(OpOperation opOperation, DiffEntity diffEntity) {
			if (DiffEntity.DiffEntityType.DELETE.equals(diffEntity.getType())) {
				OpObject deleteObject =  placeManager.getObjectByExtId(diffEntity.getOldNode().getId());
				if (deleteObject != null) {
					opOperation.addDeleted(deleteObject.getId());
				}
			} else if (DiffEntity.DiffEntityType.CREATE.equals(diffEntity.getType())) {

				OpObject loadedObj = placeManager.getObjectByExtId(diffEntity.getNewNode().getId());
				if (loadedObj == null) {
					OpObject newObj = generateCreateOpObject(diffEntity.getNewNode());
					opOperation.addCreated(newObj);
				} else {
					OpObject newObj = generateCreateOpObject(diffEntity.getNewNode());
					String matchId = placeManager.generateMatchIdFromOpObject(loadedObj);
					String newMatchId = placeManager.generateMatchIdFromOpObject(newObj);
					if (!Objects.equals(matchId, newMatchId)) {
						opOperation.addDeleted(loadedObj.getId());
						opOperation.addCreated(newObj);
					}
				}
			} else if (DiffEntity.DiffEntityType.MODIFY.equals(diffEntity.getType())) {
				OpObject edit = new OpObject();

				if (diffEntity.getNewNode().getLatLon().equals(diffEntity.getOldNode().getLatLon())) {
					OpObject loadedObject = placeManager.getObjectByExtId(diffEntity.getOldNode().getId());
					if (loadedObject != null) {
						opOperation.addEdited(generateEditOpObject(edit, diffEntity, loadedObject.getId()));
					}
				} else {
					if (diffEntity.getOldNode().getId() != diffEntity.getNewNode().getId()) {
						OpObject loadedObject = placeManager.getObjectByExtId(diffEntity.getOldNode().getId());
						if (loadedObject != null) {
							opOperation.addDeleted(loadedObject.getId());
						}
					}
					OpObject opObject = placeManager.getObjectByExtId(diffEntity.getNewNode().getId());
					if (opObject == null) {
						OpObject createObj = generateCreateOpObject(diffEntity.getNewNode());
						opOperation.addCreated(createObj);
					} else {
						String matchId = placeManager.generateMatchIdFromOpObject(opObject);
						OpObject newOpObject = generateCreateOpObject(diffEntity.getNewNode());
						String newMatchId = placeManager.generateMatchIdFromOpObject(newOpObject);
						if (!Objects.equals(matchId, newMatchId)) {
							opOperation.addDeleted(opObject.getId());
							opOperation.addCreated(newOpObject);
						}
					}
				}
			}
		}
	}


}
