package org.openplacereviews.osm.service;

import static org.openplacereviews.opendb.ops.OpBlockchainRules.F_TYPE;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.OP_BOT;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.*;

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
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockchainRules.ErrorType;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.ops.PerformanceMetrics;
import org.openplacereviews.opendb.ops.PerformanceMetrics.Metric;
import org.openplacereviews.opendb.ops.PerformanceMetrics.PerformanceMetric;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.DBConsensusManager.DBStaleException;
import org.openplacereviews.opendb.service.IOpenDBBot;
import org.openplacereviews.opendb.service.LogOperationService;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.opendb.util.OpExprEvaluator;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.openplacereviews.osm.model.DiffEntity;
import org.openplacereviews.osm.model.Entity;
import org.openplacereviews.osm.model.QuadRect;
import org.openplacereviews.osm.model.Entity.EntityType;
import org.openplacereviews.osm.parser.OsmParser;
import org.openplacereviews.osm.util.OprExprEvaluatorExt;
import org.openplacereviews.osm.util.OprUtil;
import org.openplacereviews.osm.util.PlaceOpObjectHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.xmlpull.v1.XmlPullParserException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class OsmSyncBot implements IOpenDBBot<OsmSyncBot> {

	private static final Log LOGGER = LogFactory.getLog(OsmSyncBot.class);
	
	private static final PerformanceMetric mOverpassQuery = PerformanceMetrics.i().getMetric("opr.osm-sync.overpass");
	private static final PerformanceMetric mPublish = PerformanceMetrics.i().getMetric("opr.osm-sync.publish");
	private static final PerformanceMetric mProcDiff = PerformanceMetrics.i().getMetric("opr.osm-sync.proc-diff");
	private static final PerformanceMetric mProcEntity = PerformanceMetrics.i().getMetric("opr.osm-sync.proc-e");
	private static final PerformanceMetric mBlock = PerformanceMetrics.i().getMetric("opr.osm-sync.block");
	private static final PerformanceMetric mOpAdd = PerformanceMetrics.i().getMetric("opr.osm-sync.opadd");

	
	public static final String INDEX_OSMID = "osmid";
	public static final String F_MATCH_ID = "match-id";
	
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

	private static final long TIMEOUT_WAIT_DB_STALE_MS = 5000;
	private static final int TIMEOUT_DB_STALE_COUNT = 4;
	private static final long TIMEOUT_OVERPASS_HOURS = 4;
	private static final long SPLIT_QUERY_LIMIT_PLACES = 20000;
	private static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
	static {
		TIMESTAMP_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	
	private static final long OVERPASS_MAX_ADIFF_MS = 3 * 60 * 60 * 1000l;
	private static final long OVERPASS_MIN_DELAY_MS = 3 * 1000l;

	
	
	private long placesPerOperation = 250;
	private long operationsPerBlock = 16;
	private OpObject botObject;
	private String opType;
	private OpExprEvaluator matchIdExpr;
	
	@Autowired
	private BlocksManager blocksManager;
	
	@Autowired
	private LogOperationService logSystem;

	private ThreadPoolExecutor service;


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
		String subTagRequest = "%s[\"%s\"=\"%s\"]%s%s;";
		StringBuilder ts = new StringBuilder();
		String timestamp = null;
		for (SyncRequest tag : req) {
			String ntmpDiff = "\"" + (diff ? (tag.state.date + "\",\"" + tag.date) : tag.date) + "\"";
			String changed =  diff ? ("(changed:"+ntmpDiff +")") : "";
			if(timestamp == null) {
				timestamp = ntmpDiff;
			} else if(OUtils.equals(ntmpDiff, timestamp)) {
				throw new IllegalStateException(String.format("Timestmap %s != %s", ntmpDiff, timestamp));
			}
			List<String> tps = diff ? tag.type: tag.ntype;
			List<String> values = diff ? tag.values: tag.nvalues;
			for (String type : tps) {
				for (String strTag : values) {
					String bbox = tag.coordinates;
					if(tag.coordinates == null && bboxParam == null) {
						bbox = "";
					} else if(bboxParam != null){
						bbox = "(" + bboxParam +")";
					} else {
						bbox = "(" + tag.coordinates +")";
					}
					ts.append(String.format(subTagRequest, type, tag.key, strTag, changed, bbox));
				}
				
			}
		}
		String request = String.format(requestTemplate, queryType, timestamp, ts.toString());
//		LOGGER.info(String.format("Overpass query: %s", request));
		request = URLEncoder.encode(request, StandardCharsets.UTF_8.toString());
		request = overpassURL+ "?data=" + request;
		return request;
	}
	
	
	
	private String alignTimestamp(String timestamp) {
		try {
			Date dt = TIMESTAMP_FORMAT.parse(timestamp);
			Calendar i = Calendar.getInstance();
			i.setTime(dt);
			i.set(Calendar.SECOND, 0);
			int min = i.get(Calendar.MINUTE);
			if(min > 30) {
				i.set(Calendar.MINUTE, 30);
			} else {
				i.set(Calendar.MINUTE, 0);
			}
			if(dt.getTime() - i.getTimeInMillis() < OVERPASS_MIN_DELAY_MS) {
				return null;
			}
			return TIMESTAMP_FORMAT.format(i.getTime());
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
			SyncRequest cfg = parseSyncRequest(null, e.getValue());
			cfg.name = e.getKey();
			cfg.date = timestamp;
			
			SyncRequest cstate = PlaceOpObjectHelper.parseSyncRequest(cfg, state.get(cfg.name));
			cstate.name = e.getKey();
			cfg.state = cstate;
			
			try {
				if(TIMESTAMP_FORMAT.parse(cstate.date).getTime() - TIMESTAMP_FORMAT.parse(cfg.date).getTime() > OVERPASS_MAX_ADIFF_MS) {
					Date nd = new Date(TIMESTAMP_FORMAT.parse(cstate.date).getTime() + OVERPASS_MAX_ADIFF_MS);
					cfg.date = TIMESTAMP_FORMAT.format(nd);
				}
			} catch (ParseException e1) {
				throw new IllegalArgumentException(e1);
			}
			
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
	
	private static class TaskResult { 
		public TaskResult(String msg, Exception e) {
			this.msg = msg;
			this.e = e;
		}
		Exception e;
		String msg;
	}
	
	private void submitTask(String msg, Publisher task, Deque<Future<TaskResult>> futures)
			throws Exception {
		LOGGER.info(msg);
		Future<TaskResult> f = service.submit(task);
		int cnt = 0;
		waitFuture(cnt++, f);
		while (!futures.isEmpty()) {
			waitFuture(cnt++, futures.pop());
		}
	}
	

	private void waitFuture(int cnt, Future<TaskResult> f) throws Exception {
		TaskResult r = f.get(TIMEOUT_OVERPASS_HOURS, TimeUnit.HOURS);
		if(r.e != null) {
			LOGGER.error(String.format("%d / %d: %s", cnt, service.getTaskCount(), r.msg));
			throw r.e;
		} else {
			LOGGER.info(String.format("%d / %d: %s", cnt, service.getTaskCount(), r.msg));
		}
		
	}

	@Override
	public OsmSyncBot call() throws Exception {
		try {
			Map<String, Object> urls = getMap(F_CONFIG, F_URL);
			this.opType = botObject.getStringMap(F_BLOCKCHAIN_CONFIG).get(F_TYPE);
			String matchId = botObject.getStringMap(F_CONFIG).get(F_MATCH_ID);
			matchIdExpr = OprExprEvaluatorExt.parseExpression(matchId);
			this.placesPerOperation = botObject.getField(placesPerOperation, F_BLOCKCHAIN_CONFIG,
					F_PLACES_PER_OPERATION);
			this.operationsPerBlock = botObject.getField(operationsPerBlock, F_BLOCKCHAIN_CONFIG,
					F_OPERATIONS_PER_BLOCK);

			String overpassURL = urls.get(F_OVERPASS).toString();
			String timestamp = OprUtil.downloadString(urls.get(F_TIMESTAMP).toString(),
					"Download current OSM timestamp");
			String atimestamp = alignTimestamp(timestamp);
			if(atimestamp == null) {
				LOGGER.info(String.format("Nothing to synchronize yet: %s", timestamp));
				return this;
			}
			timestamp = atimestamp;
			
			LOGGER.info(String.format("Start synchronizing: %s", timestamp));
			ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
					.setNameFormat(Thread.currentThread().getName() + "-%d").build();
			this.service = (ThreadPoolExecutor) Executors.newFixedThreadPool(botObject.getIntValue(F_THREADS, 0),
					namedThreadFactory);

			Map<String, Object> schema = getMap(F_CONFIG, F_OSM_TAGS);
			Map<String, Object> state = getMap(F_BOT_STATE, F_OSM_TAGS);

			Deque<Future<TaskResult>> futures = new ConcurrentLinkedDeque<Future<TaskResult>>();
			List<SyncRequest> requests = calculateRequests(timestamp, schema, state);
			for (SyncRequest r : requests) {
				if (!r.ntype.isEmpty() && !r.nvalues.isEmpty()) {
					Publisher task = new Publisher(futures, overpassURL, r, null, false);
					submitTask(String.format("> Start synchronizing %s new tag/values [%s] [%s] - %s", r.name,
							r.nvalues, r.ntype, r.date), task, futures);
					OpOperation op = initOpOperation(OP_BOT);
					generateEditOpForBotObject(op, r, botObject);
					generateHashAndSignAndAdd(op);
					
				}
				if (!OUtils.equals(r.date, r.state.date)) {
					Publisher task = new Publisher(futures, overpassURL, r, null, true);
					submitTask(String.format("> Start synchronizing diff %s [%s]->[%s]", r.name, r.state.date, r.date),
							task, futures);
					OpOperation op = initOpOperation(OP_BOT);
					generateEditOpForBotObject(op, r.name, r.state.date, r.date, botObject);
					generateHashAndSignAndAdd(op);
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
	
	public static class PlaceObject {
		public OpObject obj;
		public Map<String, Object> osm;
		public int ind = 0;
		public Long osmId; 
		public String type;
		public Entity e;
	}
	
	public PlaceObject getObjectByOsmEntity(Entity e) throws InterruptedException {
		Long osmId = e.getId();
		String type = EntityType.valueOf(e).getName();
		OpBlockChain.ObjectsSearchRequest objectsSearchRequest = new OpBlockChain.ObjectsSearchRequest();
		List<OpObject> r = null;
		int cnt = TIMEOUT_DB_STALE_COUNT;
		while(r == null && cnt-- >= 0) {
			try {
				r = blocksManager.getObjectsByIndex(opType, INDEX_OSMID, objectsSearchRequest, osmId);
			} catch (DBStaleException es) {
				Thread.sleep(TIMEOUT_WAIT_DB_STALE_MS);
				if(cnt == 0) {
					throw es;
				}
			}
		}
		for(OpObject o : r) {
			List<Map<String, Object>> osmObjs = o.getField(null, F_SOURCE, F_OSM);
			for (int i = 0; i < osmObjs.size(); i++) {
				Map<String, Object> osm  = osmObjs.get(i);
				if (osmId.equals(osm.get(OpOperation.F_ID)) && type.equals(osm.get(OpOperation.F_TYPE))) {
					PlaceObject po = new PlaceObject();
					po.e = e;
					po.obj = o;
					po.ind = i;
					po.osm = osm;
					po.type = type;
					po.osmId = osmId;
					return po;
				}
			}
		}
		return null;
	}
	

	public String generateMatchIdFromOpObject(Map<String, Object> osmObj) {
		JsonFormatter formatter = blocksManager.getBlockchain().getRules().getFormatter();
		OpExprEvaluator.EvaluationContext evaluationContext = new OpExprEvaluator.EvaluationContext(
				null,
				formatter.toJsonElement(osmObj).getAsJsonObject(),
				null,
				null,
				null
		);
		return matchIdExpr.evaluateObject(evaluationContext).toString();
	}
	
	public OpOperation initOpOperation(String opType) {
		OpOperation opOperation = new OpOperation();
		opOperation.setType(opType);
		opOperation.setSignedBy(blocksManager.getServerUser());
		return opOperation;
	}
	
	public OpOperation generateHashAndSignAndAdd(OpOperation opOperation) throws FailedVerificationException {
		JsonFormatter formatter = blocksManager.getBlockchain().getRules().getFormatter();
		opOperation = formatter.parseOperation(formatter.opToJson(opOperation));
		OpOperation op = blocksManager.generateHashAndSign(opOperation, blocksManager.getServerLoginKeyPair());
		blocksManager.addOperation(op);
		return op;
	}


	private class Publisher implements Callable<TaskResult> {

		private long placeCounter;
		private SyncRequest request;
		private boolean diff;
		private String overpassURL;
		private Deque<Future<TaskResult>> futures;
		private String bbox;

		public Publisher(Deque<Future<TaskResult>> futures, String overpassURL, SyncRequest request, 
				String bbox, boolean diff) {
			this.futures = futures;
			this.overpassURL = overpassURL;
			this.request = request;
			this.bbox = bbox;
			this.diff = diff;
		}
		
		private void splitRect(String p, QuadRect qr, int sx, int sy) throws IOException {
			// calculate bbox to process in parallel
			double xd = qr.width() / sx;
			double yd = qr.height() / sy;
			int i = 0;
			for (double tx = qr.minX; tx + xd <= qr.maxX; tx += xd) {
				for (double ty = qr.minY; ty + yd <= qr.maxY; ty += yd) {
					i++;
					String bbox = String.format("%f,%f,%f,%f", ty, tx, ty + yd, tx + xd);
					String reqUrl = generateRequestString(overpassURL, Collections.singletonList(request), bbox, false,
							true);
					BufferedReader r = OprUtil.downloadGzipReader(reqUrl, String.format("Check size of (%s)", bbox));
					String c = r.readLine();
					Long cnt = c == null ? null : Long.parseLong(c);
					r.close();
					if (cnt != null && cnt < SPLIT_QUERY_LIMIT_PLACES) {
						LOGGER.info(String.format("Split %s%d/%d, size %s :%s",  p, i, sx * sy, bbox, c));
						if (cnt > 0) {
							Publisher task = new Publisher(futures, overpassURL, request, bbox, diff);
							futures.add(service.submit(task));
						}
					} else {
						LOGGER.info(String.format("Split %s%d/%d, further %s", p, i, sx * sy,  bbox));
						QuadRect nqr = new QuadRect(tx, ty, tx + xd, ty + yd);
						splitRect(i +".", nqr, 2, 2);
					}
				}
			}
		}
		

		@Override
		public TaskResult call() throws IOException {
			try {
				long tm = System.currentTimeMillis();
				if (!diff && request.coordinates == null && bbox == null) {
					QuadRect qr = new QuadRect(-180, -90, 180, 90);
					splitRect("", qr, 8, 4);
					return new TaskResult(String.format("Proccessed split bbox coordinates %d ms - tasks %d", 
							(System.currentTimeMillis() - tm), futures.size()), null); 
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
					tm = System.currentTimeMillis() - tm + 1; 
					return new TaskResult(String.format("Proccessed places (%s): %d ms, %d places, %d places / sec",
							bbox == null ? "diff" : bbox, tm, placeCounter, placeCounter * 1000 / tm), null); 
				}
			} catch (Exception e) {
				logSystem.logError(botObject, ErrorType.BOT_PROCESSING_ERROR, "osm-sync failed: " + e.getMessage(), e);
				return new TaskResult(e.getMessage(), e);
			}
		}

		private void publish(OsmParser osmParser) throws FailedVerificationException, IOException, XmlPullParserException, InterruptedException {
			while (osmParser.hasNext()) {
				OpOperation opOperation = initOpOperation(opType);
				if (!diff) {
					List<Entity> newEntities = osmParser.parseNextCoordinatePlaces(placesPerOperation, Entity.class);
					for (Entity e : newEntities) {
						Metric m = mProcEntity.start();
						processEntity(opOperation, e);
						placeCounter++;
						m.capture();
					}
				} else {
					List<DiffEntity> places = osmParser.parseNextCoordinatePlaces(placesPerOperation, DiffEntity.class);
					for (DiffEntity e : places) {
						Metric m = mProcDiff.start();
						processDiffEntity(opOperation, e);
						placeCounter++;
						m.capture();
					}
				}
				if (opOperation.hasCreated() || opOperation.hasEdited()) {
					Metric m = mOpAdd.start();
					generateHashAndSignAndAdd(opOperation);
					m.capture();
				}
				if (blocksManager.getBlockchain().getQueueOperations().size() >= operationsPerBlock) {
					Metric m = mBlock.start();
					blocksManager.createBlock();
					m.capture();
				}
			}
		}

		private void processEntity(OpOperation opOperation, Entity obj) throws FailedVerificationException, InterruptedException {
			try {
				PlaceObject po = getObjectByOsmEntity(obj);
				if (po == null) {
					OpObject newObj = generateNewOprObject(obj, createOsmObject(obj));
					opOperation.addCreated(newObj);
				} else {
					TreeMap<String, Object> osmObj = createOsmObject(obj);
					String matchId = generateMatchIdFromOpObject(osmObj);
					String oldMatchId = generateMatchIdFromOpObject(po.osm);
					if (!Objects.equals(matchId, oldMatchId)) {
						OpObject newObj = generateNewOprObject(obj, osmObj);
						opOperation.addCreated(newObj);
						// separate operation to delete old object
						OpOperation d = initOpOperation(opType);
						generateEditDeleteOsmIdsForPlace(d, po);
						generateHashAndSignAndAdd(d);
					} else {
						OpOperation d = initOpOperation(opType);
						generateEditValuesForPlace(d, po, osmObj);
						if (d.hasEdited()) {
							generateHashAndSignAndAdd(d);
						}
					}
				}
			} catch (RuntimeException e) {
				// extra logging to catch exception with objects
				LOGGER.error(e.getMessage() + ": " + obj.getId() + " " + obj.getTags());
				throw e;
			}
		}

		private void processDiffEntity(OpOperation opOperation, DiffEntity diffEntity) throws FailedVerificationException, InterruptedException {
			if (DiffEntity.DiffEntityType.DELETE == diffEntity.getType()) {
				PlaceObject pdo = getObjectByOsmEntity(diffEntity.getOldEntity());
				if(pdo != null) {
					OpOperation d = initOpOperation(opType);
					generateEditDeleteOsmIdsForPlace(d, pdo);
					generateHashAndSignAndAdd(d);
				} else {
					logSystem.logError(botObject, ErrorType.BOT_PROCESSING_ERROR, 
							String.format("Couldn't find object %d: %s", diffEntity.getOldEntity().getId(), diffEntity.getOldEntity().getTags()), null);
				}
			} else if (DiffEntity.DiffEntityType.CREATE == diffEntity.getType()) {
				processEntity(opOperation, diffEntity.getNewEntity());
			} else if (DiffEntity.DiffEntityType.MODIFY == diffEntity.getType()) {
				PlaceObject prevObject = getObjectByOsmEntity(diffEntity.getOldEntity());
				if(prevObject == null) {
					logSystem.logError(botObject, ErrorType.BOT_PROCESSING_ERROR, 
							String.format("Couldn't find object %d: %s", diffEntity.getOldEntity().getId(), diffEntity.getOldEntity().getTags()), null);
				} 
				if(diffEntity.getOldEntity().getId() != diffEntity.getNewEntity().getId()) {
					throw new UnsupportedOperationException(
							String.format("Diff entity id should be equal %d != %d", diffEntity.getOldEntity().getId(), diffEntity.getNewEntity().getId()));
				}
				// not necessary to compare previous version 
				processEntity(opOperation, diffEntity.getNewEntity());
			}
		}
	}


}
