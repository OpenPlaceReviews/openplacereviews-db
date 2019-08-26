package org.openplacereviews.osm.service;

import static org.openplacereviews.opendb.ops.OpBlockchainRules.OP_BOT;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
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
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.ops.PerformanceMetrics;
import org.openplacereviews.opendb.ops.PerformanceMetrics.Metric;
import org.openplacereviews.opendb.ops.PerformanceMetrics.PerformanceMetric;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.DBConsensusManager.DBStaleException;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.opendb.util.OpExprEvaluator;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.openplacereviews.osm.model.DiffEntity;
import org.openplacereviews.osm.model.Entity;
import org.openplacereviews.osm.model.Entity.EntityType;
import org.openplacereviews.osm.model.QuadRect;
import org.openplacereviews.osm.parser.OsmParser;
import org.openplacereviews.osm.util.OprExprEvaluatorExt;
import org.openplacereviews.osm.util.OprUtil;
import org.openplacereviews.osm.util.PlaceOpObjectHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.xmlpull.v1.XmlPullParserException;

public class OsmSyncBot extends GenericMultiThreadBot<OsmSyncBot> {

	private static final Log LOGGER = LogFactory.getLog(OsmSyncBot.class);
	
	private static final PerformanceMetric mOverpassQuery = PerformanceMetrics.i().getMetric("opr.osm-sync.overpass");
	private static final PerformanceMetric mPublish = PerformanceMetrics.i().getMetric("opr.osm-sync.publish");
	private static final PerformanceMetric mProcDiff = PerformanceMetrics.i().getMetric("opr.osm-sync.proc-diff");
	private static final PerformanceMetric mProcEntity = PerformanceMetrics.i().getMetric("opr.osm-sync.proc-e");
	private static final PerformanceMetric mOpAdd = PerformanceMetrics.i().getMetric("opr.osm-sync.opadd");
	
	public static final String INDEX_OSMID = "osmid";
	public static final String F_MATCH_ID = "match-id";
	
	public static final String F_CONFIG = "config";
	public static final String F_BOT_STATE = "bot-state";
	public static final String F_OSM_TAGS = "osm-tags";
	public static final String F_DATE = "date";
	
	public static final String F_URL = "url";
	public static final String F_TIMESTAMP = "overpass_timestamp";
	public static final String F_OVERPASS = "overpass";
	
	// public static final String OVERPASS_CACHE_FOLDER = null;
	
	
	private static final long SPLIT_QUERY_LIMIT_PLACES = 20000;
	private static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
	static {
		TIMESTAMP_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	
	private static final long OVERPASS_MAX_ADIFF_MS = 3 * 60 * 60 * 1000l;
	private static final int OVERPASS_MIN_DELAY_MIN = 3;
	private static final long DB_STALE_TIMEOUT_MS = 5000;

	
	@Value("${opendb.files-backup.overpass-cache}")
	private String overpassCacheFolder;
	
	private OpExprEvaluator matchIdExpr;
	
	@Autowired
	private BlocksManager blocksManager;
	

	public OsmSyncBot(OpObject botObject) {
		super(botObject);
	}
	
	public static class SyncRequest {
		public String date;
		public String key;
		public List<String> values = new ArrayList<String>();
		public List<String> type = new ArrayList<String>();
		public String bbox;
		
		List<String> nvalues = new ArrayList<String>();
		List<String> ntype = new ArrayList<String>();
		
		public SyncRequest state;
		public String name;
		public boolean empty;
		
		public QuadRect coordinates() {
			if(bbox == null || bbox.length() == 0) {
				return null;
			}
			QuadRect qr = new QuadRect();
			String[] s = bbox.split(",");
			qr.minY = Double.parseDouble(s[0]);
			qr.minX = Double.parseDouble(s[1]);
			qr.maxY = Double.parseDouble(s[2]);
			qr.maxX = Double.parseDouble(s[3]);
			return qr;
		}

		public String getCacheId(QuadRect b, boolean diff, boolean cnt) {
			String r = diff ? "diff_": "req_" ;
			if(cnt) {
				r += "cnt_";
			}
			r += name + "/";
			r += date;
			if (diff) {
				r += "-" + state.date;
			}
			DecimalFormat df = new DecimalFormat("0.#");
			r += "_" + String.format("%s,%s,%s,%s", 
					df.format(b.minY), df.format(b.minX), df.format(b.maxY), df.format(b.maxX));
			return r.replace(':', '_');
		}
	}
	

	public String generateRequestString(String overpassURL,
			Collection<SyncRequest> reqs, QuadRect bbox, boolean diff, boolean cnt) throws UnsupportedEncodingException {
		// check if works 'out meta; >; out geom;' vs 'out geom meta;';
		String queryType = diff ? "diff" : "date";
		String sizeLimit = "[timeout:1800]"; // [maxsize:8000000000]
		String requestTemplate = "[out:xml]"+sizeLimit+"[%s:%s]; %s ( %s ); out geom meta;";
		if(cnt) {
			requestTemplate = "[out:csv(::count;false)]"+sizeLimit+"[%s:%s]; %s ( %s ); out count;";
		}
		
		StringBuilder ts = new StringBuilder();
		String timestamp = "";
		Set<String> changedTypes = new TreeSet<>();
		String bboxs = "";
		if (bbox != null && (bbox.width() < 360 || bbox.height() < 180)) {
			bboxs = String.format("(%f,%f,%f,%f)", bbox.minY, bbox.minX, bbox.maxY, bbox.maxX);
		}
		for (SyncRequest req : reqs) {
			List<String> tps = diff ? req.type: req.ntype;
			List<String> values = diff ? req.values: req.nvalues;
			String setName = "";
			// retrieve new data on state date
			String ntimestamp = "\"" + req.state.date + "\"";
			if(diff) {
				ntimestamp = "\"" + req.state.date + "\",\"" + req.date + "\"";
				setName = ".a";
				changedTypes.addAll(tps);
			}
			if (timestamp.length() == 0) {
				timestamp = ntimestamp;
			} else if(!OUtils.equals(ntimestamp, timestamp)) {
				throw new IllegalStateException(String.format("Timestamp %s != %s", ntimestamp, timestamp));
			}
			
			for (String type : tps) {
				for (String vl : values) {
					String tagFilter = "[" + req.key + "=" + vl + "]";
					String reqFilter = type + setName + tagFilter + bboxs + ";";
					ts.append(reqFilter);
				}
			}
		}
		StringBuilder changedS = new StringBuilder();
		if(changedTypes.size() > 0) {
			changedS.append("(");
			for(String tp : changedTypes) {
				changedS.append(" ").append(tp).append("(changed:").append(timestamp).append(")");
				changedS.append(bboxs).append(";");
			}
			changedS.append(")->.a; ");
		}

		String request = String.format(requestTemplate, queryType, timestamp, changedS.toString(), ts.toString());
		LOGGER.info(String.format("Overpass query: %s", request));
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
			i.add(Calendar.MINUTE, -OVERPASS_MIN_DELAY_MIN);
			i.set(Calendar.MINUTE, (i.get(Calendar.MINUTE) / 15) * 15);
			if(dt.getTime() - i.getTimeInMillis() < OVERPASS_MIN_DELAY_MIN) {
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
	
	private List<SyncRequest> calculateRequests(Map<String, Object> schema, Map<String, Object> state) {
		List<SyncRequest> r = new ArrayList<OsmSyncBot.SyncRequest>();
		Iterator<Entry<String, Object>> i = schema.entrySet().iterator();
		while (i.hasNext()) {
			Entry<String, Object> e = i.next();
			SyncRequest cfg = parseSyncRequest(null, e.getValue());
			cfg.name = e.getKey();
			
			SyncRequest cstate = PlaceOpObjectHelper.parseSyncRequest(cfg, state.get(cfg.name));
			cstate.name = e.getKey();
			cfg.state = cstate;
			cfg.date = cstate.date;
			
			// calculate diff to retrieve new objects
			if(!OUtils.equalsStringValue(cfg.key, cstate.key)) {
				throw new UnsupportedOperationException("Change sync key is not supported");
			}
			if(!OUtils.equalsStringValue(cfg.bbox, cstate.bbox)) {
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
	

	@Override
	public synchronized OsmSyncBot call() throws Exception {
		try {
			Map<String, Object> urls = getMap(F_CONFIG, F_URL);
			
			String matchId = botObject.getStringMap(F_CONFIG).get(F_MATCH_ID);
			matchIdExpr = OprExprEvaluatorExt.parseExpression(matchId);
			

			String overpassURL = urls.get(F_OVERPASS).toString();
			String ctimestamp = OprUtil.downloadString(urls.get(F_TIMESTAMP).toString(),
					"Download current OSM timestamp");
			String atimestamp = alignTimestamp(ctimestamp);
			if(atimestamp == null) {
				LOGGER.info(String.format("Nothing to synchronize yet: %s", ctimestamp));
				return this;
			}
			ctimestamp = atimestamp;
			super.initVars();
			
			LOGGER.info(String.format("Start synchronizing: %s", ctimestamp));
			
			Map<String, Object> schema = getMap(F_CONFIG, F_OSM_TAGS);
			Map<String, Object> state = getMap(F_BOT_STATE, F_OSM_TAGS);

			Deque<Future<TaskResult>> futures = new ConcurrentLinkedDeque<Future<TaskResult>>();
			List<SyncRequest> requests = calculateRequests(schema, state);
			for (SyncRequest r : requests) {
				if (!r.ntype.isEmpty() && !r.nvalues.isEmpty()) {
					if(r.date == null) {
						r.date = ctimestamp;
					}
					if(r.state.empty) {
						OpOperation op = initOpOperation(OP_BOT);
						r.state.date = r.date;
						PlaceOpObjectHelper.generateEditBeginObject(op, r, botObject);
						generateHashAndSignAndAdd(op);
					}
					Publisher task = new Publisher(futures, overpassURL, r, r.coordinates(), false); //.setUseCount(true);
					String msg = String.format(" %s new tag/values [%s] [%s] - %s", r.name, r.nvalues, r.ntype, r.date);
					submitTaskAndWait("Synchronization started: " + msg, task, futures);
					if(isInterrupted()) {
						LOGGER.info("Synchronization interrupted: " + msg);
						return this;
					}
					OpOperation op = initOpOperation(OP_BOT);
					generateEditOpForBotObject(op, r, botObject);
					generateHashAndSignAndAdd(op);
					
				}
				if (!OUtils.equals(ctimestamp, r.state.date)) {
					r.date = ctimestamp;
					try {
						if(TIMESTAMP_FORMAT.parse(r.date).getTime() - TIMESTAMP_FORMAT.parse(r.state.date).getTime() > OVERPASS_MAX_ADIFF_MS) {
							Date nd = new Date(TIMESTAMP_FORMAT.parse(r.state.date).getTime() + OVERPASS_MAX_ADIFF_MS);
							r.date = TIMESTAMP_FORMAT.format(nd);
						}
					} catch (ParseException e1) {
						throw new IllegalArgumentException(e1);
					}
					Publisher task = new Publisher(futures, overpassURL, r, r.coordinates(), true);
					String msg = String.format(" diff %s [%s]->[%s]", r.name, r.state.date, r.date);
					submitTaskAndWait("Synchronization started: " + msg, task, futures);
					if(isInterrupted()) {
						LOGGER.info("Synchronization interrupted: " + msg);
						return this;
					}
					OpOperation op = initOpOperation(OP_BOT);
					generateEditOpForBotObject(op, r.name, r.state.date, r.date, botObject);
					generateHashAndSignAndAdd(op);
				}
			}
			LOGGER.info("Synchronization is finished ");
		} catch (Exception e) {
			LOGGER.info("Synchronization has failed: " + e.getMessage(), e);
			throw e;
		} finally {
			super.shutdown();
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
	
	public static class PlaceObject {
		public OpObject obj;
		public Map<String, Object> osm;
		public int ind = 0;
		public Long osmId; 
		public String type;
		public Entity e;
		public String osmKey;
	}
	
	public PlaceObject getObjectByOsmEntity(String key, Entity e) throws InterruptedException, DBStaleException {
		Long osmId = e.getId();
		String type = EntityType.valueOf(e).getName();
		OpBlockChain.ObjectsSearchRequest objectsSearchRequest = new OpBlockChain.ObjectsSearchRequest();
		List<OpObject> r = blocksManager.getObjectsByIndex(opType, INDEX_OSMID, objectsSearchRequest, osmId);
		for(OpObject o : r) {
			List<Map<String, Object>> osmObjs = o.getField(null, F_SOURCE, F_OSM);
			for (int i = 0; i < osmObjs.size(); i++) {
				Map<String, Object> osm  = osmObjs.get(i);
				if (osmId.equals(osm.get(OpOperation.F_ID)) && type.equals(osm.get(OpOperation.F_TYPE))
						&& key.equals(osm.get(F_OSM_TAG))) {
					PlaceObject po = new PlaceObject();
					po.e = e;
					po.obj = o;
					po.ind = i;
					po.osm = osm;
					po.type = type;
					po.osmId = osmId;
					po.osmKey = key;
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
	
	
	private class Publisher implements Callable<TaskResult> {

		
		private int placeCounter;
		private SyncRequest request;
		private boolean diff;
		private boolean useCount;
		private String overpassURL;
		private Deque<Future<TaskResult>> futures;
		private QuadRect bbox;
		private int level = 0;
		private String levelString = "";

		public Publisher(Deque<Future<TaskResult>> futures, String overpassURL, SyncRequest request, 
				QuadRect bbox, boolean diff) {
			this.futures = futures;
			this.overpassURL = overpassURL;
			this.request = request;
			this.bbox = bbox;
			this.diff = diff;
		}
		
		public Publisher setUseCount(boolean useCount) {
			this.useCount = useCount;
			return this;
		}
		
		public Publisher setLevel(int level) {
			this.level = level;
			return this;
		}
		
		public Publisher setLevelString(String levelString) {
			this.levelString = levelString;
			return this;
		}
		
		
		private TaskResult split(long tm, String reason) throws IOException {
			// calculate bbox to process in parallel
			int sx = 2, sy = 2;
			if(level < 3 && !diff) {
				sx = sy = 4;
			}
			if(bbox.width() >= 180) {
				sx = 10 ;
			}
			if(bbox.height() >= 90) {
				sy = 4;
			}
			double xd = bbox.width() / sx;
			double yd = bbox.height() / sy;
			if(level >= 7) {
				throw new IllegalStateException("Split went too deep"); 
			}
			int i = 0;
			for (double tx = bbox.minX; tx + xd <= bbox.maxX; tx += xd) {
				for (double ty = bbox.minY; ty + yd <= bbox.maxY; ty += yd) {
					i++;
					QuadRect nqr = new QuadRect(tx, ty, tx + xd, ty + yd);
					Publisher task = new Publisher(futures, overpassURL, request, nqr, diff)
							.setUseCount(useCount)
							.setLevelString(String.format("%s(%d/%d)", levelString, i, sx * sy))
							.setLevel(level +1);
					submitTask(null, task, futures);
				}
			}
			return new TaskResult(String.format("Split further %s into %d %s after %d ms: %s ", 
							levelString, sx * sy, bbox.toString(), 
							System.currentTimeMillis() - tm, reason), null);
		}
		

		@Override
		public TaskResult call() throws IOException {
			try {
				long tm = System.currentTimeMillis();
				String splitReason = "";
				TaskResult res = null;	
				if(bbox == null) {
					bbox = new QuadRect(-180, -90, 180, 90);
					if(!diff) {
						return split(tm, splitReason);
					}
				}
				try {
					waitBlockCreation();
					res = proc();
				} catch (IOException e) {
					// repeat and continue split
					splitReason = e.getMessage() + " (" + e.getClass() + ") ";
				} catch (DBStaleException | DataAccessException | InterruptedException e) {
					// repeat because of db stale exception
					Thread.sleep(DB_STALE_TIMEOUT_MS);
					submitTask(null, this, futures);
				}
				if(res == null) {
					return split(tm, splitReason);					
				}
				return res;
			} catch (Exception e) {
				return errorResult(e.getMessage(), e);
			}
		}

		private TaskResult proc() throws Exception {
			long tm = System.currentTimeMillis();
			
			Metric m = mOverpassQuery.start();
			String msg  = String.format("%s overpass data %s", useCount ? "Count":"Download", bbox);
			BufferedReader r;
			File cacheFile = null;
			if(!OUtils.isEmpty(overpassCacheFolder)) {
				String cid = request.getCacheId(bbox, diff, useCount);
				cacheFile = new File(overpassCacheFolder, cid + ".osm.gz");
				if(!cacheFile.exists()) {
					r = downloadOverpass(msg);
					cacheFile.getParentFile().mkdirs();
					OutputStreamWriter w = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(cacheFile)));
					IOUtils.copy(r, w);
					r.close();
					w.close();
				}
				GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(cacheFile));
				r = new BufferedReader(new InputStreamReader(gzis));
			} else {
				r = downloadOverpass(msg);
			}
			m.capture();
			if (useCount) {
				String c = r.readLine();
				Long cnt = c == null ? null : Long.parseLong(c);
				r.close();
				if (cnt != null && cnt < SPLIT_QUERY_LIMIT_PLACES) {
					if(cnt > 0) {
						Publisher task = new Publisher(futures, overpassURL, request, bbox, diff)
								.setUseCount(false)
								.setLevelString(levelString)
								.setLevel(level);
						submitTask(null, task, futures);
					} 
					return new TaskResult(String.format("Proccessed count (%s) bbox %s coordinates %d ms",
							c, bbox, (System.currentTimeMillis() - tm), futures.size()), 0, null);
				}
				return null;
			} else {
				try {
					OsmParser osmParser = new OsmParser(r);
					m = mPublish.start();
					List<OpOperation> opsToAdd = publish(request.key, osmParser);
					m.capture();
					r.close();
					tm = System.currentTimeMillis() - tm + 1;
					for(OpOperation o : opsToAdd) {
						waitBlockCreation();
						m = mOpAdd.start();
						generateHashAndSignAndAdd(o);
						m.capture();
					}
				} catch (IOException e) {
					if (cacheFile != null) {
						r.close();
						cacheFile.delete();
					}
					throw e;
				}
				return new TaskResult(String.format("Proccessed places %s: %d ms, %d places, %d places / sec",
						bbox, tm, placeCounter, placeCounter * 1000 / tm), placeCounter, null);

			}
		}

		private BufferedReader downloadOverpass(String msg) throws UnsupportedEncodingException, IOException {
			BufferedReader r;
			String reqUrl = generateRequestString(overpassURL, Collections.singletonList(request), bbox, diff, useCount);
			r = OprUtil.downloadGzipReader(reqUrl, msg);
			return r;
		}

		private List<OpOperation> publish(String key, OsmParser osmParser) throws FailedVerificationException, IOException, XmlPullParserException, InterruptedException {
			List<OpOperation> opsToAdd = new ArrayList<OpOperation>();
			while (osmParser.hasNext()) {
				OpOperation addOp = initOpOperation(opType);
				OpOperation editOp = initOpOperation(opType);
				if (!diff) {
					List<Entity> newEntities = osmParser.parseNextCoordinatePlaces(placesPerOperation, Entity.class);
					for (Entity e : newEntities) {
						Metric m = mProcEntity.start();
						processEntity(key, addOp, editOp, e);
						placeCounter++;
						m.capture();
					}
				} else {
					List<DiffEntity> places = osmParser.parseNextCoordinatePlaces(placesPerOperation, DiffEntity.class);
					for (DiffEntity e : places) {
						Metric m = mProcDiff.start();
						processDiffEntity(key, addOp, editOp, e);
						placeCounter++;
						m.capture();
					}
				}
				if (addOp.hasCreated()) {
					opsToAdd.add(addOp);
				}
				if(editOp.hasEdited()) {
					opsToAdd.add(editOp);
				}
			}
			return opsToAdd;
		}

		private void processEntity(String key, OpOperation addOp, OpOperation editOp, Entity obj) throws FailedVerificationException, InterruptedException {
			try {
				PlaceObject po = getObjectByOsmEntity(key, obj);
				if (po == null) {
					OpObject newObj = generateNewOprObject(obj, createOsmObject(key, obj));
					addOp.addCreated(newObj);
				} else {
					TreeMap<String, Object> osmObj = createOsmObject(key, obj);
					String matchId = generateMatchIdFromOpObject(osmObj);
					String oldMatchId = generateMatchIdFromOpObject(po.osm);
					if (!Objects.equals(matchId, oldMatchId)) {
						OpObject newObj = generateNewOprObject(obj, osmObj);
						addOp.addCreated(newObj);
						// separate operation to delete old object
						generateEditDeleteOsmIdsForPlace(editOp, po);
					} else {
						generateEditValuesForPlace(editOp, po, osmObj);
					}
				}
			} catch (RuntimeException e) {
				// extra logging to catch exception with objects
				if(obj != null) {
					LOGGER.error(e.getMessage() + ": " + obj.getId() + " " + obj.getTags());
				}
				throw e;
			}
		}

		private void processDiffEntity(String key, OpOperation addOp, OpOperation editOp, DiffEntity diffEntity) throws FailedVerificationException, InterruptedException {
			if (DiffEntity.DiffEntityType.DELETE == diffEntity.getType()) {
				PlaceObject pdo = getObjectByOsmEntity(key, diffEntity.getOldEntity());
				if(pdo != null) {
					generateEditDeleteOsmIdsForPlace(editOp, pdo);
				} else {
					logError(String.format("Couldn't find object %d: %s", diffEntity.getOldEntity().getId(), diffEntity.getOldEntity().getTags()));
					
				}
			} else if (DiffEntity.DiffEntityType.CREATE == diffEntity.getType()) {
				processEntity(key, addOp, editOp, diffEntity.getNewEntity());
			} else if (DiffEntity.DiffEntityType.MODIFY == diffEntity.getType()) {
				PlaceObject prevObject = getObjectByOsmEntity(key, diffEntity.getOldEntity());
				if(prevObject == null) {
					logError( String.format("Couldn't find object %d: %s", diffEntity.getOldEntity().getId(), diffEntity.getOldEntity().getTags()));
				} 
				if(diffEntity.getOldEntity().getId() != diffEntity.getNewEntity().getId()) {
					throw new UnsupportedOperationException(
							String.format("Diff entity id should be equal %d != %d", diffEntity.getOldEntity().getId(), diffEntity.getNewEntity().getId()));
				}
				// not necessary to compare previous version 
				processEntity(key, addOp, editOp, diffEntity.getNewEntity());
			}
		}
	}

}
