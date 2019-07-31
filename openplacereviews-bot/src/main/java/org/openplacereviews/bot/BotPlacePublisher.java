package org.openplacereviews.bot;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.openplacereviews.osm.OsmLocationTool;
import org.openplacereviews.osm.OsmParser;
import org.openplacereviews.osm.model.DiffEntity;
import org.openplacereviews.osm.model.Entity;
import org.openplacereviews.osm.model.EntityInfo;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.xmlpull.v1.XmlPullParserException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static org.openplacereviews.opendb.ops.OpBlockchainRules.OP_TYPE_SYS;
import static org.openplacereviews.opendb.ops.OpObject.*;
import static org.openplacereviews.opendb.ops.OpOperation.F_DELETE;
import static org.openplacereviews.opendb.ops.OpOperation.F_TYPE;
import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;
import static org.openplacereviews.osm.model.EntityInfo.*;

public class BotPlacePublisher {

	protected static final Log LOGGER = LogFactory.getLog(BotPlacePublisher.class);
	protected static String SETTINGS_TABLE = "opendb_settings";
	protected static String SETTINGS_TABLE_SYNC = "db.sync";

	public static final String OP_BOT = OP_TYPE_SYS + "bot";
	public static final String ATTR_OSM = "osm";
	public static final String ATTR_TAGS = "tags";
	public static final String ATTR_SET = "set";
	public static final String ATTR_OSM_TAGS = "osm.tags.";

	public static final String F_SYNC_STATES = "sync-states";
	public static final String F_DATE = "date";
	public static final String F_DIFF = "diff";

	private Integer placesPerOperation;
	private Integer operationsPerBlock;
	private String osmOpType;
	private String timestamp;
	private String overpassURL;
	private String overpassTimestampURL;
	private volatile long opCounter;
	private volatile long appStartedMs;

	private BlocksManager blocksManager;
	private OpObject botObject;
	private JdbcTemplate jdbcTemplate;

	public BotPlacePublisher(BlocksManager blocksManager, OpObject botObject, JdbcTemplate jdbcTemplate) {
		this.blocksManager = blocksManager;
		this.botObject = botObject;
		this.jdbcTemplate = jdbcTemplate;
		init();
	}

	protected void init() {
		OpObject crawler = getOpOprCrawler();
		osmOpType = crawler.getStringValue("osmOpType");
		overpassURL = crawler.getStringValue("overpassURL");
		overpassTimestampURL = crawler.getStringValue("timestampURL");
		placesPerOperation = crawler.getIntValue("places_per_operation", 0);
		operationsPerBlock = crawler.getIntValue("operations_per_block", 0);
		//timestamp = "2019-06-01T00:00:00Z";
		try {
			timestamp = getTimestamp();
		} catch (IOException e) {
			LOGGER.error("Error while getting timestamp", e);
		}
	}

	public void publish() {
		SyncStatus statusSync = getSyncStatus(timestamp);
		if (statusSync.equals(SyncStatus.DIFF) || statusSync.equals(SyncStatus.NEW)) {
			LOGGER.info("Start synchronizing");
			OsmParser osmParser;
			Reader reader;
			try {
				reader = retrieveFile(timestamp);
				osmParser = new OsmParser(reader);
			} catch (XmlPullParserException | IOException e) {
				LOGGER.error(e.getMessage());
				throw new IllegalArgumentException("Error while creating osm parser", e);
			}

			publish(osmParser, statusSync);
			try {
				if (!Thread.currentThread().isInterrupted()) {
					blocksManager.addOperation(generateEditOpForOpOprCrawler(timestamp));
					TreeMap<String, Object> dbSync = new TreeMap<>();
					dbSync.put(F_DATE, timestamp);
					dbSync.put(ATTR_TAGS, botObject.getListStringObjMap(F_SYNC_STATES).get(0).get(ATTR_TAGS));
					saveNewSyncState(dbSync);
				}
			} catch (FailedVerificationException e) {
				LOGGER.error(e.getMessage());
				throw new IllegalArgumentException("Error while updating op opr.crawler", e);
			}

			try {
				reader.close();
			} catch (IOException e) {
				LOGGER.error(e.getMessage());
				throw new IllegalArgumentException("Cannot to close stream", e);
			}
			LOGGER.info("Synchronization is finished");
		} else if (statusSync.equals(SyncStatus.DB_NOT_SYNC)) {
			LOGGER.info("DB is not synchronized!");
			LOGGER.info("Start db syncing!");

			// TODO
			LOGGER.info("DB syncing is finished!");
		} else {
			LOGGER.info("Blockchain and DB is synchronized");
		}
	}

	protected void publish(OsmParser osmParser, SyncStatus statusSync) {
		appStartedMs = System.currentTimeMillis();
		OpBlockChain.ObjectsSearchRequest objectsSearchRequest = new OpBlockChain.ObjectsSearchRequest();
		blocksManager.getBlockchain().getObjectHeaders(osmOpType, objectsSearchRequest);
		HashSet<List<String>> osmPlaceHeaders = objectsSearchRequest.resultWithHeaders;
		while (!Thread.currentThread().isInterrupted()) {
			try {
				Object places;
				if (statusSync.equals(SyncStatus.NEW)) {
					places = osmParser.parseNextCoordinatePlaces(placesPerOperation, false);
				} else {
					places = osmParser.parseNextCoordinatePlaces(placesPerOperation, true);
				}
				if (places == null || ((List)places).isEmpty())
					break;

				List<Object> objectList = (List<Object>) places;
				List<Object> removeCollection = new LinkedList<>();
				for (Object o : objectList) {
					if (o instanceof Entity) {
						Entity entity = (Entity) o;
						List<String> strId = Collections.singletonList(OsmLocationTool.generateStrId(entity.getLatLon(), entity.getId()));
						if (osmPlaceHeaders.contains(strId)) {
							removeCollection.add(o);
						} else {
							osmPlaceHeaders.add(strId);
						}
					} else if (o instanceof DiffEntity) {
						DiffEntity diffEntity = (DiffEntity) o;
						if (diffEntity.getType().equals(DiffEntity.DiffEntityType.CREATE)) {
							List<String> strId = Collections.singletonList(OsmLocationTool.generateStrId(diffEntity.getNewNode().getLatLon(), diffEntity.getNewNode().getId()));
							if (osmPlaceHeaders.contains(strId)) {
								removeCollection.add(o);
							} else {
								osmPlaceHeaders.add(strId);
							}
						}
					}
				}
				objectList.removeAll(removeCollection);

				publish(places);
			} catch (Exception e) {
				LOGGER.error("Error while publishing", e);
				break;
			}
		}
		LOGGER.info("Amount of operation transferred data: " + opCounter);
	}

	protected void publish(Object places) throws FailedVerificationException {
		List<OpOperation> osmCoordinatePlacesDto = generateOpOperationFromPlaceList((List<Object>) places);
		try {
			for (OpOperation opOperation : osmCoordinatePlacesDto) {
				blocksManager.addOperation(opOperation);
				opCounter += 1;
			}
		} catch (Exception e) {
			LOGGER.error("Error occurred while posting places: " + e.getMessage());
			LOGGER.info("Amount of pack transferred data: " + opCounter);
		}

		try {
			if (!blocksManager.getBlockchain().getQueueOperations().isEmpty()) {
				boolean createBlock = (opCounter % operationsPerBlock == 0);
				if (createBlock) {
					blocksManager.createBlock();
					LOGGER.info("Op saved: " + opCounter + " date: " + new Date());
					LOGGER.info("Places saved: " + (opCounter * placesPerOperation) + " date: " + new Date());
					long currTimeMs = System.currentTimeMillis();
					long placesPerSecond = (opCounter * placesPerOperation) / ((currTimeMs - appStartedMs) / 1000);
					LOGGER.info("Places per second: " + placesPerSecond);
				}
			}
		} catch (Exception e) {
			LOGGER.error("Error occurred while creating block: " +e.getMessage());
		}

	}

	protected List<OpOperation> generateOpOperationFromPlaceList(List<Object> places) throws FailedVerificationException {
		List<OpOperation> opOperations = new ArrayList<>();

		OpOperation createOperation = null;
		OpOperation deleteOperation = null;
		OpOperation editOperation = null;

		for (Object e : places) {
			if (e instanceof Entity) {
				Entity obj = (Entity) e;
				createOperation = initOpOperation(createOperation);
				createOperation.addCreated(generateCreateOpObject(obj));
			} else if (e instanceof DiffEntity) {
				DiffEntity diffEntity = (DiffEntity) e;
				if (diffEntity.getType().equals(DiffEntity.DiffEntityType.DELETE)) {
					deleteOperation = initOpOperation(deleteOperation);
					List<List<String>> deletedObjs = deleteOperation.getDeleted();
					if (deletedObjs.isEmpty()) {
						deletedObjs = new ArrayList<>();
					}
					deleteOperation.putObjectValue(F_DELETE, generateDeleteOpObject(deletedObjs, diffEntity.getOldNode()));
				} else if (diffEntity.getType().equals(DiffEntity.DiffEntityType.CREATE)) {
					createOperation = initOpOperation(createOperation);
					createOperation.addCreated(generateCreateOpObject(diffEntity.getNewNode()));
				} else if (diffEntity.getType().equals(DiffEntity.DiffEntityType.MODIFY)) {
					OpObject edit = new OpObject();
					if (diffEntity.getNewNode().getLatLon().equals(diffEntity.getOldNode().getLatLon())) {
						editOperation = initOpOperation(editOperation);
						editOperation.addEdited(generateEditOpObject(edit, diffEntity));
					} else {
						deleteOperation = initOpOperation(deleteOperation);
						List<List<String>> deletedObjs = deleteOperation.getDeleted();
						if (deletedObjs.isEmpty()) {
							deletedObjs = new ArrayList<>();
						}
						deleteOperation.putObjectValue(F_DELETE, generateDeleteOpObject(deletedObjs, diffEntity.getOldNode()));

						createOperation = initOpOperation(createOperation);
						createOperation.addCreated(generateCreateOpObject(diffEntity.getNewNode()));
					}
				}
			}
		}

		generateHashAndSign(opOperations, deleteOperation);
		generateHashAndSign(opOperations, createOperation);
		generateHashAndSign(opOperations, editOperation);

		return opOperations;
	}

	private List<List<String>> generateDeleteOpObject(List<List<String>> deletedObjs, Entity entity) {
		deletedObjs.add(Collections.singletonList(OsmLocationTool.generateStrId(entity.getLatLon(), entity.getId())));
		return deletedObjs;
	}

	private OpObject generateCreateOpObject(Entity entity) {
		OpObject create = new OpObject();
		create.setId(OsmLocationTool.generateStrId(entity.getLatLon(), entity.getId()));
		TreeMap<String, Object> osmObject = new TreeMap<>();
		osmObject.put(F_ID, String.valueOf(entity.getId()));
		osmObject.put(F_TYPE, entity.getClass().getSimpleName().toLowerCase());
		osmObject.put(ATTR_TAGS, entity.getTags());
		osmObject.put(ATTR_LONGITUDE, entity.getLongitude());
		osmObject.put(ATTR_LATITUDE, entity.getLatitude());

		if (entity.getEntityInfo() != null) {
			generateEntityInfo(osmObject, entity.getEntityInfo());
		}

		create.putObjectValue(ATTR_OSM, osmObject);
		return create;
	}

	private OpObject generateEditOpObject(OpObject edit, DiffEntity diffEntity) {
		edit.setId(OsmLocationTool.generateStrId(diffEntity.getNewNode().getLatLon(), diffEntity.getNewNode().getId()));
		TreeMap<String, Object> changeTagMap = new TreeMap<>();
		TreeMap<String, String> currentTagMAp = new TreeMap<>();
		Map<String, String> tempTags = diffEntity.getOldNode().getTags();
		for (Map.Entry<String, String> newEntry : diffEntity.getNewNode().getTags().entrySet()) {
			if (tempTags.containsKey(newEntry.getKey())) {
				if (!newEntry.getValue().equals(tempTags.get(newEntry.getKey()))) {
					TreeMap<String, String> setValue = new TreeMap<>();
					setValue.put(ATTR_SET, newEntry.getValue());
					changeTagMap.put(ATTR_OSM_TAGS + newEntry.getKey(), setValue);
					currentTagMAp.put(ATTR_OSM_TAGS + newEntry.getKey(), tempTags.get(newEntry.getKey()));
				}
			} else {
				TreeMap<String, String> setValue = new TreeMap<>();
				setValue.put(ATTR_SET, newEntry.getValue());
				changeTagMap.put(ATTR_OSM_TAGS + newEntry.getKey(), setValue);
			}
		}
		tempTags = diffEntity.getNewNode().getTags();
		for (Map.Entry<String, String> oldEntry : diffEntity.getOldNode().getTags().entrySet()) {
			if (!tempTags.containsKey(oldEntry.getKey())) {
				changeTagMap.put(ATTR_OSM_TAGS + oldEntry.getKey(), F_DELETE);
				currentTagMAp.put(ATTR_OSM_TAGS + oldEntry.getKey(), oldEntry.getValue());
			}
		}

		edit.putObjectValue(F_CHANGE, changeTagMap);
		edit.putObjectValue(F_CURRENT, currentTagMAp);

		return edit;
	}

	private OpOperation initOpOperation(OpOperation opOperation) {
		if (opOperation == null) {
			opOperation = new OpOperation();
			opOperation.setType(osmOpType);
			opOperation.setSignedBy(blocksManager.getServerUser());
		}
		return opOperation;
	}

	private void generateHashAndSign(List<OpOperation> opOperations, OpOperation opOperation) throws FailedVerificationException {
		if (opOperation != null) {
			String obj = blocksManager.getBlockchain().getRules().getFormatter().opToJson(opOperation);
			opOperation = blocksManager.getBlockchain().getRules().getFormatter().parseOperation(obj);
			opOperation = blocksManager.generateHashAndSign(opOperation, blocksManager.getServerLoginKeyPair());
			opOperations.add(opOperation);
		}
	}

	private void generateEntityInfo(TreeMap<String, Object> osmObject, EntityInfo entityInfo) {
		osmObject.put(ATTR_TIMESTAMP, entityInfo.getTimestamp());
		osmObject.put(ATTR_UID, entityInfo.getUid());
		osmObject.put(ATTR_USER, entityInfo.getUser());
		osmObject.put(ATTR_VERSION, entityInfo.getVersion());
		osmObject.put(ATTR_CHANGESET, entityInfo.getChangeset());
		osmObject.put(ATTR_VISIBLE, entityInfo.getVisible());
		osmObject.put(ATTR_ACTION, entityInfo.getAction());
	}

	public enum SyncStatus {
		DB_NOT_SYNC,
		SYNC,
		NEW,
		DIFF
	}

	private SyncStatus getSyncStatus(String timestamp) {
		Map<String, Object> syncStates = getOpOprCrawler().getListStringObjMap(F_SYNC_STATES).get(0);
		Map<String, Object> dbSyncState = getDBSyncState();

		// TODO add check for tags
		if (dbSyncState == null) {
			if (syncStates.get(F_DATE).equals("")) {
				return SyncStatus.NEW;
			} else {
				return SyncStatus.DB_NOT_SYNC;
			}
		} else {
			if (!dbSyncState.get(F_DATE).equals(syncStates.get(F_DATE))) {
				return SyncStatus.DB_NOT_SYNC;
			} else if (dbSyncState.get(F_DATE).equals(timestamp) && syncStates.get(F_DATE).equals(timestamp)) {
				return SyncStatus.SYNC;
			} else if (!syncStates.get(F_DATE).equals(timestamp)) {
				return SyncStatus.DIFF;
			}
		}

		return SyncStatus.SYNC;
	}

	private Reader retrieveFile(String timestamp) throws IOException {
		String subRequest = "[out:xml][timeout:25][%s:\"%s\"];(%s); out body; >; out geom meta;";
		String request;
		List<Map<String, Object>> mapList = getOpOprCrawler().getListStringObjMap(F_SYNC_STATES);
		Map<String, String> states = (Map<String, String>) ((Map<String, Object>) mapList.get(0)).get(ATTR_TAGS);
		StringBuilder tags = new StringBuilder();
		for (Map.Entry<String, String> e : states.entrySet()) {
			tags.append(e.getKey()).append(e.getValue()).append(";");
		}

		if (getOpOprCrawler().getListStringObjMap(F_SYNC_STATES).get(0).get(F_DATE).equals("")) {
			request = String.format(subRequest, F_DATE, timestamp, tags);
		} else {
			request = String.format(subRequest, F_DIFF, getOpOprCrawler().getListStringObjMap(F_SYNC_STATES).get(0).get(F_DATE) + "\",\"" + timestamp, tags);
		}

		request = URLEncoder.encode(request, StandardCharsets.UTF_8.toString());
		request = overpassURL+ "?data=" + request;

		URL url = new URL(request);
		HttpURLConnection con = (HttpURLConnection) url.openConnection();
		LOGGER.info("Sending 'GET' request to URL : " + url);
		LOGGER.info("Response Code : " + con.getResponseCode());

		GZIPInputStream gzis = new GZIPInputStream(con.getInputStream());
		return new BufferedReader(new InputStreamReader(gzis));

	}

	private OpObject getOpOprCrawler() {
		return botObject;
	}

	private OpOperation generateEditOpForOpOprCrawler(String timestamp) throws FailedVerificationException {
		OpOperation opOperation = new OpOperation();
		opOperation.setType(OP_BOT);
		opOperation.setSignedBy(blocksManager.getServerUser());
		OpObject editObject = new OpObject();
		editObject.setId(getOpOprCrawler().getId().get(0));

		TreeMap<String, Object> changeDate = new TreeMap<>();
		TreeMap<String, String> setDate = new TreeMap<>();
		setDate.put(ATTR_SET, timestamp);
		changeDate.put(F_SYNC_STATES + "[0]." + F_DATE, setDate);
		editObject.putObjectValue(F_CHANGE, changeDate);

		TreeMap<String, String> previousDate = new TreeMap<>();
		previousDate.put(F_SYNC_STATES + "[0]." + F_DATE, (String) getOpOprCrawler().getListStringObjMap(F_SYNC_STATES).get(0).get(F_DATE));
		editObject.putObjectValue(F_CURRENT, previousDate);

		opOperation.addEdited(editObject);
		blocksManager.generateHashAndSign(opOperation, blocksManager.getServerLoginKeyPair());

		return opOperation;
	}

	private String getTimestamp() throws IOException {
		URL url = new URL(overpassTimestampURL);
		HttpURLConnection con = (HttpURLConnection) url.openConnection();

		int responseCode = con.getResponseCode();
		LOGGER.debug("Sending 'GET' request to URL : " + url);
		LOGGER.debug("Response Code : " + responseCode);

		return IOUtils.toString(con.getInputStream(), StandardCharsets.UTF_8);
	}

	private boolean saveNewSyncState(TreeMap<String, Object> v) {
		String value = blocksManager.getBlockchain().getRules().getFormatter().fullObjectToJson(v);
		return jdbcTemplate.update("insert into  " + SETTINGS_TABLE + "(key,value) values (?, ?) "
				+ " ON CONFLICT (key) DO UPDATE SET value = ? ", SETTINGS_TABLE_SYNC, value, value) != 0;
	}

	private TreeMap<String, Object> getDBSyncState() {
		String s = null;
		try {
			s = jdbcTemplate.query("select value from " + SETTINGS_TABLE + " where key = ?", new ResultSetExtractor<String>() {

				@Override
				public String extractData(ResultSet rs) throws SQLException, DataAccessException {
					boolean next = rs.next();
					if (next) {
						return rs.getString(1);
					}
					return null;
				}
			}, SETTINGS_TABLE_SYNC);
		} catch (DataAccessException e) {
			return null;
		}

		return blocksManager.getBlockchain().getRules().getFormatter().fromJsonToTreeMap(s);
	}
}
