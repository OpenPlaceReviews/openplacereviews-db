package org.openplacereviews.osm.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.IPublishBot;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.openplacereviews.osm.db.DbManager;
import org.openplacereviews.osm.model.DiffEntity;
import org.openplacereviews.osm.model.Entity;
import org.openplacereviews.osm.parser.OsmParser;
import org.springframework.jdbc.core.JdbcTemplate;
import org.xmlpull.v1.XmlPullParserException;

import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.*;

import static org.openplacereviews.opendb.ops.OpBlockchainRules.F_TYPE;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.OP_TYPE_SYS;
import static org.openplacereviews.opendb.ops.OpObject.*;
import static org.openplacereviews.opendb.ops.OpOperation.F_DELETE;
import static org.openplacereviews.osm.util.ObjectGenerator.*;

public class PublishBot implements IPublishBot {

	private static final Log LOGGER = LogFactory.getLog(PublishBot.class);

	public static final String ATTR_BOT = "bot";
	public static final String ATTR_SOURCE = "source";
	public static final String ATTR_OLD_OSM_IDS = "old-osm-ids";
	public static final String ATTR_OSM = "osm";
	public static final String ATTR_TAGS = "tags";
	public static final String ATTR_SET = "set";
	public static final String ATTR_OPR = "opr";
	public static final String ATTR_SOURCE_OSM_TAGS = "source.osm[0].tags.";
	public static final String ATTR_SYNC_STATES = "sync-states";

	private static final String F_DATE = "date";
	private static final String F_DIFF = "diff";
	public static final String F_MATCH_ID = "match_id";
	private static final String F_URL = "url";
	private static final String F_TIMESTAMP = "overpass_timestamp";
	private static final String F_OVERPASS = "overpass";
	private static final String F_THREADS = "threads";
	private static final String F_PLACES_PER_OPERATION = "places_per_operation";
	private static final String F_OPERATIONS_PER_BLOCK = "operations_per_block";
	public static final String F_RULES = "rules";
	public static final String F_MAPPING = "mapping";

	public static final String OP_BOT = OP_TYPE_SYS + ATTR_BOT;

	private Long placesPerOperation;
	private Long operationsPerBlock;
	private String opType;
	private String timestamp;

	private volatile BlocksManager blocksManager;
	private OpObject botObject;
	private RequestService requestService;
	private DbManager dbManager;
	private PlaceManager placeManager;

	List<Future<?>> futures = new ArrayList<>();
	ExecutorService service;

	public PublishBot(BlocksManager blocksManager, OpObject botObject, JdbcTemplate jdbcTemplate) {
		this.blocksManager = blocksManager;
		this.botObject = botObject;
		this.dbManager = new DbManager(blocksManager, jdbcTemplate);
		this.placeManager = new PlaceManager(blocksManager, botObject);
		init();
	}

	private void init() {
		this.requestService = new RequestService(
				botObject.getStringMap(F_URL).get(F_OVERPASS),
				botObject.getStringMap(F_URL).get(F_TIMESTAMP));
		this.opType = ((Map<String, String>)botObject.getStringObjMap(ATTR_BOT).get(ATTR_OPR)).get(F_TYPE);
		this.placesPerOperation = (Long) ((Map<String, Object>)botObject.getStringObjMap(ATTR_BOT).get(ATTR_OPR)).get(F_PLACES_PER_OPERATION);
		this.operationsPerBlock = (Long) ((Map<String, Object>)botObject.getStringObjMap(ATTR_BOT).get(ATTR_OPR)).get(F_OPERATIONS_PER_BLOCK);
		Long maxThreads = (Long) botObject.getStringObjMap(ATTR_BOT).get(F_THREADS);
		this.service = Executors.newFixedThreadPool(maxThreads.intValue());
//		timestamp = "2019-06-01T00:00:00Z";
		try {
			this.timestamp = requestService.getTimestamp();
		} catch (IOException e) {
			LOGGER.error("Error while getting timestamp", e);
		}
	}

	public void publish() throws IOException, ExecutionException, InterruptedException {
		SyncStatus syncStatus = getSyncStatus(timestamp);
		if (syncStatus.equals(SyncStatus.DIFF_SYNC) || syncStatus.equals(SyncStatus.NEW_SYNC) || syncStatus.equals(SyncStatus.TAGS_SYNC)) {
			LOGGER.info("Start synchronizing: " + syncStatus);
			List<String> requests = new ArrayList<>(generateRequestList(timestamp, syncStatus));

			for (int i = 0; i < requests.size(); i++) {
				futures.add(service.submit(new Publisher(requests.get(i), syncStatus)));
			}

			for (Future future : futures) {
				Long amount = (Long) future.get();
			}

			LOGGER.info("Synchronization is finished");
			try {
				if (!Thread.currentThread().isInterrupted()) {
					TreeMap<String, Object> dbSync = new TreeMap<>();
					if (!SyncStatus.TAGS_SYNC.equals(syncStatus)) {
						blocksManager.addOperation(generateEditOpForOpOprCrawler(timestamp));
						dbSync.put(F_DATE, timestamp);
					} else {
						dbSync.put(F_DATE, botObject.getListStringObjMap(ATTR_SYNC_STATES).get(0).get(F_DATE));
					}

					dbSync.put(ATTR_TAGS, botObject.getListStringObjMap(ATTR_SYNC_STATES).get(0).get(ATTR_TAGS));
					dbManager.saveNewSyncState(dbSync);
				}
			} catch (FailedVerificationException e) {
				LOGGER.error(e.getMessage());
				throw new IllegalArgumentException("Error while updating op opr.crawler", e);
			}
		} else if (syncStatus.equals(SyncStatus.DB_SYNC)) {
			LOGGER.info("DB is not synchronized!");
			LOGGER.info("Start db syncing!");

			// TODO implement db syncing
			LOGGER.info("DB syncing is finished!");
		} else {
			LOGGER.info("Blockchain and DB is synchronized");
		}
	}

	@Override
	public Object call() throws Exception {
		publish();
		return null;
	}

	private synchronized void createBlock(long allOperationsCounter, long opCounter, long appStartedMs) {
		try {
			if (!blocksManager.getBlockchain().getQueueOperations().isEmpty()) {
				boolean createBlock = (allOperationsCounter % operationsPerBlock == 0 || blocksManager.getBlockchain().getQueueOperations().size() >= operationsPerBlock);
				if (createBlock) {
					blocksManager.createBlock();
					LOGGER.info("Op saved: " + opCounter + " date: " + new Date());
					LOGGER.info("Places saved: " + (allOperationsCounter * placesPerOperation) + " date: " + new Date());
					long currTimeMs = System.currentTimeMillis();
					long placesPerSecond = (opCounter * placesPerOperation) / ((currTimeMs - appStartedMs) / 1000);
					LOGGER.info("Places per second: " + placesPerSecond);
				}
			}
		} catch (Exception e) {
			LOGGER.error("Error occurred while creating block: " +e.getMessage());
		}
	}

	private class Publisher implements Callable {

		private long opCounter;
		private long allOperationsCounter;
		private long appStartedMs;
		private String request;
		private SyncStatus syncStatus;

		public Publisher(String request, SyncStatus syncStatus) {
			this.request = request;
			this.syncStatus = syncStatus;
		}

		@Override
		public Long call() {
			startPublish();
			return allOperationsCounter;
		}

		private void startPublish() {
			Reader file = null;
			try {
				file = requestService.retrieveFile(request);
			} catch (IOException e) {
				LOGGER.error("Error while getting file", e);
			}
			OsmParser osmParser = null;
			try {
				osmParser = new OsmParser(file);
			} catch (XmlPullParserException e) {
				LOGGER.error("Error while reading file", e);
			}
			publish(osmParser, syncStatus);
			try {
				file.close();
			} catch (IOException e) {
				LOGGER.error(e.getMessage());
				throw new IllegalArgumentException("Error while stream closing", e);
			}
		}

		private void publish(OsmParser osmParser, SyncStatus statusSync) {
			appStartedMs = System.currentTimeMillis();
			while (!Thread.currentThread().isInterrupted()) {
				try {
					Object places;
					if (SyncStatus.NEW_SYNC.equals(statusSync) || SyncStatus.TAGS_SYNC.equals(statusSync)) {
						places = osmParser.parseNextCoordinatePlaces(placesPerOperation, false);
					} else {
						places = osmParser.parseNextCoordinatePlaces(placesPerOperation, true);
					}
					if (places == null || ((List)places).isEmpty())
						break;

					publish(places);
				} catch (Exception e) {
					LOGGER.error("Error while publishing", e);
					break;
				}
			}
		}

		private void publish(Object places) throws FailedVerificationException {
			// TODO change logs!!!
			//LOGGER.info(request + "     AMOUNT" + ((List<Object>) places).size());
			List<OpOperation> osmCoordinatePlacesDto = generateOpOperationFromPlaceList((List<Object>) places);
			try {
				for (OpOperation opOperation : osmCoordinatePlacesDto) {
					blocksManager.addOperation(opOperation);
					opCounter += 1;
				}
				allOperationsCounter += 1;
			} catch (Exception e) {
				e.printStackTrace();
				LOGGER.error("Error occurred while posting places: " + e.getMessage());
				LOGGER.info("Amount of pack transferred data: " + allOperationsCounter);
			}

			createBlock(allOperationsCounter, opCounter, appStartedMs);
		}

		private List<OpOperation> generateOpOperationFromPlaceList(List<Object> places) throws FailedVerificationException {
			List<OpOperation> opOperations = new ArrayList<>();

			OpOperation createOperation = null;
			OpOperation deleteOperation = null;
			OpOperation editOperation = null;

			for (Object e : places) {
				if (e instanceof Entity) {
					Entity obj = (Entity) e;
					createOperation = initOpOperation(createOperation, opType, blocksManager);

					// TODO load object by osm id, if not exist -> create, else check matchId equals -> skip, else -> create new Object
					OpObject objExist = placeManager.getObjectByExtId(String.valueOf(obj.getId()));
					if (objExist == null) {
						OpObject createOpObject = generateCreateOpObject(obj);
						createOperation.addCreated(createOpObject);
					} else {
						// TODO add check match ID if matchIds equals skip, else create new
						OpObject newOpObject = generateCreateOpObject(obj);
						String matchId = placeManager.generateMatchIdFromOpObject(newOpObject);
						String oldMatchId = placeManager.generateMatchIdFromOpObject(objExist);
						if (!Objects.equals(matchId, oldMatchId)) {
							// TODO create with same OSM-ID??
							createOperation.addCreated(newOpObject);
						}
					}

//					OpObject newOpObject = generateCreateOpObject(obj);
//					String matchId = placeManager.generateMatchIdFromOpObject(newOpObject);
//					if (placeManager.getObjectByMatchId(matchId) == null) {
//						createOperation.addCreated(newOpObject);
//					}
				} else if (e instanceof DiffEntity) {
					DiffEntity diffEntity = (DiffEntity) e;
					if (DiffEntity.DiffEntityType.DELETE.equals(diffEntity.getType())) {
						deleteOperation = initOpOperation(deleteOperation, opType, blocksManager);
						List<List<String>> deletedObjs = deleteOperation.getDeleted();
						if (deletedObjs.isEmpty()) {
							deletedObjs = new ArrayList<>();
						}

						OpObject deleteObject =  placeManager.getObjectByExtId(String.valueOf(diffEntity.getOldNode().getId()));
						if (deleteObject != null) {
							deletedObjs.add(deleteObject.getId());
							deleteOperation.putObjectValue(F_DELETE, deletedObjs);
						}
					} else if (DiffEntity.DiffEntityType.CREATE.equals(diffEntity.getType())) {
						createOperation = initOpOperation(createOperation, opType, blocksManager);

						OpObject objExist = placeManager.getObjectByExtId(String.valueOf(diffEntity.getNewNode().getId()));
						if (objExist == null) {
							OpObject createObj = generateCreateOpObject(diffEntity.getNewNode());
							createOperation.addCreated(createObj);
						} else {
							// TODO add check match ID if matchIds equals skip, else create new
							String matchId = placeManager.generateMatchIdFromOpObject(objExist);
							OpObject newOpObject = generateCreateOpObject(diffEntity.getNewNode());
							String newMatchId = placeManager.generateMatchIdFromOpObject(newOpObject);
							if (!Objects.equals(matchId, newMatchId)) {
								// TODO create with same OSM-ID??
								createOperation.addCreated(newOpObject);
							}
						}

//						OpObject newOpObject = generateCreateOpObject(diffEntity.getNewNode());
//						String matchId = placeManager.generateMatchIdFromOpObject(newOpObject);
//						if (placeManager.getObjectByMatchId(matchId) == null) {
//							createOperation.addCreated(newOpObject);
//						}
					} else if (DiffEntity.DiffEntityType.MODIFY.equals(diffEntity.getType())) {
						OpObject edit = new OpObject();

						/// TODO refactor that code -> change check only by matchID ??
						if (diffEntity.getNewNode().getLatLon().equals(diffEntity.getOldNode().getLatLon())) {
							editOperation = initOpOperation(editOperation, opType, blocksManager);

							OpObject deleteObject = placeManager.getObjectByExtId(String.valueOf(diffEntity.getOldNode().getId()));
							if (deleteObject != null) {
								editOperation.addEdited(generateEditOpObject(edit, diffEntity, deleteObject.getId()));
							}
						} else {
							deleteOperation = initOpOperation(deleteOperation, opType, blocksManager);
							List<List<String>> deletedObjs = deleteOperation.getDeleted();
							if (deletedObjs.isEmpty()) {
								deletedObjs = new ArrayList<>();
							}

							List<OpObject> opObjects = blocksManager.getObjectByIndex("obj_opr_places", "osmid", "id", String.valueOf(diffEntity.getOldNode().getId()));
							if (!opObjects.isEmpty()) {
								deletedObjs.add(opObjects.get(0).getId());
								deleteOperation.putObjectValue(F_DELETE, deletedObjs);
							}

							createOperation = initOpOperation(createOperation, opType, blocksManager);
							opObjects = blocksManager.getObjectByIndex("obj_opr_places", "osmid", "id", String.valueOf(diffEntity.getNewNode().getId()));
							if (opObjects.isEmpty()) {
								OpObject createObj = generateCreateOpObject(diffEntity.getNewNode());
								createOperation.addCreated(createObj);
							} else {
								// TODO add check match ID if matchIds equals skip, else remove old create new
							}
						}
					}
				}
			}

			if (deleteOperation != null && deleteOperation.getDeleted().size() != 0) {
				generateHashAndSign(opOperations, deleteOperation, blocksManager);
			}
			if (createOperation != null && createOperation.getCreated().size() != 0) {
				generateHashAndSign(opOperations, createOperation, blocksManager);
			}
			if (editOperation != null && editOperation.getEdited().size() != 0) {
				generateHashAndSign(opOperations, editOperation, blocksManager);
			}

			return opOperations;
		}
	}

	public enum SyncStatus {
		DB_SYNC,
		TAGS_SYNC,
		SYNCHRONIZED,
		NEW_SYNC,
		DIFF_SYNC
	}

	private SyncStatus getSyncStatus(String timestamp) {
		Map<String, Object> syncStates = botObject.getListStringObjMap(ATTR_SYNC_STATES).get(0);
		Map<String, Object> dbSyncState = dbManager.getDBSyncState();

		if (dbSyncState == null) {
			if (syncStates.get(F_DATE).equals("")) {
				return SyncStatus.NEW_SYNC;
			} else {
				return SyncStatus.DB_SYNC;
			}
		} else {
			if (!dbSyncState.get(F_DATE).equals(syncStates.get(F_DATE))) {
				return SyncStatus.DB_SYNC;
			} else if (dbSyncState.get(F_DATE).equals(timestamp) && syncStates.get(F_DATE).equals(timestamp)) {
				if (!syncStates.get(ATTR_TAGS).equals(dbSyncState.get(ATTR_TAGS))) {
					return SyncStatus.TAGS_SYNC;
				}
				return SyncStatus.SYNCHRONIZED;
			} else if (!syncStates.get(F_DATE).equals(timestamp)) {
				return SyncStatus.DIFF_SYNC;
			}
		}

		return SyncStatus.SYNCHRONIZED;
	}

	private List<String> generateRequestList(String timestamp, SyncStatus syncStatus) throws UnsupportedEncodingException {
		List<String> requests = new ArrayList<>();

		Map<String, String> tagsCoordinates = new HashMap<>();
		Map<String, String> tagsBbox = new HashMap<>();
		List<Map<String, Object>> mapList = botObject.getListStringObjMap(ATTR_SYNC_STATES);
		Map<String, String> states = (Map<String, String>) ((Map<String, Object>) mapList.get(0)).get(ATTR_TAGS);

		for (Map.Entry<String, String> e : states.entrySet()) {
			if (e.getValue().equals("")) {
				tagsBbox.put(e.getKey(), e.getValue());
			} else {
				tagsCoordinates.put(e.getKey(), e.getValue());
			}
		}

		if (!tagsCoordinates.isEmpty()) {
			StringBuilder tags = new StringBuilder();
			if (SyncStatus.TAGS_SYNC.equals(syncStatus)) {
				Map<String, String> dbSyncState = (Map<String, String>) dbManager.getDBSyncState().get(ATTR_TAGS);
				for (String key : tagsCoordinates.keySet()) {
					if (!dbSyncState.containsKey(key) || !dbSyncState.get(key).equals(states.get(key))) {
						tags.append(key).append(states.get(key)).append(";");
					}
				}
			} else {
				for (Map.Entry<String, String> e : tagsCoordinates.entrySet()) {
					tags.append(e.getKey()).append(e.getValue()).append(";");
				}
			}
			addRequest(timestamp, syncStatus, requests, tags);
		}

		if (!tagsBbox.isEmpty()) {
			List<String> coordinates = generateListCoordinates();
			for (String coordinate : coordinates) {
				StringBuilder tag = new StringBuilder();
				if (SyncStatus.TAGS_SYNC.equals(syncStatus)) {
					Map<String, String> dbSyncState = (Map<String, String>) dbManager.getDBSyncState().get(ATTR_TAGS);
					for (String key : tagsBbox.keySet()) {
						if (!dbSyncState.containsKey(key) || !dbSyncState.get(key).equals(states.get(key))) {
							tag.append(key).append("(").append(coordinate).append(");");
						}
					}
				} else {
					for (Map.Entry<String, String> e : tagsBbox.entrySet()) {
						tag.append(e.getKey()).append("(").append(coordinate).append(");");
					}
				}
				addRequest(timestamp, syncStatus, requests, tag);
			}
		}

		return requests;
	}

	private void addRequest(String timestamp, SyncStatus syncStatus, List<String> requests, StringBuilder tag) throws UnsupportedEncodingException {
		if (botObject.getListStringObjMap(ATTR_SYNC_STATES).get(0).get(F_DATE).equals("") || SyncStatus.TAGS_SYNC.equals(syncStatus)) {
			requests.add(requestService.generateRequestString(tag.toString(), F_DATE, timestamp));
		} else {
			requests.add(requestService.generateRequestString(tag.toString(), F_DIFF, botObject.getListStringObjMap(ATTR_SYNC_STATES).get(0).get(F_DATE) + "\",\"" + timestamp));
		}
	}

	private OpOperation generateEditOpForOpOprCrawler(String timestamp) throws FailedVerificationException {
		OpOperation opOperation = new OpOperation();
		opOperation.setType(OP_BOT);
		opOperation.setSignedBy(blocksManager.getServerUser());
		OpObject editObject = new OpObject();
		editObject.setId(botObject.getId().get(0));

		TreeMap<String, Object> changeDate = new TreeMap<>();
		TreeMap<String, String> setDate = new TreeMap<>();
		setDate.put(ATTR_SET, timestamp);
		changeDate.put(ATTR_SYNC_STATES + "[0]." + F_DATE, setDate);
		editObject.putObjectValue(F_CHANGE, changeDate);

		TreeMap<String, String> previousDate = new TreeMap<>();
		previousDate.put(ATTR_SYNC_STATES + "[0]." + F_DATE, (String) botObject.getListStringObjMap(ATTR_SYNC_STATES).get(0).get(F_DATE));
		editObject.putObjectValue(F_CURRENT, previousDate);

		opOperation.addEdited(editObject);
		blocksManager.generateHashAndSign(opOperation, blocksManager.getServerLoginKeyPair());

		return opOperation;
	}

	private OpOperation generateEditOpDeleteOsmIds(List<String> placeId, String extId) throws FailedVerificationException {
		OpOperation opOperation = new OpOperation();
		opOperation.setType(opType);
		opOperation.setSignedBy(blocksManager.getServerUser());
		OpObject editObject = new OpObject();
		editObject.putObjectValue(F_ID, placeId);

		TreeMap<String, Object> addDeletedIds = new TreeMap<>();
		TreeMap<String, Object> appendOp = new TreeMap<>();
		appendOp.put("append", extId);
		addDeletedIds.put(ATTR_SOURCE + "." + ATTR_OLD_OSM_IDS, appendOp);

		opOperation.addEdited(editObject);
		blocksManager.generateHashAndSign(opOperation, blocksManager.getServerLoginKeyPair());

		return opOperation;
	}

}
