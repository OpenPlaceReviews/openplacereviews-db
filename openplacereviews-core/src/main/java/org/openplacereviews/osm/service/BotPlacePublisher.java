package org.openplacereviews.osm.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.openplacereviews.osm.db.DbManager;
import org.openplacereviews.osm.parser.OsmLocationTool;
import org.openplacereviews.osm.parser.OsmParser;
import org.openplacereviews.osm.model.DiffEntity;
import org.openplacereviews.osm.model.Entity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.xmlpull.v1.XmlPullParserException;

import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.*;

import static org.openplacereviews.osm.util.ObjectGenerator.*;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.OP_TYPE_SYS;
import static org.openplacereviews.opendb.ops.OpObject.F_CHANGE;
import static org.openplacereviews.opendb.ops.OpObject.F_CURRENT;
import static org.openplacereviews.opendb.ops.OpOperation.F_DELETE;

public class BotPlacePublisher implements Callable {

	private static final Log LOGGER = LogFactory.getLog(BotPlacePublisher.class);

	public static final String OP_BOT = OP_TYPE_SYS + "bot";
	public static final String ATTR_OSM = "osm";
	public static final String ATTR_TAGS = "tags";
	public static final String ATTR_SET = "set";
	public static final String ATTR_OSM_TAGS = "osm.tags.";
	private static final String ATTR__SYNC_STATES = "sync-states";

	private static final String F_DATE = "date";
	private static final String F_DIFF = "diff";

	private Long placesPerOperation;
	private Long operationsPerBlock;
	private Long maxThreads;
	private String opType;
	private String timestamp;
	private volatile HashSet<List<String>> osmPlaceHeaders;

	private BlocksManager blocksManager;
	private OpObject botObject;
	private RequestService requestService;
	private DbManager dbManager;

	public BotPlacePublisher(BlocksManager blocksManager, OpObject botObject, JdbcTemplate jdbcTemplate) {
		this.blocksManager = blocksManager;
		this.botObject = botObject;
		this.dbManager = new DbManager(blocksManager, jdbcTemplate);
		init();
	}

	private void init() {
		this.requestService = new RequestService(
				botObject.getStringMap("url").get("overpass"),
				botObject.getStringMap("url").get("overpass_timestamp"));
		this.opType = ((Map<String, String>)botObject.getStringObjMap("bot").get("opr")).get("type");
		this.placesPerOperation = (Long) ((Map<String, Object>)botObject.getStringObjMap("bot").get("opr")).get("places_per_operation");
		this.operationsPerBlock = (Long) ((Map<String, Object>)botObject.getStringObjMap("bot").get("opr")).get("operations_per_block");
		this.maxThreads = (Long) botObject.getStringObjMap("bot").get("threads");
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
			List<String> requests = new ArrayList<>(generateRequests(timestamp, syncStatus));
			List<Future<?>> futures = new ArrayList<>();
			ExecutorService service = Executors.newFixedThreadPool(maxThreads.intValue());
			OpBlockChain.ObjectsSearchRequest objectsSearchRequest = new OpBlockChain.ObjectsSearchRequest();
			blocksManager.getBlockchain().getObjectHeaders(opType, objectsSearchRequest);
			osmPlaceHeaders = objectsSearchRequest.resultWithHeaders;
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
						dbSync.put(F_DATE, botObject.getListStringObjMap(ATTR__SYNC_STATES).get(0).get(F_DATE));
					}

					dbSync.put(ATTR_TAGS, botObject.getListStringObjMap(ATTR__SYNC_STATES).get(0).get(ATTR_TAGS));
					dbManager.saveNewSyncState(dbSync);
				}
			} catch (FailedVerificationException e) {
				LOGGER.error(e.getMessage());
				throw new IllegalArgumentException("Error while updating op opr.crawler", e);
			}
		} else if (syncStatus.equals(SyncStatus.DB_SYNC)) {
			LOGGER.info("DB is not synchronized!");
			LOGGER.info("Start db syncing!");

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

	private class Publisher implements Callable {

		private volatile long opCounter;
		private volatile long allOperationsCounter;
		private volatile long appStartedMs;
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
			publish(osmParser, syncStatus, osmPlaceHeaders);
			try {
				file.close();
			} catch (IOException e) {
				LOGGER.error(e.getMessage());
				throw new IllegalArgumentException("Error while stream closing", e);
			}
		}

		private void publish(OsmParser osmParser, SyncStatus statusSync, HashSet<List<String>> osmPlaceHeaders) {
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

					removeDuplicates(places, osmPlaceHeaders);
					publish(places);
				} catch (Exception e) {
					LOGGER.error("Error while publishing", e);
					break;
				}
			}
			LOGGER.debug("Amount of operation transferred data: " + opCounter);
		}

		private void publish(Object places) throws FailedVerificationException {
			List<OpOperation> osmCoordinatePlacesDto = generateOpOperationFromPlaceList((List<Object>) places);
			try {
				for (OpOperation opOperation : osmCoordinatePlacesDto) {
					blocksManager.addOperation(opOperation);
					opCounter += 1;
				}
				allOperationsCounter += 1;
			} catch (Exception e) {
				LOGGER.error("Error occurred while posting places: " + e.getMessage());
				LOGGER.info("Amount of pack transferred data: " + opCounter);
			}

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

		private void removeDuplicates(Object places, HashSet<List<String>> osmPlaceHeaders) {
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
					if (DiffEntity.DiffEntityType.CREATE.equals(diffEntity.getType())) {
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
					createOperation.addCreated(generateCreateOpObject(obj));
				} else if (e instanceof DiffEntity) {
					DiffEntity diffEntity = (DiffEntity) e;
					if (diffEntity.getType().equals(DiffEntity.DiffEntityType.DELETE)) {
						deleteOperation = initOpOperation(deleteOperation, opType, blocksManager);
						List<List<String>> deletedObjs = deleteOperation.getDeleted();
						if (deletedObjs.isEmpty()) {
							deletedObjs = new ArrayList<>();
						}
						deleteOperation.putObjectValue(F_DELETE, generateDeleteOpObject(deletedObjs, diffEntity.getOldNode()));
					} else if (diffEntity.getType().equals(DiffEntity.DiffEntityType.CREATE)) {
						createOperation = initOpOperation(createOperation, opType, blocksManager);
						createOperation.addCreated(generateCreateOpObject(diffEntity.getNewNode()));
					} else if (diffEntity.getType().equals(DiffEntity.DiffEntityType.MODIFY)) {
						OpObject edit = new OpObject();
						if (diffEntity.getNewNode().getLatLon().equals(diffEntity.getOldNode().getLatLon())) {
							editOperation = initOpOperation(editOperation, opType, blocksManager);
							editOperation.addEdited(generateEditOpObject(edit, diffEntity));
						} else {
							deleteOperation = initOpOperation(deleteOperation, opType, blocksManager);
							List<List<String>> deletedObjs = deleteOperation.getDeleted();
							if (deletedObjs.isEmpty()) {
								deletedObjs = new ArrayList<>();
							}
							deleteOperation.putObjectValue(F_DELETE, generateDeleteOpObject(deletedObjs, diffEntity.getOldNode()));

							createOperation = initOpOperation(createOperation, opType, blocksManager);
							createOperation.addCreated(generateCreateOpObject(diffEntity.getNewNode()));
						}
					}
				}
			}

			generateHashAndSign(opOperations, deleteOperation, blocksManager);
			generateHashAndSign(opOperations, createOperation, blocksManager);
			generateHashAndSign(opOperations, editOperation, blocksManager);

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
		Map<String, Object> syncStates = botObject.getListStringObjMap(ATTR__SYNC_STATES).get(0);
		Map<String, Object> dbSyncState = dbManager.getDBSyncState();

		// TODO add check for tags -> added
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

	private List<String> generateRequests(String timestamp, SyncStatus syncStatus) throws UnsupportedEncodingException {
		List<String> requests = new ArrayList<>();

		Map<String, String> tagsCoordinates = new HashMap<>();
		Map<String, String> tagsBbox = new HashMap<>();
		List<Map<String, Object>> mapList = botObject.getListStringObjMap(ATTR__SYNC_STATES);
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
					if (!dbSyncState.containsKey(key) || dbSyncState.get(key).equals(states.get(key))) {
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
						if (!dbSyncState.containsKey(key) || dbSyncState.get(key).equals(states.get(key))) {
							tag.append(key).append(coordinate).append(";");
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
		if (botObject.getListStringObjMap(ATTR__SYNC_STATES).get(0).get(F_DATE).equals("") || SyncStatus.TAGS_SYNC.equals(syncStatus)) {
			requests.add(requestService.generateRequestString(tag.toString(), F_DATE, timestamp));
		} else {
			requests.add(requestService.generateRequestString(tag.toString(), F_DIFF, botObject.getListStringObjMap(ATTR__SYNC_STATES).get(0).get(F_DATE) + "\",\"" + timestamp));
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
		changeDate.put(ATTR__SYNC_STATES + "[0]." + F_DATE, setDate);
		editObject.putObjectValue(F_CHANGE, changeDate);

		TreeMap<String, String> previousDate = new TreeMap<>();
		previousDate.put(ATTR__SYNC_STATES + "[0]." + F_DATE, (String) botObject.getListStringObjMap(ATTR__SYNC_STATES).get(0).get(F_DATE));
		editObject.putObjectValue(F_CURRENT, previousDate);

		opOperation.addEdited(editObject);
		blocksManager.generateHashAndSign(opOperation, blocksManager.getServerLoginKeyPair());

		return opOperation;
	}

	private List<String> generateListCoordinates() {
		double quad_size_length = 360/32d;
		double quad_size_height = 180/16d;
		double p1 = -90, p2 = -180;
		List<String> coordinates = new ArrayList<>();
		while (p2 != 180) {
			double t = p1 + quad_size_height;
			double t1 = p2 + quad_size_length;
			coordinates.add(p1 + ", " + p2 + ", " + t + ", " + t1);
			p1 += quad_size_height;
			if (p1 == 90) {
				p1 = -90;
				p2 += quad_size_length;
			}
		}

		return coordinates;
	}


}
