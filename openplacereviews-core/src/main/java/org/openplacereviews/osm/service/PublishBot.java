package org.openplacereviews.osm.service;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.IOpenDBBot;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.openplacereviews.osm.db.DbManager;
import org.openplacereviews.osm.model.DiffEntity;
import org.openplacereviews.osm.model.Entity;
import org.openplacereviews.osm.parser.OsmParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.xmlpull.v1.XmlPullParserException;

import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.*;

import static org.openplacereviews.opendb.ops.OpBlockchainRules.F_TYPE;
import static org.openplacereviews.opendb.ops.OpObject.*;
import static org.openplacereviews.osm.util.ObjectGenerator.*;

public class PublishBot implements IOpenDBBot {

	private static final Log LOGGER = LogFactory.getLog(PublishBot.class);

	public static final String ATTR_SOURCE = "source";
	public static final String ATTR_OLD_OSM_IDS = "old-osm-ids";
	public static final String ATTR_OSM = "osm";
	public static final String ATTR_TAGS = "tags";
	public static final String ATTR_SET = "set";
	public static final String ATTR_SOURCE_OSM_TAGS = "source.osm[0].tags.";
	public static final String ATTR_SYNC_STATES = "bot-state";

	public static final String F_DATE = "date";
	public static final String F_DIFF = "diff";

	public static final String F_CONFIG = "config";
	public static final String F_BLOCKCHAIN_CONFIG = "blockchain-config";
	public static final String F_URL = "url";
	public static final String F_TIMESTAMP = "overpass_timestamp";
	public static final String F_OVERPASS = "overpass";
	public static final String F_THREADS = "threads";
	public static final String F_PLACES_PER_OPERATION = "places_per_operation";
	public static final String F_OPERATIONS_PER_BLOCK = "operations_per_block";

	private Long placesPerOperation;
	private Long operationsPerBlock;
	private String opType;
	private String timestamp;

	@Autowired
	private BlocksManager blocksManager;
	@Autowired
	private JdbcTemplate jdbcTemplate;

	private OpObject botObject;
	private RequestService requestService;
	private DbManager dbManager;
	private PlaceManager placeManager;

	List<Future<?>> futures = new ArrayList<>();
	ThreadPoolExecutor service;

	public PublishBot(OpObject botObject) {
		this.botObject = botObject;
	}

	private void initVariables() {
		this.dbManager = new DbManager(blocksManager, jdbcTemplate);
		this.requestService = new RequestService(
				((Map<String, String>)botObject.getStringObjMap(F_CONFIG).get(F_URL)).get(F_OVERPASS),
				((Map<String, String>)botObject.getStringObjMap(F_CONFIG).get(F_URL)).get(F_TIMESTAMP));
		this.opType = botObject.getStringMap(F_BLOCKCHAIN_CONFIG).get(F_TYPE);
		this.placeManager = new PlaceManager(opType, blocksManager, botObject);
		this.placesPerOperation = (Long) botObject.getStringObjMap(F_BLOCKCHAIN_CONFIG).get(F_PLACES_PER_OPERATION);
		this.operationsPerBlock = (Long) botObject.getStringObjMap(F_BLOCKCHAIN_CONFIG).get(F_OPERATIONS_PER_BLOCK);

		ThreadFactory namedThreadFactory =
				new ThreadFactoryBuilder().setNameFormat(Thread.currentThread().getName() + "-%d").build();
		this.service = (ThreadPoolExecutor) Executors.newFixedThreadPool(botObject.getIntValue(F_THREADS, 0), namedThreadFactory);
//		timestamp = "2019-06-01T00:00:00Z";
		try {
			this.timestamp = requestService.getTimestamp();
		} catch (IOException e) {
			LOGGER.error("Error while getting timestamp", e);
		}
	}

	public void publish() throws IOException {
		initVariables();
		SyncStatus syncStatus = getSyncStatus(timestamp);
		if (!syncStatus.equals(SyncStatus.SYNCHRONIZED)) {
			LOGGER.info("Start synchronizing: " + syncStatus);
			List<String> requests = getRequestList(timestamp, syncStatus);

			for (int i = 0; i < requests.size(); i++) {
				futures.add(service.submit(new Publisher(requests.get(i), syncStatus)));
			}
			service.shutdown();
			for (Future future : futures) {
				try {
					Long amount = (Long) future.get();
					if (amount != 0) {
						LOGGER.info("Completed/total tasks " + service.getCompletedTaskCount() + "/" + service.getTaskCount()
								+ ", queue: " + service.getQueue().size() + " Added ops: " + amount);
					}
				} catch (Exception e) {
					LOGGER.error("Error while executing task", e);
				}
			}

			try {
				TreeMap<String, Object> dbSync = new TreeMap<>();
				if (!SyncStatus.TAGS_SYNC.equals(syncStatus) && !SyncStatus.DB_SYNC.equals(syncStatus)) {
					blocksManager.addOperation(generateEditOpForBotObject(timestamp, botObject, blocksManager));
					dbSync.put(F_DATE, timestamp);
				} else {
					dbSync.put(F_DATE, botObject.getStringMap(ATTR_SYNC_STATES).get(F_DATE));
				}

				dbSync.put(ATTR_TAGS, botObject.getStringObjMap(ATTR_SYNC_STATES).get(ATTR_TAGS));
				dbManager.saveNewSyncState(dbSync);
			} catch (FailedVerificationException e) {
				LOGGER.error(e.getMessage());
				throw new IllegalArgumentException("Error while updating op opr.crawler", e);
			}
			LOGGER.info("Synchronization is finished");
		} else {
			LOGGER.info("Blockchain and DB is synchronized");
		}
	}

	@Override
	public Object call() throws Exception {
		publish();
		return this;
	}

	@Override
	public String getTaskDescription() {
		return null;
	}

	@Override
	public String getTaskName() {
		return Thread.currentThread().getName();
	}

	@Override
	public int taskCount() {
		return service.getQueue().size();
	}

	@Override
	public int total() {
		return ((Number) service.getTaskCount()).intValue();
	}

	@Override
	public int progress() {
		return ((Number) ((service.getCompletedTaskCount() / service.getTaskCount()) * 100)).intValue();
	}

	private class Publisher implements Callable {

		private long opCounter;
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
			return opCounter;
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
			while (true) {
				try {
					Object places;
					if (SyncStatus.NEW_SYNC.equals(statusSync) || SyncStatus.TAGS_SYNC.equals(statusSync)) {
						places = osmParser.parseNextCoordinatePlaces(placesPerOperation, false);
					} else {
						places = osmParser.parseNextCoordinatePlaces(placesPerOperation, true);
					}
					if (places == null || ((List)places).isEmpty())
						break;

					publish((List<Object>) places);
				} catch (Exception e) {
					LOGGER.error("Error while publishing", e);
					break;
				}
			}
		}

		private void publish(List<Object> places) throws FailedVerificationException {
			List<OpOperation> osmCoordinatePlacesDto = generateOpOperationFromPlaceList(places);
			try {
				for (OpOperation opOperation : osmCoordinatePlacesDto) {
					if(opOperation.hasCreated() || opOperation.hasEdited()) {
						blocksManager.addOperation(opOperation);
					}
				}
				opCounter += osmCoordinatePlacesDto.size();
			} catch (Exception e) {
				e.printStackTrace();
				LOGGER.error("Error occurred while posting places: " + e.getMessage());
				LOGGER.info("Amount of pack transferred data: " + opCounter);
			}

			createBlock(opCounter, appStartedMs);
		}

		private void createBlock(long opCounter, long appStartedMs) {
			try {
				if (!blocksManager.getBlockchain().getQueueOperations().isEmpty()) {
					boolean createBlock = (opCounter % operationsPerBlock == 0 || blocksManager.getBlockchain().getQueueOperations().size() >= operationsPerBlock);
					if (createBlock) {
						OpBlock opBlock = blocksManager.createBlock();
						LOGGER.info("Created block: " + opBlock.getRawHash() + " with size: " + opBlock.getOperations().size());
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

		private List<OpOperation> generateOpOperationFromPlaceList(List<Object> places) throws FailedVerificationException {
			List<OpOperation> opOperations = new ArrayList<>();

			OpOperation opOperation = initOpOperation(opType, blocksManager);

			for (Object e : places) {
				if (e instanceof Entity) {
					Entity obj = (Entity) e;
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
				} else if (e instanceof DiffEntity) {
					DiffEntity diffEntity = (DiffEntity) e;
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

			generateHashAndSign(opOperations, opOperation, blocksManager);
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
		Map<String, Object> syncStates = botObject.getStringObjMap(ATTR_SYNC_STATES);
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

	public static class Tag {
		String name;
		List<String> tags;
		String coordinate;
		String type;
	}

	private List<String> getRequestList(String timestamp, SyncStatus syncStatus) throws UnsupportedEncodingException {
		List<String> requests = new ArrayList<>();
		List<Tag> coordinateList = new ArrayList<>();
		List<Tag> bboxList = new ArrayList<>();
		Map<String, Object> states = botObject.getStringObjMap(ATTR_SYNC_STATES);

		for (Map<String, Object> objectMap : ((List<Map<String, Object>>)states.get(ATTR_TAGS))) {
			if (SyncStatus.TAGS_SYNC.equals(syncStatus)) {
				List<Map<String, Object>> dbSyncState = (List<Map<String, Object>>) dbManager.getDBSyncState().get(ATTR_TAGS);
				for (Map<String, Object> map : dbSyncState) {
					if (objectMap.get("name").equals(map.get("name"))) {
						if (objectMap.get("bbox").equals("")) {
							for (String coordinate : generateListCoordinates()) {
								List<String> dbTags = (List<String>) map.get("values");
								List<String> botTags = (List<String>) objectMap.get("values");
								List<String> notSyncTags = new ArrayList<>();
								for (String botTag : botTags) {
									if (!dbTags.contains(botTag)) {
										notSyncTags.add(botTag);
									}
								}
								Tag tag = new Tag();
								tag.name = (String) objectMap.get("name");
								tag.tags = notSyncTags;
								tag.coordinate = "(" + coordinate + ")";
								tag.type = (String) objectMap.get("type");
								bboxList.add(tag);
							}
						} else {
							List<String> dbTags = (List<String>) map.get("values");
							List<String> botTags = (List<String>) objectMap.get("values");
							List<String> notSyncTags = new ArrayList<>();
							for (String botTag : botTags) {
								if (!dbTags.contains(botTag)) {
									notSyncTags.add(botTag);
								}
							}
							if (!notSyncTags.isEmpty()) {
								Tag tag = new Tag();
								tag.name = (String) objectMap.get("name");
								tag.tags = notSyncTags;
								tag.coordinate = (String) objectMap.get("bbox");
								tag.type = (String) objectMap.get("type");
								coordinateList.add(tag);
							}
						}
					}
				}
			} else {
				if (objectMap.get("bbox").equals("")) {
					if (SyncStatus.DIFF_SYNC.equals(syncStatus)) {
						Tag tag = new Tag();
						tag.name = (String) objectMap.get("name");
						tag.tags = (List<String>) objectMap.get("values");
						tag.coordinate = "({{bbox}})";
						tag.type = (String) objectMap.get("type");
						bboxList.add(tag);
					} else {
						for (String coordinate : generateListCoordinates()) {
							Tag tag = new Tag();
							tag.name = (String) objectMap.get("name");
							tag.tags = (List<String>) objectMap.get("values");
							tag.coordinate = "(" + coordinate + ")";
							tag.type = (String) objectMap.get("type");
							bboxList.add(tag);
						}
					}
				} else {
					Tag tag = new Tag();
					tag.name = (String) objectMap.get("name");
					tag.tags = (List<String>) objectMap.get("values");
					tag.coordinate = (String) objectMap.get("bbox");
					tag.type = (String) objectMap.get("type");
					coordinateList.add(tag);
				}
			}
		}

		requests.addAll(getRequestList(timestamp, syncStatus, bboxList));
		requests.addAll(getRequestList(timestamp, syncStatus, coordinateList));
		return requests;
	}

	private List<String> getRequestList(String timestamp, SyncStatus syncStatus, List<Tag> listTags) throws UnsupportedEncodingException {
		List<String> requests = new ArrayList<>();
		for (Tag tag : listTags) {
			if (botObject.getStringMap(ATTR_SYNC_STATES).get(F_DATE).equals("")
					|| SyncStatus.TAGS_SYNC.equals(syncStatus)) {
				requests.add(requestService.generateRequestString(tag, F_DATE, timestamp));
			} else {
				requests.add(requestService.generateRequestString(tag, F_DIFF,
						botObject.getStringMap(ATTR_SYNC_STATES).get(F_DATE) + "\",\"" + timestamp));
			}
		}

		return requests;
	}

	private OpOperation generateEditOpDeleteOsmIds(OpObject oldObject, String extId) throws FailedVerificationException {
		OpOperation opOperation = new OpOperation();
		opOperation.setType(opType);
		opOperation.setSignedBy(blocksManager.getServerUser());
		OpObject editObject = new OpObject();
		editObject.putObjectValue(F_ID, oldObject.getId());

		TreeMap<String, Object> addDeletedIds = new TreeMap<>();
		TreeMap<String, Object> appendOp = new TreeMap<>();
		TreeMap<String, Object> idMap = new TreeMap<>();
		idMap.put(F_ID, extId);
		appendOp.put("append", idMap);
		addDeletedIds.put(ATTR_SOURCE + "." + ATTR_OLD_OSM_IDS, appendOp);

		for (Map<String, Object> map : ((List<Map<String, Object>>)oldObject.getStringObjMap(ATTR_SOURCE).get(ATTR_OSM))) {
			if (map.get(F_ID).equals(extId)) {
				addDeletedIds.put(ATTR_SOURCE + "." + ATTR_OSM + "[0]", "delete");
			}
		}
		editObject.putObjectValue(F_CHANGE, addDeletedIds);
		editObject.putObjectValue(F_CURRENT, Collections.emptyList());

		opOperation.addEdited(editObject);
		blocksManager.generateHashAndSign(opOperation, blocksManager.getServerLoginKeyPair());

		return opOperation;
	}

}
