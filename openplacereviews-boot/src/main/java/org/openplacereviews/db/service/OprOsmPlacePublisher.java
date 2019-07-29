package org.openplacereviews.db.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.openplacereviews.osm.OsmLocationTool;
import org.openplacereviews.osm.OsmParser;
import org.openplacereviews.osm.model.DiffEntity;
import org.openplacereviews.osm.model.Entity;
import org.openplacereviews.osm.model.EntityInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.xmlpull.v1.XmlPullParserException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.openplacereviews.opendb.ops.OpBlockchainRules.OP_OPERATION;
import static org.openplacereviews.opendb.ops.OpObject.F_ID;
import static org.openplacereviews.opendb.ops.OpOperation.F_DELETE;
import static org.openplacereviews.opendb.ops.OpOperation.F_TYPE;
import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;
import static org.openplacereviews.osm.model.EntityInfo.*;

@Component
public class OprOsmPlacePublisher implements ApplicationListener<ApplicationReadyEvent> {

	public static final String ATTR_OSM = "osm";
	public static final String ATTR_TAGS = "tags";
	public static final String F_SYNC_STATES = "sync-states";
	public static final String F_DATE = "date";
	public static final String OP_OPR_CRAWLER = "opr.crawler";

	@Autowired
	public BlocksManager blocksManager;

	@Autowired
	public JdbcTemplate jdbcTemplate;

	protected static final Log LOGGER = LogFactory.getLog(OprOsmPlacePublisher.class);

	@Value("${import}")
	private String runImport;

	@Value("${osm.parser.timestamp}")
	protected String timestamp;

	@Value("${osm.parser.overpass.url}")
	protected String overpassURL;

	@Value("${osm.parser.overpass.timestamp}")
	protected String overpassTimestampURL;

	@Value("${osm.parser.operation.type}")
	private String osmOpType;

	@Value("${osm.parser.places.per.operation}")
	private Integer placesPerOperation;

	@Value("${osm.parser.operation.per.block}")
	private Integer operationPerBlock;

	private volatile long opCounter;

	private volatile long appStartedMs;

	@Override
	public void onApplicationEvent(final ApplicationReadyEvent event) {
		if (blocksManager.getBlockchain().getParent().isNullBlock()) {
			try {
				blocksManager.bootstrap(blocksManager.getServerUser(), blocksManager.getServerLoginKeyPair());
				blocksManager.createBlock();
			} catch (FailedVerificationException e) {
				throw new IllegalStateException("Nullable blockchain was not created");
			}
		}
		if (runImport.equals("true")) {
			publish();
		}
	}

	protected void publish() {
		String statusSync = getSyncStatus(timestamp);
		if (statusSync.equals("diff") || statusSync.equals("new")) {
			OsmParser osmParser;
			Reader reader;
			try {
				reader = retrieveFile(timestamp);
				osmParser = new OsmParser(reader);
			} catch (XmlPullParserException | IOException e) {
				LOGGER.error(e.getMessage());
				throw new IllegalArgumentException("Error while creating osm parser");
			}

			publish(osmParser, statusSync);
			try {
				blocksManager.addOperation(generateEditOpForOpOprCrawler(timestamp));
			} catch (FailedVerificationException e) {
				LOGGER.error(e.getMessage());
				throw new IllegalArgumentException("Error while updating op opr.crawler");
			}

			try {
				reader.close();
			} catch (IOException e) {
				LOGGER.error(e.getMessage());
				throw new IllegalArgumentException("Cannot to close stream");
			}
		}
	}

	protected void publish(OsmParser osmParser, String statusSync) {
		appStartedMs = System.currentTimeMillis();
		//List<String> osmPlaceHeaders = blocksManager.getBlockchain().getObjects();
		while (true) {
			try {
				Object places;
				if (statusSync.equals("new")) {
					places = osmParser.parseNextCoordinatePlaces(placesPerOperation);
				} else {
					places = osmParser.parseNextDiffCoordinatePlace(placesPerOperation);
				}

				if (places == null || ((List)places).isEmpty())
					break;

				// todo filter duplicates for create op!!!
				publish(places);
			} catch (Exception e) {
				LOGGER.error("Error while publishing");
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
			}
			opCounter += 1;
		} catch (Exception e) {
			LOGGER.error("Error occurred while posting places: " + e.getMessage());
			LOGGER.info("Amount of pack transferred data: " + opCounter);
		}

		try {
			boolean createBlock = (opCounter % operationPerBlock == 0);
			if (createBlock) {
				blocksManager.createBlock();
				LOGGER.info("Op saved: " + opCounter + " date: " + new Date());
				LOGGER.info("Places saved: " + (opCounter * placesPerOperation) + " date: " + new Date());
				long currTimeMs = System.currentTimeMillis();
				long placesPerSecond = (opCounter * placesPerOperation) / ((currTimeMs - appStartedMs) / 1000);
				LOGGER.info("Places per second: " + placesPerSecond);
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
				OpObject create = new OpObject();
				create.setId(OsmLocationTool.generateStrId(obj.getLatLon(), obj.getId()));
				TreeMap<String, Object> osmObject = new TreeMap<>();
				osmObject.put(F_ID, String.valueOf(obj.getId()));
				osmObject.put(F_TYPE, obj.getClass().getSimpleName().toLowerCase());
				osmObject.put(ATTR_TAGS, obj.getTags());
				osmObject.put(ATTR_LONGITUDE, obj.getLongitude());
				osmObject.put(ATTR_LATITUDE, obj.getLatitude());

				if (obj.getEntityInfo() != null) {
					generateEntityInfo(osmObject, obj.getEntityInfo());
				}

				create.putObjectValue(ATTR_OSM, osmObject);
				createOperation = initOpOperation(createOperation);
				createOperation.addCreated(create);
			} else if (e instanceof DiffEntity) {
				DiffEntity diffEntity = (DiffEntity) e;
				if (diffEntity.getType().equals(DiffEntity.DiffEntityType.DELETE)) {
					deleteOperation = initOpOperation(deleteOperation);
					List<String> objId = Arrays.asList(OsmLocationTool.generateStrId(diffEntity.getOldNode().getLatLon(), diffEntity.getOldNode().getId()));
					deleteOperation.putObjectValue(F_DELETE, Arrays.asList(objId));
				} else if (diffEntity.getType().equals(DiffEntity.DiffEntityType.CREATE)) {
					OpObject create = new OpObject();
					if (diffEntity.getNewNode() == null) {
						System.out.println(diffEntity);
					}
					if (diffEntity.getNewNode().getLatLon() == null) {
						System.out.println(diffEntity);
					}
					create.setId(OsmLocationTool.generateStrId(diffEntity.getNewNode().getLatLon(), diffEntity.getNewNode().getId()));
					TreeMap<String, Object> osmObject = new TreeMap<>();
					osmObject.put(F_ID, String.valueOf(diffEntity.getNewNode().getId()));
					osmObject.put(F_TYPE, diffEntity.getNewNode().getClass().getSimpleName().toLowerCase());
					osmObject.put(ATTR_TAGS, diffEntity.getNewNode().getTags());
					osmObject.put(ATTR_LONGITUDE, diffEntity.getNewNode().getLongitude());
					osmObject.put(ATTR_LATITUDE, diffEntity.getNewNode().getLatitude());

					if (diffEntity.getNewNode().getEntityInfo() != null) {
						generateEntityInfo(osmObject, diffEntity.getNewNode().getEntityInfo());
					}

					create.putObjectValue(ATTR_OSM, osmObject);
					createOperation = initOpOperation(createOperation);
					createOperation.addCreated(create);
				} else if (diffEntity.getType().equals(DiffEntity.DiffEntityType.MODIFY)) {
					OpObject edit = new OpObject();
					if (diffEntity.getNewNode().getLatLon().equals(diffEntity.getOldNode().getLatLon())) {
						edit.setId(OsmLocationTool.generateStrId(diffEntity.getNewNode().getLatLon(), diffEntity.getNewNode().getId()));
						TreeMap<String, Object> changeTagMap = new TreeMap<>();
						TreeMap<String, String> currentTagMAp = new TreeMap<>();
						Map<String, String> tempTags = diffEntity.getOldNode().getTags();
						for (Map.Entry<String, String> newEntry : diffEntity.getNewNode().getTags().entrySet()) {
							if (tempTags.containsKey(newEntry.getKey())) {
								if (!newEntry.getValue().equals(tempTags.get(newEntry.getKey()))) {
									TreeMap<String, String> setValue = new TreeMap<>();
									setValue.put("set", newEntry.getValue());
									changeTagMap.put("osm.tags." + newEntry.getKey(), setValue);
									currentTagMAp.put("osm.tags." + newEntry.getKey(), tempTags.get(newEntry.getKey()));
								}
							} else {
								TreeMap<String, String> setValue = new TreeMap<>();
								setValue.put("set", newEntry.getValue());
								changeTagMap.put("osm.tags." + newEntry.getKey(), setValue);
							}
						}
						tempTags = diffEntity.getNewNode().getTags();
						for (Map.Entry<String, String> oldEntry : diffEntity.getOldNode().getTags().entrySet()) {
							if (!tempTags.containsKey(oldEntry.getKey())) {
								changeTagMap.put("osm.tags." + oldEntry.getKey(), F_DELETE);
								currentTagMAp.put("osm.tags." + oldEntry.getKey(), oldEntry.getValue());
							}
						}

						edit.putObjectValue("change", changeTagMap);
						edit.putObjectValue("current", currentTagMAp);
						editOperation = initOpOperation(editOperation);
						editOperation.addEdited(edit);
					} else {
						List<String> objId = Arrays.asList(OsmLocationTool.generateStrId(diffEntity.getNewNode().getLatLon(), diffEntity.getNewNode().getId()));
						deleteOperation = initOpOperation(deleteOperation);
						deleteOperation.putObjectValue(F_DELETE,  Arrays.asList(objId));
						OpObject create = new OpObject();
						create.setId(OsmLocationTool.generateStrId(diffEntity.getNewNode().getLatLon(), diffEntity.getNewNode().getId()));
						TreeMap<String, Object> osmObject = new TreeMap<>();
						osmObject.put(F_ID, String.valueOf(diffEntity.getNewNode().getId()));
						osmObject.put(F_TYPE, diffEntity.getNewNode().getClass().getSimpleName().toLowerCase());
						osmObject.put(ATTR_TAGS, diffEntity.getNewNode().getTags());
						osmObject.put(ATTR_LONGITUDE, diffEntity.getNewNode().getLongitude());
						osmObject.put(ATTR_LATITUDE, diffEntity.getNewNode().getLatitude());

						if (diffEntity.getNewNode().getEntityInfo() != null) {
							generateEntityInfo(osmObject, diffEntity.getNewNode().getEntityInfo());
						}
						create.putObjectValue(ATTR_OSM, osmObject);

						createOperation = initOpOperation(createOperation);
						createOperation.addCreated(create);
					}
				}
			}
		}

		generateHashAndSign(opOperations, deleteOperation);
		generateHashAndSign(opOperations, createOperation);
		generateHashAndSign(opOperations, editOperation);

		return opOperations;
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

	// TODO add sync status for DB!!!!!
	private String getSyncStatus(String timestamp) {
		Map<String, Object> syncStates = getOpOprCrawler().getListStringObjMap(F_SYNC_STATES).get(0);

		if (syncStates.get(F_DATE).equals(timestamp)) {
			return "sync";
		}

		if (syncStates.get(F_DATE).equals("")) {
			return "new";
		}

		return "diff";
	}

	private Reader retrieveFile(String timestamp) throws IOException {
		String request;
		if (getOpOprCrawler().getListStringObjMap(F_SYNC_STATES).get(0).get(F_DATE).equals("")) {
			request = "[out:xml][timeout:25][date:\"" + timestamp + "\"];";
		} else {
			request = "[out:xml][timeout:25][diff:\"" + getOpOprCrawler().getListStringObjMap(F_SYNC_STATES).get(0).get(F_DATE) + "\",\"" + timestamp + "\"];";
		}

		List<Map<String, Object>> mapList = getOpOprCrawler().getListStringObjMap(F_SYNC_STATES);
		Map<String, String> states = (Map<String, String>) ((Map<String, Object>) mapList.get(0)).get(ATTR_TAGS);
		StringBuilder tags = new StringBuilder();
		for (Map.Entry<String, String> e : states.entrySet()) {
			tags.append("node[").append(e.getKey()).append("]").append(e.getValue()).append(";");
		}

		request += "("+ tags +
					"); " +
				"out body;" +
				">;" +
				"out geom meta;";

		request = URLEncoder.encode(request, StandardCharsets.UTF_8.toString());
		request = overpassURL+ "?data=" + request;

		URL url = new URL(request);
		HttpURLConnection con = (HttpURLConnection) url.openConnection();

		int responseCode = con.getResponseCode();
		System.out.println("Sending 'GET' request to URL : " + url);
		System.out.println("Response Code : " + responseCode);

		return new BufferedReader(
				new InputStreamReader(con.getInputStream()));

	}

	private OpObject getOpOprCrawler() {
		return blocksManager.getBlockchain().getObjectByName(OP_OPERATION, OP_OPR_CRAWLER);
	}

	private OpOperation generateEditOpForOpOprCrawler(String timestamp) throws FailedVerificationException {
		OpOperation opOperation = new OpOperation();
		opOperation.setType(OP_OPERATION);
		opOperation.setSignedBy(blocksManager.getServerUser());
		OpObject editObject = new OpObject();
		editObject.setId(getOpOprCrawler().getId().get(0));
		TreeMap<String, Object> changeDate = new TreeMap<>();
		TreeMap<String, String> setDate = new TreeMap<>();
		setDate.put("set", timestamp);
		changeDate.put(F_SYNC_STATES + "[0]." + F_DATE, setDate);
		editObject.putObjectValue("change", changeDate);
		editObject.putObjectValue("current", Collections.EMPTY_MAP);
		opOperation.addEdited(editObject);
		blocksManager.generateHashAndSign(opOperation, blocksManager.getServerLoginKeyPair());

		return opOperation;
	}

	private String getTimestamp() throws IOException {
		URL url = new URL(overpassTimestampURL);

		//gzip input string
		HttpURLConnection con = (HttpURLConnection) url.openConnection();

		int responseCode = con.getResponseCode();
		System.out.println("Sending 'GET' request to URL : " + url);
		System.out.println("Response Code : " + responseCode);

		return con.getInputStream().toString();
	}

}
