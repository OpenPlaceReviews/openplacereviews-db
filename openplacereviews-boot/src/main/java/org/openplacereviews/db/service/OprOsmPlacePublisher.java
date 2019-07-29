package org.openplacereviews.db.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.openplacereviews.osm.OsmLocationTool;
import org.openplacereviews.osm.OsmParser;
import org.openplacereviews.osm.model.Entity;
import org.openplacereviews.osm.model.EntityInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
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
import java.util.stream.Collectors;

import static org.openplacereviews.opendb.ops.OpBlockchainRules.OP_OPERATION;
import static org.openplacereviews.opendb.ops.OpObject.F_ID;
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
	private DbOprOsmSchemaManager dbSchemaManager;

	protected static final Log LOGGER = LogFactory.getLog(OprOsmPlacePublisher.class);

	@Value("${import}")
	private String runImport;

	@Value("${osm.parser.timestamp}")
	protected String timestamp;

	@Value("${osm.parser.overpass.url}")
	protected String overpassURL;

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
			while (dbSchemaManager.countUndeployedPlaces() > 0) {
				publish();
			}
		}
	}

	protected void publish() {
		if (!checkSyncStateActive(timestamp)) {
			OsmParser osmParser = null;
			Reader reader;
			try {
				reader = retrieveFile(timestamp);
				osmParser = new OsmParser(reader);
			} catch (XmlPullParserException | IOException e) {
				LOGGER.error(e.getMessage());
				throw new IllegalArgumentException("Error while creating osm parser");
			}

			publish(osmParser);
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

	protected void publish(OsmParser osmParser) {
		appStartedMs = System.currentTimeMillis();
		while (true) {
			try {
				List<Entity> places = osmParser.parseNextCoordinatePlaces(placesPerOperation);

				if (places == null || places.isEmpty())
					break;

				if (dbSchemaManager.areSomeExistInDb(places)) {
					//Has place already deployed.
					places = places.stream()
							.filter(place -> !dbSchemaManager.isDeployed(place))
							.collect(Collectors.toList());
				}
				publish(places);
			} catch (Exception e) {
				e.printStackTrace();
				break;
			}
		}
		LOGGER.info("Amount of operation transferred data: " + opCounter);
	}

	protected void publish(List<Entity> places) throws FailedVerificationException {
		if (places.isEmpty())
			return;

		OpOperation osmCoordinatePlacesDto = generateOpOperationFromPlaceList(places);
		try {
			blocksManager.addOperation(osmCoordinatePlacesDto);
			opCounter += 1;
			dbSchemaManager.insertPlaces(places, true);
		} catch (Exception e) {
			LOGGER.error("Error occurred while posting places: " + e.getMessage());
			LOGGER.info("Amount of pack transferred data: " + opCounter);
			dbSchemaManager.insertPlaces(places, false);
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

	protected OpOperation generateOpOperationFromPlaceList(List<Entity> places) throws FailedVerificationException {
		OpOperation opOperation = new OpOperation();
		opOperation.setType(osmOpType);
		opOperation.setSignedBy(blocksManager.getServerUser());

		for (Entity e : places) {
			OpObject create = new OpObject();
			create.setId(OsmLocationTool.generateStrId(e.getLatLon()));
			TreeMap<String, Object> osmObject = new TreeMap<>();
			osmObject.put(F_ID, String.valueOf(e.getId()));
			osmObject.put(F_TYPE, e.getClass().getSimpleName().toLowerCase());
			osmObject.put(ATTR_TAGS, e.getTags());
			osmObject.put(ATTR_LONGITUDE, e.getLongitude());
			osmObject.put(ATTR_LATITUDE, e.getLatitude());

			if (e.getEntityInfo() != null) {
				generateEntityInfo(osmObject, e.getEntityInfo());
			}

			create.putObjectValue(ATTR_OSM, osmObject);
			opOperation.addCreated(create);
		}
		String obj = blocksManager.getBlockchain().getRules().getFormatter().opToJson(opOperation);
		opOperation = blocksManager.getBlockchain().getRules().getFormatter().parseOperation(obj);

		return blocksManager.generateHashAndSign(opOperation, blocksManager.getServerLoginKeyPair());
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

	private boolean checkSyncStateActive(String timestamp) {
		Map<String, Object> syncStates = getOpOprCrawler().getListStringObjMap(F_SYNC_STATES).get(0);

		if (syncStates.get(F_DATE).equals(timestamp)) {
			return true;
		}

		return false;
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
		opOperation.putObjectValue("edit", Arrays.asList(editObject));
		blocksManager.generateHashAndSign(opOperation, blocksManager.getServerLoginKeyPair());

		return opOperation;
	}

}
