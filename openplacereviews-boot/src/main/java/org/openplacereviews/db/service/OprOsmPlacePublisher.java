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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.openplacereviews.opendb.ops.OpObject.F_ID;
import static org.openplacereviews.opendb.ops.OpOperation.F_TYPE;
import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;
import static org.openplacereviews.osm.model.EntityInfo.*;

@Component
public class OprOsmPlacePublisher implements ApplicationListener<ApplicationReadyEvent> {

	public static final String ATTR_OSM = "osm";
	public static final String ATTR_TAGS = "tags";

	@Autowired
	public BlocksManager blocksManager;

	@Autowired
	private DbOprOsmSchemaManager dbSchemaManager;

	protected static final Log LOGGER = LogFactory.getLog(OprOsmPlacePublisher.class);

	@Value("${import}")
	private String runImport;

	@Value("${file.path}")
	protected String sourceXmlFilePath;

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
		OsmParser osmParser = null;
		try {
			osmParser = new OsmParser(new File(sourceXmlFilePath));
		} catch (FileNotFoundException | XmlPullParserException e) {
			LOGGER.error(e.getMessage());
			throw new IllegalArgumentException("Error while creating osm parser");
		}

		publish(osmParser);
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

}
