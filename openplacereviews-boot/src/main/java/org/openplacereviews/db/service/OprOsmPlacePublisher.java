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

@Component
public class OprOsmPlacePublisher implements ApplicationListener<ApplicationReadyEvent> {

	@Autowired
	public BlocksManager blocksManager;

	@Autowired
	private DbPlacesService dbPlacesService;

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

			while (dbPlacesService.countUndeployedPlaces() > 0) {
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
			System.exit(-1);
			return;
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

				if (dbPlacesService.areSomeExistInDb(places)) {
					//Has place already deployed.
					places = places.stream()
							.filter(place -> !dbPlacesService.isDeployed(place))
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

	protected void publish(List<Entity> places) throws FailedVerificationException, InterruptedException {
		if (places.isEmpty())
			return;

		OpOperation osmCoordinatePlacesDto = generateOpOperationFromPlaceList(places);
		try {
			blocksManager.addOperation(osmCoordinatePlacesDto);
			opCounter += 1;
			dbPlacesService.insertPlaces(places, true);
		} catch (Exception e) {
			LOGGER.error("Error occurred while posting places: " + e.getMessage());
			LOGGER.info("Amount of pack transferred data: " + opCounter);
			dbPlacesService.insertPlaces(places, false);
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
			osmObject.put("id", String.valueOf(e.getId()));
			osmObject.put("type", e.getClass().getSimpleName().toLowerCase());
			osmObject.put("tags", e.getTags());
			osmObject.put("lon", e.getLongitude());
			osmObject.put("lat", e.getLatitude());

			if (e.getEntityInfo() != null) {
				generateEntityInfo(osmObject, e.getEntityInfo());
			}

			create.putObjectValue("osm", osmObject);
			opOperation.addCreated(create);
		}
		String obj = blocksManager.getBlockchain().getRules().getFormatter().opToJson(opOperation);
		opOperation = blocksManager.getBlockchain().getRules().getFormatter().parseOperation(obj);

		return blocksManager.generateHashAndSign(opOperation, blocksManager.getServerLoginKeyPair());
	}

	private void generateEntityInfo(TreeMap<String, Object> osmObject, EntityInfo entityInfo) {
		osmObject.put("timestamp", entityInfo.getTimestamp());
		osmObject.put("uid", entityInfo.getUid());
		osmObject.put("user", entityInfo.getUser());
		osmObject.put("version", entityInfo.getVersion());
		osmObject.put("changeset", entityInfo.getChangeset());
		osmObject.put("visible", entityInfo.getVisible());
		osmObject.put("action", entityInfo.getAction());
	}

}
