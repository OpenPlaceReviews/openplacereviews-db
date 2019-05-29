package org.openplacereviews.db.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.db.dto.OsmCoordinatePlacesDto;
import org.openplacereviews.db.model.OsmCoordinatePlace;
import org.openplacereviews.db.parser.OsmParser;
import org.openplacereviews.db.persistence.DbPlacesService;
import org.openplacereviews.db.rest.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;

/**
 * This class responsible for publish {@see OsmCoordinatePlace} to open-place-db using rest api.
 */
@Service("osmCoordinatePlacePublisher")
public class OsmCoordinatePlacePublisher implements OsmPublisher {

	protected static final Log LOGGER = LogFactory.getLog(OsmCoordinatePlacePublisher.class);

	@Autowired
	private DbPlacesService dbPlacesService;

	@Autowired
	private RestClient restClient;

	@Value("${osm.operation.type}")
	private String osmOpType;

	@Value("${places.per.operation}")
	private Integer placesPerOperation;

	@Value("${operation.per.block}")
	private Integer operationPerBlock;

	private volatile long opCounter;

	private volatile long appStartedMs;

	@Override
	public void publish(OsmParser osmParser) {
		appStartedMs = System.currentTimeMillis();
		while (true) {
			try {
				List<OsmCoordinatePlace> places = osmParser.parseNextCoordinatePalaces(placesPerOperation);


				if (places == null || places.isEmpty())
					break;

				publish(places);
			} catch (Exception e) {
				e.printStackTrace();
				break;
			}
		}
		LOGGER.info("Amount of operation transferred data: " + opCounter);
	}

	protected void publish(List<OsmCoordinatePlace> places) throws JsonProcessingException {
		OsmCoordinatePlacesDto osmCoordinatePlacesDto = new OsmCoordinatePlacesDto(osmOpType, places);
		try {
			restClient.postOsmCoordinatePlace(osmCoordinatePlacesDto);
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
				restClient.createBlock();
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


}
