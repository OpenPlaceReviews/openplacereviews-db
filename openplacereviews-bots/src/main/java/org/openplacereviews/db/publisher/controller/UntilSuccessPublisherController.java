package org.openplacereviews.db.publisher.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.db.parser.OsmParser;
import org.openplacereviews.db.parser.OsmParserKxmlImpl;
import org.openplacereviews.db.persistence.DbPlacesService;
import org.openplacereviews.db.publisher.OsmPublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.xmlpull.v1.XmlPullParserException;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Execute {@link OsmPublisher} in loop while all places will be deployed successfully.
 */
@Component
public class UntilSuccessPublisherController implements PublisherController {

	protected static final Log LOGGER = LogFactory.getLog(UntilSuccessPublisherController.class);

	@Value("${file.path}")
	protected String sourceXmlFilePath;

	@Autowired
	@Qualifier("osmCoordinatePlacePublisher")
	protected OsmPublisher osmPublisher;

	@Autowired
	protected DbPlacesService dbPlacesService;

	@Override
	public void executePublishing() {
		publish();

		while (dbPlacesService.countUndeployedPlaces() > 0) {
			publish();
		}
	}

	/**
	 * Create {@link OsmParserKxmlImpl} using sourceXmlFilePath, and start publish places.
	 */
	private void publish() {
		OsmParser osmParser = null;
		try {
			osmParser = new OsmParserKxmlImpl(new File(sourceXmlFilePath));
		} catch (FileNotFoundException | XmlPullParserException e) {
			LOGGER.error(e.getMessage());
			System.exit(-1);
			return;
		}

		osmPublisher.publish(osmParser);
	}
}
