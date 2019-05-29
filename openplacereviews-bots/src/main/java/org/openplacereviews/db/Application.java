package org.openplacereviews.db;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.db.parser.OsmParser;
import org.openplacereviews.db.parser.OsmParserKxmlImpl;
import org.openplacereviews.db.publisher.OsmPublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.xmlpull.v1.XmlPullParserException;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@SpringBootApplication
public class Application implements CommandLineRunner {
	protected static final Log LOGGER = LogFactory.getLog(Application.class);

	@Autowired
	private JdbcTemplate jdbcTemplate;

	public static void main(String[] args)
			throws IOException, XmlPullParserException {
		SpringApplication app = new SpringApplication(Application.class);
		ConfigurableApplicationContext context = app.run(args);

		OsmPublisher osmPublisher = context.getBean("osmCoordinatePlacePublisher", OsmPublisher.class);
		File file = new File("/Users/mishasavchuk/projects/vareger/open_db/amenities.osm");
//		File file = new File("/Users/mishasavchuk/projects/vareger/open_db/am.osm");
		OsmParser osmParser = new OsmParserKxmlImpl(file);
		System.out.println("Started at: " + new Date());
		osmPublisher.publish(osmParser);
		System.out.println("Finished at: " + new Date());
	}

	@Override
	public void run(String... strings) throws Exception {
		LOGGER.info("Creating table place");

		jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS place(id VARCHAR(19), deployed BOOLEAN DEFAULT FALSE)");
		jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS pk_index ON place(id)");
	}
}
