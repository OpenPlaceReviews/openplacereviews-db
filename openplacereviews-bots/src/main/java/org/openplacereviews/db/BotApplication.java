package org.openplacereviews.db;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.db.publisher.controller.PublisherController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Date;

@SpringBootApplication
public class BotApplication implements ApplicationRunner {
	protected static final Log LOGGER = LogFactory.getLog(BotApplication.class);

	@Autowired
	private JdbcTemplate jdbcTemplate;

	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(BotApplication.class);
		ConfigurableApplicationContext context = app.run(args);

		PublisherController publisherController =
				context.getBean("untilSuccessPublisherController", PublisherController.class);

		LOGGER.info("Started at: " + new Date());
		publisherController.executePublishing();
		LOGGER.info("Finished at: " + new Date());
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		boolean isFilePath = args.getOptionNames().contains("file.path");
		if (!isFilePath) {
			LOGGER.error("--file.path argument is required");
			System.exit(-1);
		}

		LOGGER.info("Creating table place");
		jdbcTemplate.execute(
				"CREATE TABLE IF NOT EXISTS place(id VARCHAR(19), timestamp int, deployed BOOLEAN DEFAULT FALSE)");
		jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS pk_index ON place(id)");
	}
}
