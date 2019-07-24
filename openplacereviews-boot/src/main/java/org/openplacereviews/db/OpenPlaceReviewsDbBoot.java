package org.openplacereviews.db;


import org.openplacereviews.db.service.OprOsmPlacePublisher;
import org.openplacereviews.opendb.OpenDBServer;
import org.openplacereviews.opendb.service.BlocksManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Arrays;
import java.util.List;

@SpringBootApplication
@ComponentScan("org.openplacereviews")
public class OpenPlaceReviewsDbBoot extends OpenDBServer implements ApplicationRunner {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	public BlocksManager blocksManager;

	@Autowired
	public OprOsmPlacePublisher oprOsmPlacePublisher;

	public static void main(String[] args) {
		System.setProperty("spring.devtools.restart.enabled", "false");
		SpringApplication.run(OpenPlaceReviewsDbBoot.class, args);
	}

	public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
		return cmd -> {
			super.commandLineRunner(ctx).run();
		};
	}
	
	public void preStartApplication() {
		List<String> bootstrapList =
				Arrays.asList("opr-0-test-user", BlocksManager.BOOT_STD_OPS_DEFINTIONS, BlocksManager.BOOT_STD_ROLES,
						"opr-0-test-grant", BlocksManager.BOOT_STD_VALIDATION, "opr-osm");
		blocksManager.setBootstrapList(bootstrapList);
	}

	@Override
	public void run(ApplicationArguments args) {
		boolean isImport = args.getOptionNames().contains("import");
		if (isImport && args.getOptionValues("import").get(0).equals("true")) {
			boolean isFilePath = args.getOptionNames().contains("file.path");
			if (!isFilePath) {
				LOGGER.error("--file.path argument is required");
				System.exit(-1);
			}

			LOGGER.info("Creating table place");
			jdbcTemplate.execute(
					"CREATE TABLE IF NOT EXISTS place(id VARCHAR(19) PRIMARY KEY, deployed BOOLEAN DEFAULT FALSE)");
			jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS pk_index ON place(id)");
		}
	}

}
