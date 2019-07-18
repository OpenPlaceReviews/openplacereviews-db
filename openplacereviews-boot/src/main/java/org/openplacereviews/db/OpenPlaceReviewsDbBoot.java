package org.openplacereviews.db;


import java.util.Arrays;
import java.util.List;

import org.openplacereviews.opendb.OpenDBServer;
import org.openplacereviews.opendb.service.BlocksManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("org.openplacereviews")
public class OpenPlaceReviewsDbBoot extends OpenDBServer {

	@Autowired
	public BlocksManager blocksManager;

	public static void main(String[] args) {
		System.setProperty("spring.devtools.restart.enabled", "false");
		SpringApplication.run(OpenPlaceReviewsDbBoot.class, args);
	}

	public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
		CommandLineRunner cmd = super.commandLineRunner(ctx);
		return cmd;
	}
	
	public void preStartApplication() {
		List<String> bootstrapList =
				Arrays.asList("opr-0-test-user", BlocksManager.BOOT_STD_OPS_DEFINTIONS, BlocksManager.BOOT_STD_ROLES,
						"opr-0-test-grant", BlocksManager.BOOT_STD_VALIDATION, "opr-osm");
		blocksManager.setBootstrapList(bootstrapList);
	}

}
