package org.openplacereviews.db;


import org.openplacereviews.opendb.OpenDBServer;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.DBSchemaManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.util.Arrays;
import java.util.List;

@SpringBootApplication
@ComponentScan("org.openplacereviews")
public class OpenPlaceReviewsDbBoot extends OpenDBServer {

	@Autowired
	public BlocksManager blocksManager;

	public static void main(String[] args) {
		System.setProperty("spring.devtools.restart.enabled", "false");
		SpringApplication.run(OpenPlaceReviewsDbBoot.class, args);
	}

	@Override
	public void preStartApplication() {
		List<String> bootstrapList =
				Arrays.asList("opr-0-test-user", BlocksManager.BOOT_STD_OPS_DEFINTIONS, BlocksManager.BOOT_STD_ROLES,
						"opr-0-test-grant", BlocksManager.BOOT_STD_VALIDATION, "opr-osm");
		blocksManager.setBootstrapList(bootstrapList);
	}

}
