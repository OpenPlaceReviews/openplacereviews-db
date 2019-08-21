package org.openplacereviews.db;

import org.openplacereviews.opendb.OpenDBServer;
import org.openplacereviews.opendb.service.BlocksManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
	public JdbcTemplate jdbcTemplate;

	@Autowired
	public BlocksManager blocksManager;
	
	@Value("${opendb.mgmt.user}")
	public String opendbMgmtUser; 
	

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
		String usr = opendbMgmtUser.substring(opendbMgmtUser.indexOf(':') + 1);
 		List<String> bootstrapList =
				Arrays.asList("opr-0-"+usr+"-user", BlocksManager.BOOT_STD_OPS_DEFINTIONS, BlocksManager.BOOT_STD_ROLES,
						"opr-0-"+usr+"-grant", BlocksManager.BOOT_STD_VALIDATION, "opr-osm", "opr-bot");
		blocksManager.setBootstrapList(bootstrapList);
	}

	@Override
	public void run(ApplicationArguments args) {
	}

}
