package org.openplacereviews.db;

import org.openplacereviews.opendb.OpenDBServer;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.SettingsManager;
import org.openplacereviews.scheduled.OpenPlaceReviewsScheduledService;
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

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.openplacereviews.db.config.DefaultPreferences.OBJTABLE_OPR_PLACE;
import static org.openplacereviews.db.config.DefaultPreferences.OPENDB_STORAGE_REPORTS;
import static org.openplacereviews.db.config.DefaultPreferences.getDefaultOprPlacePreference;

@SpringBootApplication
@ComponentScan("org.openplacereviews")
public class OpenPlaceReviewsDbBoot extends OpenDBServer implements ApplicationRunner {

	@Autowired
	public JdbcTemplate jdbcTemplate;

	@Autowired
	public BlocksManager blocksManager;

	@Autowired
	public SettingsManager settingsManager;

	@Autowired
	private OpenPlaceReviewsScheduledService openPlaceReviewsScheduledService;
	
	@Value("${opendb.mgmt.user}")
	public String opendbMgmtUser; 
	

	public static void main(String[] args) {
		System.setProperty("spring.devtools.restart.enabled", "false");
		SpringApplication.run(OpenPlaceReviewsDbBoot.class, args);
	}

	public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
		return cmd -> {
			openPlaceReviewsScheduledService.init();
			super.commandLineRunner(ctx).run();
		};
	}
	
	public void preStartApplication() {
		String usr = opendbMgmtUser.substring(opendbMgmtUser.indexOf(':') + 1);
 		List<String> bootstrapList =
				Arrays.asList("opr-0-" + usr + "-user", BlocksManager.BOOT_STD_OPS_DEFINTIONS,
						BlocksManager.BOOT_STD_ROLES, 
						"opr-0-" + usr + "-grant", 
						BlocksManager.BOOT_STD_VALIDATION, "opr-osm", 
						"opr-0-" + usr + "-bot");
		blocksManager.setBootstrapList(bootstrapList);
	}

	@Override
	public void run(ApplicationArguments args) {
	}

	@PostConstruct
	private void initPrefs() {
		OBJTABLE_OPR_PLACE_PREF = settingsManager.registerMapPreference(OBJTABLE_OPR_PLACE, getDefaultOprPlacePreference(), true, "Determines table for storing superblock opr.place objects", false);
		OPENDB_STORAGE_REPORTS_PREF = settingsManager.registerStringPreference(OPENDB_STORAGE_REPORTS, "reports", true, "Determines path for storing report files", true);
	}

	public static SettingsManager.CommonPreference<Map<String, Object>> OBJTABLE_OPR_PLACE_PREF;
	public static SettingsManager.CommonPreference<String> OPENDB_STORAGE_REPORTS_PREF;


}
