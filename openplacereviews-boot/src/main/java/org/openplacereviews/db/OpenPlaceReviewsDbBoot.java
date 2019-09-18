package org.openplacereviews.db;


import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.PostConstruct;

import org.openplacereviews.opendb.OpenDBServer;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.SettingsManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootApplication
@ComponentScan("org.openplacereviews")
public class OpenPlaceReviewsDbBoot extends OpenDBServer implements ApplicationRunner {

	public final static String OBJTABLE_OPR_PLACE = "opendb.db-schema.objtables.obj_opr_places";
	public final static String OPENDB_STORAGE_REPORTS = "opendb.storage.report-storage";
	
	@Autowired
	public JdbcTemplate jdbcTemplate;

	@Autowired
	public BlocksManager blocksManager;

	@Autowired
	public SettingsManager settingsManager;

	
	@Value("${opendb.mgmt.user}")
	public String opendbMgmtUser; 
	

	public static void main(String[] args) {
		System.setProperty("spring.devtools.restart.enabled", "false");
		SpringApplication.run(OpenPlaceReviewsDbBoot.class, args);
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
	
	public static Map<String, Object> getDefaultOprPlacePreference() {
		Map<String, Object> obj_logins = new TreeMap<>();
		obj_logins.put("types", Arrays.asList("opr.place"));
		obj_logins.put("keysize", 2);
		Map<String, Object> columnMap = new TreeMap<>();
		columnMap.put("name", "osmid");
		columnMap.put("field", Arrays.asList("source.osm.id"));
		columnMap.put("sqlmapping", "array");
		columnMap.put("sqltype", "bigint[]");
		columnMap.put("cache-runtime-max", 128);
		columnMap.put("cache-db-max", 256);
		columnMap.put("index", "GIN");
		Map<String, Object> columnMap1 = new TreeMap<>();
		columnMap1.put("name", "placetype");
		columnMap1.put("field", Arrays.asList("placetype"));
		columnMap1.put("sqlmapping", "single");
		columnMap1.put("sqltype", "text");
		columnMap.put("cache-runtime-max", 64);
		columnMap.put("cache-db-max", 64);
		columnMap1.put("index", "true");
		obj_logins.put("columns", Arrays.asList(columnMap, columnMap1));
		return obj_logins;
	}

	public static SettingsManager.CommonPreference<Map<String, Object>> OBJTABLE_OPR_PLACE_PREF;
	public static SettingsManager.CommonPreference<String> OPENDB_STORAGE_REPORTS_PREF;


}
