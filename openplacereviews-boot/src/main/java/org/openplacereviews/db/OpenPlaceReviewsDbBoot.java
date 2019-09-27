package org.openplacereviews.db;


import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
		settingsManager.registerTableMapping("obj_opr_places", 2, "opr.place");
		// FIXME add indexes
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
	}

	@Override
	public void run(ApplicationArguments args) {
	}


}
