package org.openplacereviews.db;


import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.openplacereviews.controllers.OprPlaceDataProvider;
import org.openplacereviews.opendb.OpenDBServer;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.PublicDataManager;
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
	
	@Autowired
	public PublicDataManager publicDataManager;

	
	@Value("${opendb.mgmt.user}")
	public String opendbMgmtUser; 
	

	public static void main(String[] args) {
		System.setProperty("spring.devtools.restart.enabled", "false");
		SpringApplication.run(OpenPlaceReviewsDbBoot.class, args);
	}

	@Override
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

		addOsmIdIndex();
		addPlaceTypeIndex();
		addGeoIndexReport();
	}

	private void addGeoIndexReport() {
		Map<String, Object> mp = new TreeMap<>();
		mp.put(SettingsManager.ENDPOINT_ID, "geo");
		mp.put(PublicDataManager.ENDPOINT_PATH, "geo");
		mp.put(PublicDataManager.ENDPOINT_PROVIDER, OprPlaceDataProvider.class.getName());
		settingsManager.registerMapPreferenceForFamily(SettingsManager.OPENDB_ENDPOINTS_CONFIG, mp);
		publicDataManager.registerDataProvider(OprPlaceDataProvider.class);
	}
	
	private void addPlaceTypeIndex() {
		Map<String, Object> placeTypeInd = new TreeMap<>();
		placeTypeInd.put(SettingsManager.INDEX_NAME, "placetype");
		placeTypeInd.put(SettingsManager.INDEX_TABLENAME, "obj_opr_places");
		placeTypeInd.put(SettingsManager.INDEX_FIELD, Arrays.asList("placetype"));
		placeTypeInd.put("sqlmapping", "single");
		placeTypeInd.put(SettingsManager.INDEX_SQL_TYPE, "text");
		placeTypeInd.put(SettingsManager.INDEX_CACHE_RUNTIME_MAX, 64);
		placeTypeInd.put(SettingsManager.INDEX_CACHE_DB_MAX, 64);
		placeTypeInd.put(SettingsManager.INDEX_INDEX_TYPE, "true");
		settingsManager.registerMapPreferenceForFamily(SettingsManager.DB_SCHEMA_INDEXES, placeTypeInd);
	}

	private void addOsmIdIndex() {
		Map<String, Object> osmIdInd = new TreeMap<>();
		osmIdInd.put(SettingsManager.INDEX_NAME, "osmid");
		osmIdInd.put(SettingsManager.INDEX_TABLENAME, "obj_opr_places");
		osmIdInd.put(SettingsManager.INDEX_FIELD, Arrays.asList("source.osm.id"));
		osmIdInd.put("sqlmapping", "array");
		osmIdInd.put(SettingsManager.INDEX_SQL_TYPE, "bigint[]");
		osmIdInd.put(SettingsManager.INDEX_CACHE_RUNTIME_MAX, 128);
		osmIdInd.put(SettingsManager.INDEX_CACHE_DB_MAX, 256);
		osmIdInd.put(SettingsManager.INDEX_INDEX_TYPE, "GIN");
		settingsManager.registerMapPreferenceForFamily(SettingsManager.DB_SCHEMA_INDEXES, osmIdInd);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
	}

}
