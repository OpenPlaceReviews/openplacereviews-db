package org.openplacereviews.db;


import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.openplacereviews.api.OprHistoryChangesProvider;
import org.openplacereviews.api.OprPlaceDataProvider;
import org.openplacereviews.api.OprSummaryPlaceDataProvider;
import org.openplacereviews.opendb.OpenDBServer;
import org.openplacereviews.opendb.api.OpApiController;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.BotManager;
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
	
	
	@Autowired
	public UserSchemaManager userSchemaManager;
	
	@Autowired
	public OpApiController apiController;

	@Autowired
	public BotManager botManager;
	
	@Value("${opendb.mgmt.user}")
	public String opendbMgmtUser;
	
	@Value("${opendb.patch-version}")
	public String patchVersion;


	 
	public static void main(String[] args)  {
		// here is a test that application.yml is correctly read from file
//		try {
//			InputStream rr = OpenPlaceReviewsDbBoot.class.getResourceAsStream("/application.yml");
//			BufferedReader br = new BufferedReader(new InputStreamReader(rr));
//			String ln;
//			while ((ln = br.readLine()) != null) {
//				System.out.println(ln);
//			}
//			br.close();
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
		
		System.setProperty("spring.devtools.restart.enabled", "false");

		SpringApplication.run(OpenPlaceReviewsDbBoot.class, args);
	}

	@Override
	public void preStartApplication() {
		MetadataDb metadataDB = loadMetadata(userSchemaManager.getJdbcTemplate());
		userSchemaManager.initializeDatabaseSchema(metadataDB);
		apiController.setUserNameProcessor(userSchemaManager);
		
		String usr = opendbMgmtUser.substring(opendbMgmtUser.indexOf(':') + 1);
 		List<String> bootstrapList =
				Arrays.asList("opr-0-" + usr + "-user", BlocksManager.BOOT_STD_OPS_DEFINTIONS,
						BlocksManager.BOOT_STD_ROLES, 
						"opr-0-" + usr + "-grant", 
						BlocksManager.BOOT_STD_VALIDATION, "opr-osm", 
						"opr-0-" + usr + "-bot");
		blocksManager.setBootstrapList(bootstrapList);
		if ("replication:1".equals(patchVersion)) {
			// this should be changed, so we should not apply patch version for main server branch
			blocksManager.addPatchOperation(12763, "block_12763");
			blocksManager.addPatchOperation(12767, "block_12767");
			blocksManager.addPatchOperation(12774, "block_12774");
			blocksManager.addPatchOperation(12780, "block_12780");
			blocksManager.addPatchOperation(12802, "block_12802");
			blocksManager.addPatchOperation(12804, "block_12804");
			blocksManager.addPatchOperation(12890, "block_12890");
			blocksManager.addPatchOperation(12945, "block_12945");
			blocksManager.addPatchOperation(13020, "block_13020");
			blocksManager.addPatchOperation(13023, "block_13023");
			blocksManager.addPatchOperation(13170, "block_13170");
			blocksManager.addPatchOperation(13550, "block_13550");
			blocksManager.addPatchOperation(13998, "block_13998");
			blocksManager.addPatchOperation(14209, "block_14209");
			blocksManager.addPatchOperation(14335, "block_14335");
			blocksManager.addPatchOperation(14529, "block_14529");
			blocksManager.addPatchOperation(14586, "block_14586");
			blocksManager.addPatchOperation(14629, "block_14629");
			blocksManager.addPatchOperation(14646, "block_14646");
			blocksManager.addPatchOperation(14647, "block_14647");
		}
		settingsManager.registerTableMapping("obj_opr_places", 2, "opr.place");

		addOsmIdIndex();
		addPlaceTypeIndex();
//		botManager.regSystemBot(new TripAdvisorBot("tripadvisor-sync"));
		publicDataManager.registerDataProvider(OprSummaryPlaceDataProvider.class);
		publicDataManager.registerDataProvider(OprPlaceDataProvider.class);
		publicDataManager.registerDataProvider(OprHistoryChangesProvider.class);
		addGeoIndexReport();
		addGeoSummaryIndexReport();
		addDateOSMDataReport();
	}

	private void addGeoSummaryIndexReport() {
		Map<String, Object> mp = new TreeMap<>();
		mp.put(SettingsManager.ENDPOINT_ID, "geoall");
		mp.put(PublicDataManager.ENDPOINT_PATH, "geoall");
		mp.put(PublicDataManager.ENDPOINT_PROVIDER, OprSummaryPlaceDataProvider.class.getName());
		settingsManager.registerMapPreferenceForFamily(SettingsManager.OPENDB_ENDPOINTS_CONFIG, mp);
	}
	
	private void addGeoIndexReport() {
		Map<String, Object> mp = new TreeMap<>();
		mp.put(SettingsManager.ENDPOINT_ID, "geo");
		mp.put(PublicDataManager.ENDPOINT_PATH, "geo");
		mp.put(PublicDataManager.ENDPOINT_PROVIDER, OprPlaceDataProvider.class.getName());
		settingsManager.registerMapPreferenceForFamily(SettingsManager.OPENDB_ENDPOINTS_CONFIG, mp);
	}

	private void addDateOSMDataReport() {
		Map<String, Object> mp = new TreeMap<>();
		mp.put(SettingsManager.ENDPOINT_ID, "history");
		mp.put(PublicDataManager.ENDPOINT_PATH, "history");
		mp.put(PublicDataManager.ENDPOINT_PROVIDER, OprHistoryChangesProvider.class.getName());
		settingsManager.registerMapPreferenceForFamily(SettingsManager.OPENDB_ENDPOINTS_CONFIG, mp);
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
