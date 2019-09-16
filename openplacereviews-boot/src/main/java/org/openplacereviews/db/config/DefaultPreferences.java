package org.openplacereviews.db.config;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

public class DefaultPreferences {

	public final static String OBJTABLE_OPR_PLACE = "opendb.db-schema.objtables.obj_opr_places";
	public final static String OPENDB_STORAGE_REPORTS = "opendb.storage.report-storage";

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
}
