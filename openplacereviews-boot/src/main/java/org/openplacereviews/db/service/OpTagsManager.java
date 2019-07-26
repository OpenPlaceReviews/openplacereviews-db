package org.openplacereviews.db.service;

import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.DBSchemaManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class OpTagsManager {

	private static final String F_TAG_MAPPING = "tag-mapping";

	@Autowired
	private BlocksManager blocksManager;

	@Autowired
	private DBSchemaManager dbSchemaManager;

	@Value("${osm.parser.operation.osm-parser}")
	private String osmParser;

	@Value("${osm.parser.operation.type}")
	private String osmOpType;

	private Map<String, List<String>> getOprTagMapping() {
		OpBlockChain.ObjectsSearchRequest objectsSearchRequest = new OpBlockChain.ObjectsSearchRequest();
		blocksManager.getBlockchain().getObjects(osmParser, objectsSearchRequest);
		List<OpObject> objectList = objectsSearchRequest.result;

		TreeMap<String, List<String>> tagsMapping = new TreeMap<>();

		for (OpObject object : objectList) {
			Map<String, List<String>> tagMapping = object.getMapStringList(F_TAG_MAPPING);
			tagsMapping.putAll(tagMapping);
		}

		return tagsMapping;
	}

	// TODO merge and setObjtables -> don't save this objtables-> check objtables on start program
	public void mergeObjtablesAndTagMapping() {
		Map<String, List<String>> tagsMapping = getOprTagMapping();
		TreeMap<String, Map<String, Object>> objtables = dbSchemaManager.getObjtables();
		String table = dbSchemaManager.getTableByType(osmOpType);

		LinkedHashMap<String, LinkedHashMap<String, Object>> indexMap = (LinkedHashMap<String, LinkedHashMap<String, Object>>) objtables.get(table).get("indices");
		for (Map.Entry<String, LinkedHashMap<String, Object>> entry : indexMap.entrySet()) {
			LinkedHashMap<String, Object> subEntry = entry.getValue();

			if (tagsMapping.containsKey(subEntry.get("column"))) {
				List<String> newTags = tagsMapping.get(subEntry.get("column"));
				Set<Map.Entry<String, Object>> index = ((LinkedHashMap<String, Object>) subEntry.get("field")).entrySet();
				int size = index.size();
				for (String tag : newTags) {
					((LinkedHashMap<String, Object>) subEntry.get("field")).put(String.valueOf(size++), dbSchemaManager.getLastFieldValue(tag));
				}
			}
		}
		LinkedHashMap<String, LinkedHashMap<String, Object>> columnMap = (LinkedHashMap<String, LinkedHashMap<String, Object>>) objtables.get(table).get("columns");
		for (Map.Entry<String, LinkedHashMap<String, Object>> entry : columnMap.entrySet()) {
			LinkedHashMap<String, Object> subEntry = entry.getValue();

			if (tagsMapping.containsKey(subEntry.get("name"))) {
				List<String> newTags = tagsMapping.get(subEntry.get("name"));
				Set<Map.Entry<String, Object>> index = ((LinkedHashMap<String, Object>) subEntry.get("field")).entrySet();
				int size = index.size();
				for (String tag : newTags) {
					((LinkedHashMap<String, Object>) subEntry.get("field")).put(String.valueOf(size++), tag);
				}
			}
		}

		dbSchemaManager.setObjtables(objtables);
		dbSchemaManager.prepareObjTableMapping();
	}

}
