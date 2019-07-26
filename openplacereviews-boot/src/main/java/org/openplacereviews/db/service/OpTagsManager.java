package org.openplacereviews.db.opr;

import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.DBSchemaManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Service
public class OpOsmParser {

	private static final String F_TAG_MAPPING = "tag_mapping";

	@Autowired
	private BlocksManager blocksManager;

	@Autowired
	private DBSchemaManager dbSchemaManager;

	@Value("${osm.parser.operation.osm-parser}")
	private String osmParser;

	public Map<String, List<String>> getOprTagMapping() {
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

	public void generateObjactableMapping() {
		TreeMap<String, Map<String, Object>> objtables = dbSchemaManager.getObjtables();


	}

}
