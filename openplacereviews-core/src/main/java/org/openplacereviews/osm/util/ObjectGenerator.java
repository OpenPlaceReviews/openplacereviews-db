package org.openplacereviews.osm.util;

import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.openplacereviews.osm.parser.OsmLocationTool;
import org.openplacereviews.osm.model.DiffEntity;
import org.openplacereviews.osm.model.Entity;
import org.openplacereviews.osm.model.EntityInfo;

import java.util.*;

import static org.openplacereviews.osm.service.PublishBotManager.*;
import static org.openplacereviews.opendb.ops.OpObject.F_CHANGE;
import static org.openplacereviews.opendb.ops.OpObject.F_CURRENT;
import static org.openplacereviews.opendb.ops.OpObject.F_ID;
import static org.openplacereviews.opendb.ops.OpOperation.F_DELETE;
import static org.openplacereviews.opendb.ops.OpOperation.F_TYPE;
import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;
import static org.openplacereviews.osm.model.EntityInfo.*;

public class ObjectGenerator {

	public static List<List<String>> generateDeleteOpObject(List<List<String>> deletedObjs, Entity entity) {
		deletedObjs.add(Collections.singletonList(OsmLocationTool.generateStrId(entity.getLatLon(), entity.getId())));
		return deletedObjs;
	}

	public static OpObject generateCreateOpObject(Entity entity) {
		OpObject create = new OpObject();
		create.putObjectValue(F_ID, Arrays.asList(OsmLocationTool.generateStrId(entity.getLatLon(), entity.getId())));
		TreeMap<String, Object> osmObject = new TreeMap<>();
		osmObject.put(F_ID, Collections.singletonList(String.valueOf(entity.getId())));
		osmObject.put(F_TYPE, entity.getClass().getSimpleName().toLowerCase());
		osmObject.put(ATTR_TAGS, entity.getTags());
		osmObject.put(ATTR_LONGITUDE, entity.getLongitude());
		osmObject.put(ATTR_LATITUDE, entity.getLatitude());

		if (entity.getEntityInfo() != null) {
			generateEntityInfo(osmObject, entity.getEntityInfo());
		}

		create.putObjectValue(ATTR_OSM, Arrays.asList(osmObject));
		return create;
	}

	public static OpObject generateEditOpObject(OpObject edit, DiffEntity diffEntity) {
		edit.putObjectValue(F_ID, Arrays.asList(OsmLocationTool.generateStrId(diffEntity.getNewNode().getLatLon(), diffEntity.getNewNode().getId())));
		TreeMap<String, Object> changeTagMap = new TreeMap<>();
		TreeMap<String, String> currentTagMAp = new TreeMap<>();
		Map<String, String> tempTags = diffEntity.getOldNode().getTags();
		for (Map.Entry<String, String> newEntry : diffEntity.getNewNode().getTags().entrySet()) {
			if (tempTags.containsKey(newEntry.getKey())) {
				if (!newEntry.getValue().equals(tempTags.get(newEntry.getKey()))) {
					TreeMap<String, String> setValue = new TreeMap<>();
					setValue.put(ATTR_SET, newEntry.getValue());
					changeTagMap.put(ATTR_OSM_TAGS + newEntry.getKey(), setValue);
					currentTagMAp.put(ATTR_OSM_TAGS + newEntry.getKey(), tempTags.get(newEntry.getKey()));
				}
			} else {
				TreeMap<String, String> setValue = new TreeMap<>();
				setValue.put(ATTR_SET, newEntry.getValue());
				changeTagMap.put(ATTR_OSM_TAGS + newEntry.getKey(), setValue);
			}
		}
		tempTags = diffEntity.getNewNode().getTags();
		for (Map.Entry<String, String> oldEntry : diffEntity.getOldNode().getTags().entrySet()) {
			if (!tempTags.containsKey(oldEntry.getKey())) {
				changeTagMap.put(ATTR_OSM_TAGS + oldEntry.getKey(), F_DELETE);
				currentTagMAp.put(ATTR_OSM_TAGS + oldEntry.getKey(), oldEntry.getValue());
			}
		}

		edit.putObjectValue(F_CHANGE, changeTagMap);
		edit.putObjectValue(F_CURRENT, currentTagMAp);

		return edit;
	}

	public static OpOperation initOpOperation(OpOperation opOperation, String osmOpType, BlocksManager blocksManager) {
		if (opOperation == null) {
			opOperation = new OpOperation();
			opOperation.setType(osmOpType);
			opOperation.setSignedBy(blocksManager.getServerUser());
		}
		return opOperation;
	}

	public static void generateHashAndSign(List<OpOperation> opOperations, OpOperation opOperation, BlocksManager blocksManager) throws FailedVerificationException {
		if (opOperation != null) {
			String obj = blocksManager.getBlockchain().getRules().getFormatter().opToJson(opOperation);
			opOperation = blocksManager.getBlockchain().getRules().getFormatter().parseOperation(obj);
			opOperation = blocksManager.generateHashAndSign(opOperation, blocksManager.getServerLoginKeyPair());
			opOperations.add(opOperation);
		}
	}

	public static void generateEntityInfo(TreeMap<String, Object> osmObject, EntityInfo entityInfo) {
		osmObject.put(ATTR_TIMESTAMP, entityInfo.getTimestamp());
		osmObject.put(ATTR_UID, entityInfo.getUid());
		osmObject.put(ATTR_USER, entityInfo.getUser());
		osmObject.put(ATTR_VERSION, entityInfo.getVersion());
		osmObject.put(ATTR_CHANGESET, entityInfo.getChangeset());
		osmObject.put(ATTR_VISIBLE, entityInfo.getVisible());
		osmObject.put(ATTR_ACTION, entityInfo.getAction());
	}
}
