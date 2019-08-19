package org.openplacereviews.osm.util;

import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.openplacereviews.osm.model.DiffEntity;
import org.openplacereviews.osm.model.Entity;
import org.openplacereviews.osm.model.EntityInfo;
import org.openplacereviews.osm.parser.OsmLocationTool;
import org.openplacereviews.osm.service.OsmSyncBot.SyncRequest;

import java.util.*;

import static org.openplacereviews.opendb.ops.OpBlockchainRules.OP_BOT;
import static org.openplacereviews.opendb.ops.OpObject.*;
import static org.openplacereviews.opendb.ops.OpOperation.F_DELETE;
import static org.openplacereviews.opendb.ops.OpOperation.F_TYPE;
import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;
import static org.openplacereviews.osm.model.EntityInfo.*;
import static org.openplacereviews.osm.service.OsmSyncBot.*;

public class ObjectGenerator {

	public static OpObject generateCreateOpObject(Entity entity) {
		OpObject create = new OpObject();
		create.putObjectValue(F_ID, OsmLocationTool.generatePlaceLocationId(entity.getLatLon()));
		TreeMap<String, Object> source = new TreeMap<>();
		TreeMap<String, Object> osmObject = new TreeMap<>();
		osmObject.put(F_ID, entity.getId());
		osmObject.put(F_TYPE, entity.getClass().getSimpleName().toLowerCase());
		osmObject.put(ATTR_TAGS, entity.getTags());
		osmObject.put(ATTR_LONGITUDE, entity.getLongitude());
		osmObject.put(ATTR_LATITUDE, entity.getLatitude());

		if (entity.getEntityInfo() != null) {
			generateEntityInfo(osmObject, entity.getEntityInfo());
		}

		source.put(ATTR_OSM, Arrays.asList(osmObject));
		source.put(ATTR_OLD_OSM_IDS, new ArrayList<>());
		create.putObjectValue(ATTR_SOURCE, source);
		return create;
	}
	
	public static OpOperation generateEditOpDeleteOsmIds(String opType,
			OpObject oldObject, String extId, BlocksManager blocksManager) throws FailedVerificationException {
		OpOperation opOperation = new OpOperation();
		opOperation.setType(opType);
		opOperation.setSignedBy(blocksManager.getServerUser());
		OpObject editObject = new OpObject();
		editObject.putObjectValue(F_ID, oldObject.getId());

		TreeMap<String, Object> addDeletedIds = new TreeMap<>();
		TreeMap<String, Object> appendOp = new TreeMap<>();
		TreeMap<String, Object> idMap = new TreeMap<>();
		idMap.put(F_ID, extId);
		appendOp.put("append", idMap);
		addDeletedIds.put(ATTR_SOURCE + "." + ATTR_OLD_OSM_IDS, appendOp);

		for (Map<String, Object> map : ((List<Map<String, Object>>)oldObject.getStringObjMap(ATTR_SOURCE).get(ATTR_OSM))) {
			if (map.get(F_ID).equals(extId)) {
				addDeletedIds.put(ATTR_SOURCE + "." + ATTR_OSM + "[0]", "delete");
			}
		}
		editObject.putObjectValue(F_CHANGE, addDeletedIds);
		editObject.putObjectValue(F_CURRENT, Collections.emptyList());

		opOperation.addEdited(editObject);
		blocksManager.generateHashAndSign(opOperation, blocksManager.getServerLoginKeyPair());

		return opOperation;
	}


	public static OpObject generateEditOpObject(OpObject edit, DiffEntity diffEntity, List<String> objId) {
		edit.putObjectValue(F_ID, objId);
		TreeMap<String, Object> changeTagMap = new TreeMap<>();
		TreeMap<String, String> currentTagMAp = new TreeMap<>();
		Map<String, String> tempTags = diffEntity.getOldNode().getTags();
		for (Map.Entry<String, String> newEntry : diffEntity.getNewNode().getTags().entrySet()) {
			if (tempTags.containsKey(newEntry.getKey())) {
				if (!newEntry.getValue().equals(tempTags.get(newEntry.getKey()))) {
					TreeMap<String, String> setValue = new TreeMap<>();
					setValue.put(ATTR_SET, newEntry.getValue());
					changeTagMap.put(ATTR_SOURCE_OSM_TAGS + newEntry.getKey(), setValue);
					currentTagMAp.put(ATTR_SOURCE_OSM_TAGS + newEntry.getKey(), tempTags.get(newEntry.getKey()));
				}
			} else {
				TreeMap<String, String> setValue = new TreeMap<>();
				setValue.put(ATTR_SET, newEntry.getValue());
				changeTagMap.put(ATTR_SOURCE_OSM_TAGS + newEntry.getKey(), setValue);
			}
		}
		tempTags = diffEntity.getNewNode().getTags();
		for (Map.Entry<String, String> oldEntry : diffEntity.getOldNode().getTags().entrySet()) {
			if (!tempTags.containsKey(oldEntry.getKey())) {
				changeTagMap.put(ATTR_SOURCE_OSM_TAGS + oldEntry.getKey(), F_DELETE);
				currentTagMAp.put(ATTR_SOURCE_OSM_TAGS + oldEntry.getKey(), oldEntry.getValue());
			}
		}

		edit.putObjectValue(F_CHANGE, changeTagMap);
		edit.putObjectValue(F_CURRENT, currentTagMAp);

		return edit;
	}
	
	@SuppressWarnings("unchecked")
	public static SyncRequest parseSyncRequest(SyncRequest cp, Object o) {
		SyncRequest sr = new SyncRequest();
		if (o != null) {
			Map<String, Object> value = (Map<String, Object>) o;
			sr.date = (String) value.get("date");
			sr.coordinates = (String) value.get("bbox");
			sr.key = (String) value.get("key");
			sr.type = (List<String>) value.get("type");
			sr.values = (List<String>) value.get("values");
		} else {
			sr.empty = true;
			sr.coordinates = cp.coordinates;
			sr.key = cp.key;
			sr.date = cp.date;
		}
		return sr;
	}

	public static OpOperation generateEditOpForBotObject(SyncRequest r, OpObject botObject, BlocksManager blocksManager) throws FailedVerificationException {
		OpOperation opOperation = new OpOperation();
		opOperation.setType(OP_BOT);
		opOperation.setSignedBy(blocksManager.getServerUser());
		OpObject editObject = new OpObject();
		editObject.setId(botObject.getId().get(0));

		TreeMap<String, Object> change = new TreeMap<>();
		change.put(F_BOT_STATE + "." + F_OSM_TAGS + "." + r.name + ".key", r.key);
		change.put(F_BOT_STATE + "." + F_OSM_TAGS + "." + r.name + ".values", r.values);
		change.put(F_BOT_STATE + "." + F_OSM_TAGS + "." + r.name + ".type", r.type);
		change.put(F_BOT_STATE + "." + F_OSM_TAGS + "." + r.name + ".date", r.date);
		if (r.coordinates != null) {
			change.put(F_BOT_STATE + "." + F_OSM_TAGS + "." + r.name + ".bbox", r.coordinates);
		}
		editObject.putObjectValue(F_CHANGE, change);
		if(!r.state.empty) {
			TreeMap<String, Object> current = new TreeMap<>();
			current.put(F_BOT_STATE + "." + F_OSM_TAGS + "." + r.name + ".key", r.state.key);
			current.put(F_BOT_STATE + "." + F_OSM_TAGS + "." + r.name + ".values", r.state.values);
			current.put(F_BOT_STATE + "." + F_OSM_TAGS + "." + r.name + ".type", r.state.type);
			current.put(F_BOT_STATE + "." + F_OSM_TAGS + "." + r.name + ".date", r.state.date);
			if (r.coordinates != null) {
				current.put(F_BOT_STATE + "." + F_OSM_TAGS + "." + r.name + ".bbox", r.coordinates);
			}
			editObject.putObjectValue(F_CURRENT, current);	
		}
		opOperation.addEdited(editObject);
		blocksManager.generateHashAndSign(opOperation, blocksManager.getServerLoginKeyPair());

		return opOperation;
	}
	
	public static OpOperation generateEditOpForBotObject(String field, String ptimestamp, 
			String timestamp, OpObject botObject, BlocksManager blocksManager) throws FailedVerificationException {
		OpOperation opOperation = new OpOperation();
		opOperation.setType(OP_BOT);
		opOperation.setSignedBy(blocksManager.getServerUser());
		OpObject editObject = new OpObject();
		editObject.setId(botObject.getId().get(0));

		TreeMap<String, Object> changeDate = new TreeMap<>();
		TreeMap<String, String> setDate = new TreeMap<>();
		TreeMap<String, String> previousDate = new TreeMap<>();
		setDate.put(ATTR_SET, timestamp);
		changeDate.put(F_BOT_STATE + "." + F_OSM_TAGS + "." + field + "." + F_DATE, setDate);
		editObject.putObjectValue(F_CHANGE, changeDate);
		previousDate.put(F_BOT_STATE + "." + F_OSM_TAGS + "." + field + "." + F_DATE, ptimestamp);
		editObject.putObjectValue(F_CURRENT, previousDate);

		opOperation.addEdited(editObject);
		blocksManager.generateHashAndSign(opOperation, blocksManager.getServerLoginKeyPair());

		return opOperation;
	}

	public static OpOperation initOpOperation(String osmOpType, BlocksManager blocksManager) {
		OpOperation opOperation = new OpOperation();
		opOperation.setType(osmOpType);
		opOperation.setSignedBy(blocksManager.getServerUser());
		return opOperation;
	}

	public static OpOperation generateHashAndSign(OpOperation opOperation, BlocksManager blocksManager) throws FailedVerificationException {
		JsonFormatter formatter = blocksManager.getBlockchain().getRules().getFormatter();
		opOperation = formatter.parseOperation(formatter.opToJson(opOperation));
		return blocksManager.generateHashAndSign(opOperation, blocksManager.getServerLoginKeyPair());
	}

	public static void generateEntityInfo(TreeMap<String, Object> osmObject, EntityInfo entityInfo) {
		osmObject.put(ATTR_TIMESTAMP, entityInfo.getTimestamp() == null ? new Date() : entityInfo.getTimestamp());
		osmObject.put(ATTR_UID, entityInfo.getUid());
		osmObject.put(ATTR_USER, entityInfo.getUser());
		osmObject.put(ATTR_VERSION, entityInfo.getVersion() == null ? 1 : entityInfo.getVersion());
		osmObject.put(ATTR_CHANGESET, entityInfo.getChangeset());
		osmObject.put(ATTR_VISIBLE, entityInfo.getVisible());
		osmObject.put(ATTR_ACTION, entityInfo.getAction());
	}

		
}
