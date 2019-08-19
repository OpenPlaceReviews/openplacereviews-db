package org.openplacereviews.osm.util;

import static org.openplacereviews.opendb.ops.OpObject.F_CHANGE;
import static org.openplacereviews.opendb.ops.OpObject.F_CURRENT;
import static org.openplacereviews.opendb.ops.OpObject.F_ID;
import static org.openplacereviews.opendb.ops.OpOperation.F_TYPE;
import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;
import static org.openplacereviews.osm.model.EntityInfo.ATTR_CHANGESET;
import static org.openplacereviews.osm.model.EntityInfo.ATTR_TIMESTAMP;
import static org.openplacereviews.osm.model.EntityInfo.ATTR_VERSION;
import static org.openplacereviews.osm.service.OsmSyncBot.F_BOT_STATE;
import static org.openplacereviews.osm.service.OsmSyncBot.F_DATE;
import static org.openplacereviews.osm.service.OsmSyncBot.F_OSM_TAGS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.openplacereviews.osm.model.Entity;
import org.openplacereviews.osm.model.Entity.EntityType;
import org.openplacereviews.osm.model.EntityInfo;
import org.openplacereviews.osm.model.LatLon;
import org.openplacereviews.osm.parser.OsmLocationTool;
import org.openplacereviews.osm.service.OsmSyncBot.PlaceObject;
import org.openplacereviews.osm.service.OsmSyncBot.SyncRequest;

public class PlaceOpObjectHelper {
	public static final String F_SOURCE = "source";
	public static final String F_OLD_OSM_IDS = "old-osm-ids";
	public static final String F_OSM = "osm";
	public static final String F_TAGS = "tags";
	public static final String F_VERSION = "version";
	public static final String F_CHANGESET = "changeset";
	public static final String F_TIMESTAMP = "timestamp";


	public static OpObject generateNewOprObject(Entity entity, TreeMap<String, Object> osmObject ) {
		OpObject create = new OpObject();
		create.putObjectValue(F_ID, OsmLocationTool.generatePlaceLocationId(entity.getLatLon()));
		TreeMap<String, Object> source = new TreeMap<>();
		source.put(F_OSM, Arrays.asList(osmObject));
		source.put(F_OLD_OSM_IDS, new ArrayList<>());
		create.putObjectValue(F_SOURCE, source);
		return create;
	}

	public static TreeMap<String, Object> createOsmObject(Entity entity) {
		TreeMap<String, Object> osmObject = new TreeMap<>();
		osmObject.put(F_ID, entity.getId());
		String te = EntityType.valueOf(entity).name().toLowerCase();
		osmObject.put(F_TYPE, te);
		osmObject.put(F_TAGS, entity.getTags());
		LatLon l = entity.getLatLon();
		osmObject.put(ATTR_LATITUDE, l.getLatitude());
		osmObject.put(ATTR_LONGITUDE, l.getLongitude());
		if (entity.getEntityInfo() != null) {
			generateEntityInfo(osmObject, entity.getEntityInfo());
		}
		return osmObject;
	}
	
	public static OpOperation generateEditDeleteOsmIdsForPlace(OpOperation opOperation, PlaceObject po) throws FailedVerificationException {
		OpObject editObject = new OpObject();
		editObject.putObjectValue(F_ID, po.obj.getId());

		TreeMap<String, Object> change = new TreeMap<>();
		TreeMap<String, Object> current = new TreeMap<>();
		
		TreeMap<String, Object> appendOp = new TreeMap<>();
		TreeMap<String, Object> idMap = new TreeMap<>();
		idMap.put(F_ID, po.osmId);
		idMap.put(F_TYPE, po.type);
		appendOp.put(OpBlockChain.OP_CHANGE_APPEND, idMap);
		change.put(F_SOURCE + "." + F_OLD_OSM_IDS, appendOp);

		
		String f = F_SOURCE + "." + F_OSM + "[" + po.ind + "]";
		change.put(f, OpBlockChain.OP_CHANGE_DELETE);
		current.put(f, po.osm);
		
		editObject.putObjectValue(F_CHANGE, change);
		editObject.putObjectValue(F_CURRENT, current);
		opOperation.addEdited(editObject);
		return opOperation;
	}

	private static void generateDiff(OpObject editObject, String field, Map<String, Object> change,
			Map<String, Object> current, Map<String, Object> oldM, Map<String, Object> newM) {
		TreeSet<String> removedTags = new TreeSet<>(oldM.keySet());
		removedTags.removeAll(newM.keySet());
		for(String removedTag : removedTags) {
			change.put(field + removedTag, OpBlockChain.OP_CHANGE_APPEND);
			current.put(field + removedTag, oldM.get(removedTag));
		}
		for(String tag : newM.keySet()) {
			Object po = oldM.get(tag);
			Object no = newM.get(tag);
			if(!OUtils.equals(po, no)) {
				change.put(field + tag, set(no));
				if(po != null) {
					current.put(field + tag, po);
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	public static OpOperation generateEditValuesForPlace(OpOperation op, PlaceObject po, Map<String, Object> osmObj) {
		OpObject editObject = new OpObject();
		editObject.putObjectValue(F_ID, po.obj.getId());
		Map<String, Object> changeTagMap = new TreeMap<>();
		Map<String, Object> currentTagMap = new TreeMap<>();
		String field = F_SOURCE + "." + F_OSM + "[" + po.ind + "].";
		Map<String, Object> oldM = new TreeMap<String, Object>(po.osm);
		Map<String, Object> newM = new TreeMap<String, Object>(osmObj);
		
		generateDiff(editObject, field + F_TAGS + ".", changeTagMap, currentTagMap,
				(Map<String, Object>) oldM.remove(F_TAGS), (Map<String, Object>) newM.remove(F_TAGS));
		generateDiff(editObject, field, changeTagMap, currentTagMap, oldM, newM);
		if (!changeTagMap.isEmpty()) {
			editObject.putObjectValue(F_CHANGE, changeTagMap);
			editObject.putObjectValue(F_CURRENT, currentTagMap);
			op.addEdited(editObject);
		}
		return op;
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

	public static OpOperation generateEditOpForBotObject(OpOperation opOperation, SyncRequest r, OpObject botObject) throws FailedVerificationException {
		
		OpObject editObject = new OpObject();
		editObject.setId(botObject.getId().get(0));

		TreeMap<String, Object> change = new TreeMap<>();
		change.put(F_BOT_STATE + "." + F_OSM_TAGS + "." + r.name + ".key", set(r.key));
		change.put(F_BOT_STATE + "." + F_OSM_TAGS + "." + r.name + ".values", set(r.values));
		change.put(F_BOT_STATE + "." + F_OSM_TAGS + "." + r.name + ".type", set(r.type));
		change.put(F_BOT_STATE + "." + F_OSM_TAGS + "." + r.name + ".date", set(r.date));
		if (r.coordinates != null) {
			change.put(F_BOT_STATE + "." + F_OSM_TAGS + "." + r.name + ".bbox", set(r.coordinates));
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

		return opOperation;
	}
	
	public static OpOperation generateEditOpForBotObject(OpOperation opOperation, String field, String ptimestamp, 
			String timestamp, OpObject botObject) throws FailedVerificationException {
		OpObject editObject = new OpObject();
		editObject.setId(botObject.getId().get(0));

		TreeMap<String, Object> changeDate = new TreeMap<>();
		TreeMap<String, String> previousDate = new TreeMap<>();
		
		changeDate.put(F_BOT_STATE + "." + F_OSM_TAGS + "." + field + "." + F_DATE, set(timestamp));
		editObject.putObjectValue(F_CHANGE, changeDate);
		previousDate.put(F_BOT_STATE + "." + F_OSM_TAGS + "." + field + "." + F_DATE, ptimestamp);
		editObject.putObjectValue(F_CURRENT, previousDate);

		opOperation.addEdited(editObject);

		return opOperation;
	}



	private static Object set(Object vl) {
		Map<String, Object> set = new TreeMap<String, Object>();
		set.put(OpBlockChain.OP_CHANGE_SET, vl);
		return set;
	}

	public static void generateEntityInfo(TreeMap<String, Object> osmObject, EntityInfo entityInfo) {
		osmObject.put(ATTR_TIMESTAMP, entityInfo.getTimestamp() == null ? new Date() : entityInfo.getTimestamp());
		osmObject.put(ATTR_VERSION, entityInfo.getVersion() == null ? 1 : entityInfo.getVersion());
		osmObject.put(ATTR_CHANGESET, entityInfo.getChangeset());
//		osmObject.put(ATTR_UID, entityInfo.getUid());
//		osmObject.put(ATTR_USER, entityInfo.getUser());
//		osmObject.put(ATTR_VISIBLE, entityInfo.getVisible());
//		osmObject.put(ATTR_ACTION, entityInfo.getAction());
	}

		
}
