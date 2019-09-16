package org.openplacereviews.db.service;

import com.github.filosganga.geogson.model.Feature;
import com.github.filosganga.geogson.model.FeatureCollection;
import com.github.filosganga.geogson.model.Point;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.openlocationcode.OpenLocationCode;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpIndexColumn;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.DBSchemaManager;
import org.openplacereviews.osm.model.*;
import org.openplacereviews.osm.parser.OsmLocationTool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

import static org.openplacereviews.opendb.ops.OpOperation.F_TYPE;
import static org.openplacereviews.osm.model.Entity.ATTR_ID;
import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;
import static org.openplacereviews.osm.model.EntityInfo.*;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.*;

@Service
public class OprPlaceManager {

	@Autowired
	private BlocksManager blocksManager;

	public FeatureCollection getAllIds() {
		OpBlockChain blc = blocksManager.getBlockchain();
		OpBlockChain.ObjectsSearchRequest r = new OpBlockChain.ObjectsSearchRequest();
		r.requestOnlyKeys = true;
		blc.fetchAllObjects("opr.place", r);
		List<CompoundKey> ks = r.keys;
		Map<String, Integer> counts = new HashMap<>();

		FeatureCollection fc = new FeatureCollection(new ArrayList<>());
		for(CompoundKey c : ks) {
			String areaCode = c.first.substring(0, 4);
			Integer l = counts.get(areaCode);
			if(l == null) {
				l = 1;
			} else {
				l = l + 1;
			}
			counts.put(areaCode, l);
		}
		for(String areaCode: counts.keySet()) {
			OpenLocationCode.CodeArea ca = OsmLocationTool.decode(areaCode);
			Point p = Point.from(ca.getCenterLongitude(), ca.getCenterLatitude());
			Feature f = new Feature(p, ImmutableMap.of("name", new JsonPrimitive(counts.get(areaCode)),
					"code", new JsonPrimitive(areaCode)), Optional.absent());
			fc.features().add(f);
		}

		return fc;
	}

	public FeatureCollection getIdsByTileId(String tileId) {
		OpBlockChain blc = blocksManager.getBlockchain();
		OpBlockChain.ObjectsSearchRequest r = new OpBlockChain.ObjectsSearchRequest();
		OpIndexColumn ind = blocksManager.getIndex("opr.place", DBSchemaManager.INDEX_P[0]);
		blc.fetchObjectsByIndex("opr.place", ind, r, tileId);
		FeatureCollection fc = new FeatureCollection(new ArrayList<>());
		for(OpObject o : r.result) {
			List<Map<String, Object>> osmList = o.getField(null, "source", "osm");
			if(osmList.size() == 0) {
				continue;
			}
			Map<String, Object> osm = osmList.get(0);
			double lat = (double) osm.get(ATTR_LATITUDE);
			double lon = (double) osm.get(ATTR_LATITUDE);
			Point p = Point.from(lat, lon);
			ImmutableMap.Builder<String, JsonElement> bld = ImmutableMap.builder();
			for(String k : osm.keySet()) {
				bld.put(k, new JsonPrimitive(osm.get(k).toString()));
			}
			Feature f = new Feature(p, bld.build(), Optional.absent());
			fc.features().add(f);
		}

		return fc;
	}

	public Collection<Entity> getOsmObjects() {
		OpBlockChain blc = blocksManager.getBlockchain();
		OpBlockChain.ObjectsSearchRequest r = new OpBlockChain.ObjectsSearchRequest();
		blc.fetchAllObjects("opr.place", r);
		LinkedList<Entity> osmList = new LinkedList<>();
		for (OpObject opObject : r.result) {
			osmList.addAll(generateEntityFromObject(opObject));
		}

		return osmList;
	}

	private List<Entity> generateEntityFromObject(OpObject opObject) {
		List<Map<String, Object>> objectEntity = (List<Map<String, Object>>) opObject.getStringObjMap(F_SOURCE).get(F_OSM);
		List<Entity> entities = new ArrayList<>();
		for (Map<String, Object> e : objectEntity) {
			switch ((String)e.get(F_TYPE)) {
				case "way" : {
					entities.add(generateWay(e));
					break;
				}
				case "node" : {
					entities.add(generateNode(e));
					break;
				}
				case "relation" : {
					entities.add(generateRelation(e));
					break;
				}
				default: {
					break;
				}
			}
		}

		return entities;
	}

	private Entity generateRelation(Map<String, Object> map) {
		Relation relation = new Relation((Long)map.get(ATTR_ID));
		return relation;
	}

	private Entity generateNode(Map<String, Object> map) {
		Node node = new Node((Double) map.get(ATTR_LATITUDE), (Double) map.get(ATTR_LONGITUDE), (Long) map.get(ATTR_ID));
		for (Map.Entry<String, Object> entry : ((Map<String, Object>) map.get(F_TAGS)).entrySet()) {
			node.putTag(entry.getKey(), entry.getValue().toString());
		}
		generateEntityInfo(map, node);
		return node;
	}

	private Entity generateWay(Map<String, Object> map) {
		Way way = new Way((Long)map.get(ATTR_ID), null, (Double) map.get(ATTR_LATITUDE), (Double) map.get(ATTR_LONGITUDE));
		for (Map.Entry<String, Object> entry : ((Map<String, Object>) map.get(F_TAGS)).entrySet()) {
			way.putTag(entry.getKey(), entry.getValue().toString());
		}
		generateEntityInfo(map, way);
		return way;
	}

	private void generateEntityInfo(Map<String, Object> map, Entity node) {
		EntityInfo entityInfo = new EntityInfo();
		entityInfo.setVersion((String) map.get(ATTR_VERSION));
		entityInfo.setUser((String) map.get(ATTR_USER));
		entityInfo.setUid((String) map.get(ATTR_UID));
		entityInfo.setTimestamp((String) map.get(ATTR_TIMESTAMP));
		entityInfo.setChangeset((String) map.get(ATTR_CHANGESET));
		entityInfo.setAction((String) map.get(ATTR_ACTION));
		entityInfo.setVisible((String) map.get(ATTR_VISIBLE));
		node.setEntityInfo(entityInfo);
	}
}
