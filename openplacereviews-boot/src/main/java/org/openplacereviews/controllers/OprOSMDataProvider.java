package org.openplacereviews.controllers;

import static org.openplacereviews.opendb.ops.OpOperation.F_TYPE;
import static org.openplacereviews.osm.model.Entity.ATTR_ID;
import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;
import static org.openplacereviews.osm.model.EntityInfo.ATTR_ACTION;
import static org.openplacereviews.osm.model.EntityInfo.ATTR_CHANGESET;
import static org.openplacereviews.osm.model.EntityInfo.ATTR_TIMESTAMP;
import static org.openplacereviews.osm.model.EntityInfo.ATTR_UID;
import static org.openplacereviews.osm.model.EntityInfo.ATTR_USER;
import static org.openplacereviews.osm.model.EntityInfo.ATTR_VERSION;
import static org.openplacereviews.osm.model.EntityInfo.ATTR_VISIBLE;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_OSM;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_SOURCE;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_TAGS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.PublicDataManager.PublicDataProvider;
import org.openplacereviews.osm.model.Entity;
import org.openplacereviews.osm.model.EntityInfo;
import org.openplacereviews.osm.model.Node;
import org.openplacereviews.osm.model.Relation;
import org.openplacereviews.osm.model.Way;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.AbstractResource;

public class OprOSMDataProvider implements PublicDataProvider {
	@Autowired
	private BlocksManager blocksManager;
	
	@Override
	public AbstractResource getContent(Map<String, String[]> params) {
		throw new UnsupportedOperationException();
	}

	@Override
	public AbstractResource getPage(Map<String, String[]> params) {
		throw new UnsupportedOperationException();
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

	

	@SuppressWarnings("unchecked")
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
		addTags(map, node);
		generateEntityInfo(map, node);
		return node;
	}

	@SuppressWarnings("unchecked")
	private void addTags(Map<String, Object> map, Entity node) {
		for (Map.Entry<String, Object> entry : ((Map<String, Object>) map.get(F_TAGS)).entrySet()) {
			node.putTag(entry.getKey(), entry.getValue().toString());
		}
	}

	private Entity generateWay(Map<String, Object> map) {
		Way way = new Way((Long)map.get(ATTR_ID), null, (Double) map.get(ATTR_LATITUDE), (Double) map.get(ATTR_LONGITUDE));
		addTags(map, way);
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
