package org.openplacereviews.controllers;


import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpIndexColumn;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.DBSchemaManager;
import org.openplacereviews.opendb.service.PublicDataManager.PublicDataProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.security.util.InMemoryResource;

import com.github.filosganga.geogson.gson.GeometryAdapterFactory;
import com.github.filosganga.geogson.model.Feature;
import com.github.filosganga.geogson.model.FeatureCollection;
import com.github.filosganga.geogson.model.Point;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class OprPlaceDataProvider implements PublicDataProvider {
	
	protected static final String PARAM_TILE_ID = "tileid";
	protected static final int INDEXED_TILEID = 6;
	
	protected Gson geoJson;
	
	@Autowired
	protected BlocksManager blocksManager;
	
	public OprPlaceDataProvider() {
		geoJson = new GsonBuilder().registerTypeAdapterFactory(new GeometryAdapterFactory()).create();
	}
	
	public FeatureCollection getIdsByTileId(String tileId) {
		if(tileId.length() > INDEXED_TILEID) {
			tileId = tileId.trim().substring(0, INDEXED_TILEID);
		}
		OpBlockChain blc = blocksManager.getBlockchain();
		OpBlockChain.ObjectsSearchRequest r = new OpBlockChain.ObjectsSearchRequest();
		OpIndexColumn ind = blocksManager.getIndex("opr.place", DBSchemaManager.INDEX_P[0]);
		blc.fetchObjectsByIndex("opr.place", ind, r, tileId);

		return generateFeatureCollectionFromResult(r.result);
	}
	
	@SuppressWarnings("unchecked")
	public FeatureCollection generateFeatureCollectionFromResult(List<OpObject> opObjects) {
		FeatureCollection fc = new FeatureCollection(new ArrayList<>());
		for(OpObject o : opObjects) {
			List<Map<String, Object>> osmList = o.getField(null, "source", "osm");
			if(osmList.size() == 0) {
				continue;
			}
			Map<String, Object> osm = osmList.get(0);
			double lat = (double) osm.get(ATTR_LATITUDE);
			double lon = (double) osm.get(ATTR_LONGITUDE);
			Point p = Point.from(lon, lat);
			ImmutableMap.Builder<String, JsonElement> bld = ImmutableMap.builder();
			for(String k : osm.keySet()) {
				if(k.equals("tags")) {
					continue;
				}
				bld.put(k, new JsonPrimitive(osm.get(k).toString()));
			}
			Map<String, Object> tagsValue = (Map<String, Object>) osm.get("tags");
			if(tagsValue != null) {
				JsonObject obj = new JsonObject();
				Iterator<Entry<String, Object>> it = tagsValue.entrySet().iterator();
				while(it.hasNext()) {
					Entry<String, Object> e = it.next();
					obj.add(e.getKey(), new JsonPrimitive(e.getValue().toString()));
				}
				bld.put("tags", obj);
			}
			Feature f = new Feature(p, bld.build(), Optional.absent());
			fc.features().add(f);
		}
		return fc;
	}
	
	
	@Override
	public AbstractResource getContent(Map<String, String[]> params) {
		String[] tls = params.get(PARAM_TILE_ID);
		if(tls == null || tls.length != 1) {
			new InMemoryResource(geoJson.toJson(Collections.EMPTY_MAP));
		}
		return new InMemoryResource(geoJson.toJson(getIdsByTileId(tls[0])));
	}
	
	@Override
	public AbstractResource getPage(Map<String, String[]> params) {
		return new InputStreamResource(OprPlaceDataProvider.class.getResourceAsStream("/map.html"));
	}

}
