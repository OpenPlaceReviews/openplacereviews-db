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
import org.openplacereviews.osm.parser.OsmLocationTool;
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
import com.google.openlocationcode.OpenLocationCode;
import com.google.openlocationcode.OpenLocationCode.CodeArea;

public class OprPlaceDataProvider implements PublicDataProvider {
	
	protected static final String PARAM_TILE_ID = "tileid";
	protected static final int INDEXED_TILEID = 6;
	
	protected Gson geoJson;
	
	@Autowired
	protected BlocksManager blocksManager;
	
	public OprPlaceDataProvider() {
		geoJson = new GsonBuilder().registerTypeAdapterFactory(new GeometryAdapterFactory()).create();
	}
	
	public void fetchObjectsByTileId(String tileId, FeatureCollection fc) {
		OpBlockChain blc = blocksManager.getBlockchain();
		OpBlockChain.ObjectsSearchRequest r = new OpBlockChain.ObjectsSearchRequest();
		OpIndexColumn ind = blocksManager.getIndex("opr.place", DBSchemaManager.INDEX_P[0]);
		blc.fetchObjectsByIndex("opr.place", ind, r, tileId);
		generateFeatureCollectionFromResult(r.result, fc);
	}
	
	@SuppressWarnings("unchecked")
	public void generateFeatureCollectionFromResult(List<OpObject> opObjects, FeatureCollection fc) {
		for (OpObject o : opObjects) {
			List<Map<String, Object>> osmList = o.getField(null, "source", "osm");
			if (osmList.size() == 0) {
				continue;
			}
			Map<String, Object> osm = osmList.get(0);
			double lat = (double) osm.get(ATTR_LATITUDE);
			double lon = (double) osm.get(ATTR_LONGITUDE);
			Point p = Point.from(lon, lat);
			ImmutableMap.Builder<String, JsonElement> bld = ImmutableMap.builder();
			for (String k : osm.keySet()) {
				if (k.equals("tags")) {
					continue;
				}
				bld.put(k, new JsonPrimitive(osm.get(k).toString()));
			}
			Map<String, Object> tagsValue = (Map<String, Object>) osm.get("tags");
			if (tagsValue != null) {
				JsonObject obj = new JsonObject();
				Iterator<Entry<String, Object>> it = tagsValue.entrySet().iterator();
				while (it.hasNext()) {
					Entry<String, Object> e = it.next();
					obj.add(e.getKey(), new JsonPrimitive(e.getValue().toString()));
				}
				bld.put("tags", obj);
			}
			Feature f = new Feature(p, bld.build(), Optional.absent());
			fc.features().add(f);
		}
	}
	
	private static final int INT_PR = 20;
	private static final double MIN_PR = 100.0 /  INT_PR;
	
	
	@Override
	public AbstractResource getContent(Map<String, String[]> params) {
		String[] tls = params.get(PARAM_TILE_ID);
		if(tls == null || tls.length == 0) {
			return new InMemoryResource(geoJson.toJson(Collections.EMPTY_MAP));
		}
		tls = tls[0].split(",");
		FeatureCollection fc = new FeatureCollection(new ArrayList<>());
		String topLeft = formatTile(tls[0]), bottomRight;
		if(tls.length > 1) {
			bottomRight = formatTile(tls[1]);
		} else {
			bottomRight = topLeft;
		}
		CodeArea tlc = OsmLocationTool.decode(topLeft);
		CodeArea brc = OsmLocationTool.decode(bottomRight);
		int tllat = (int) Math.ceil(tlc.getNorthLatitude() * INT_PR);
		int tllon = (int) Math.floor(tlc.getWestLongitude() * INT_PR);
		int brlat = (int) Math.floor(brc.getSouthLatitude() * INT_PR);
		int brlon = (int) Math.ceil(brc.getEastLongitude() * INT_PR);
		for (int lat = tllat; lat > brlat; lat --) {
			for (int lon = tllon; lon < brlon; lon ++) {
				double clat = (lat - 0.5d) / INT_PR ;
				double clon = (lon + 0.5d) / INT_PR ;
				String tileId = OpenLocationCode.encode(clat, clon, INDEXED_TILEID).substring(0, INDEXED_TILEID);
				fetchObjectsByTileId(tileId, fc);
			}
		}
		
		return new InMemoryResource(geoJson.toJson(fc));
	}
	
	private String formatTile(String string) {
		if(string.length() > INDEXED_TILEID) {
			return string.substring(0, INDEXED_TILEID);
		} else {
			return string;
		}
	}

	@Override
	public AbstractResource getPage(Map<String, String[]> params) {
		return new InputStreamResource(OprPlaceDataProvider.class.getResourceAsStream("/map.html"));
	}

}
