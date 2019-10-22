package org.openplacereviews.controllers;

import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpIndexColumn;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.de.CompoundKey;
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
import com.google.gson.JsonPrimitive;
import com.google.openlocationcode.OpenLocationCode;

public class OprPlaceDataProvider implements PublicDataProvider {
	
	private static final int TILE_INFO_SUBSET = 4;

	private Gson geoJson;
	
	@Autowired
	private BlocksManager blocksManager;

	
	public OprPlaceDataProvider() {
		geoJson = new GsonBuilder().registerTypeAdapterFactory(new GeometryAdapterFactory()).create();
	}
	
	public FeatureCollection getIdsByTileId(String tileId) {
		OpBlockChain blc = blocksManager.getBlockchain();
		OpBlockChain.ObjectsSearchRequest r = new OpBlockChain.ObjectsSearchRequest();
		OpIndexColumn ind = blocksManager.getIndex("opr.place", DBSchemaManager.INDEX_P[0]);
		blc.fetchObjectsByIndex("opr.place", ind, r, tileId);

		return generateFeatureCollectionFromResult(r.result);
	}

	
	public FeatureCollection generateFeatureCollectionFromResult(List<OpObject> opObjects) {
		FeatureCollection fc = new FeatureCollection(new ArrayList<>());
		for(OpObject o : opObjects) {
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
	
	public FeatureCollection getAllIds() {
		OpBlockChain blc = blocksManager.getBlockchain();
		OpBlockChain.ObjectsSearchRequest r = new OpBlockChain.ObjectsSearchRequest();
		r.requestOnlyKeys = true;
		blc.fetchAllObjects("opr.place", r);
		List<CompoundKey> ks = r.keys;
		Map<String, Integer> counts = new HashMap<>();

		FeatureCollection fc = new FeatureCollection(new ArrayList<>());
		for(CompoundKey c : ks) {
			String areaCode = c.first.substring(0, TILE_INFO_SUBSET);
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
	
	@Override
	public AbstractResource getContent() {
		return new InMemoryResource(geoJson.toJson(getAllIds()));
	}
	
	@Override
	public AbstractResource getPage() {
		return new InputStreamResource(OprPlaceDataProvider.class.getResourceAsStream("/map.html"));
	}

}
