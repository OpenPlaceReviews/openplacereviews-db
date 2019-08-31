package org.openplacereviews.controllers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpSession;

import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockChain.ObjectsSearchRequest;
import org.openplacereviews.opendb.ops.OpIndexColumn;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.DBSchemaManager;
import org.openplacereviews.osm.parser.OsmLocationTool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.github.filosganga.geogson.gson.GeometryAdapterFactory;
import com.github.filosganga.geogson.model.Feature;
import com.github.filosganga.geogson.model.FeatureCollection;
import com.github.filosganga.geogson.model.Point;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.openlocationcode.OpenLocationCode.CodeArea;

@Controller
@RequestMapping("/api/places")
public class PlaceController {

	@Autowired
	public BlocksManager blocksManager;
	
	private Gson geoJson;
	
	public PlaceController() {
		geoJson = new GsonBuilder().registerTypeAdapterFactory(new GeometryAdapterFactory()).create();
	}

	
	@GetMapping(path = "/geojson-ids")
	public ResponseEntity<String> allIds(HttpSession session) {
		OpBlockChain blc = blocksManager.getBlockchain();
		ObjectsSearchRequest r = new ObjectsSearchRequest();
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
			CodeArea ca = OsmLocationTool.decode(areaCode);
			Point p = Point.from(ca.getCenterLongitude(), ca.getCenterLatitude());
			Feature f = new Feature(p, ImmutableMap.of("name", new JsonPrimitive(counts.get(areaCode)),
					"code", new JsonPrimitive(areaCode)), Optional.absent());
			fc.features().add(f);
		}
		
		return ResponseEntity.ok(geoJson.toJson(fc));
	}
	
	@GetMapping(path = "/geojson-by-id")
	public ResponseEntity<String> serverLogin(HttpSession session, 
			@RequestParam(required = false) String tileId) {
		OpBlockChain blc = blocksManager.getBlockchain();
		ObjectsSearchRequest r = new ObjectsSearchRequest();
		OpIndexColumn ind = blocksManager.getIndex("opr.place", DBSchemaManager.INDEX_P[0]);
		blc.fetchObjectsByIndex("opr.place", ind, r, tileId);
		FeatureCollection fc = new FeatureCollection(new ArrayList<>());
		for(OpObject o : r.result) {
			List<Map<String, Object>> osmList = o.getField(null, "source", "osm");
			Map<String, Object> osm = osmList.get(0);
			double lat = (double) osm.get("lat");
			double lon = (double) osm.get("lon");
			Point p = Point.from(lat, lon);
			Builder<String, JsonElement> bld = ImmutableMap.builder();
			for(String k : osm.keySet()) {
				bld.put(k, new JsonPrimitive(osm.get(k).toString()));
			}
			Feature f = new Feature(p, bld.build(), Optional.absent());
			fc.features().add(f);
		}
		return ResponseEntity.ok(geoJson.toJson(fc));
	}
}
