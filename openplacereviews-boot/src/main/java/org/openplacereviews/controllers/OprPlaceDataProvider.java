package org.openplacereviews.controllers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.PublicDataManager.PublicDataProvider;
import org.openplacereviews.osm.parser.OsmLocationTool;
import org.springframework.beans.factory.annotation.Autowired;

import com.github.filosganga.geogson.gson.GeometryAdapterFactory;
import com.github.filosganga.geogson.model.Feature;
import com.github.filosganga.geogson.model.FeatureCollection;
import com.github.filosganga.geogson.model.Point;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.openlocationcode.OpenLocationCode;

public class OprPlaceDataProvider implements PublicDataProvider {
	
	private Gson geoJson;
	
	@Autowired
	private BlocksManager blocksManager;

	
	public OprPlaceDataProvider() {
		geoJson = new GsonBuilder().registerTypeAdapterFactory(new GeometryAdapterFactory()).create();
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
	@Override
	public String getContent() {
		return geoJson.toJson(getAllIds());
	}

}
