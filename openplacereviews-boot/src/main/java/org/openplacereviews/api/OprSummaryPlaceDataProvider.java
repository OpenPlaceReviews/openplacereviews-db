package org.openplacereviews.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openplacereviews.api.OprMapCollectionApiResult.MapCollectionParameters;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.service.PublicDataManager.PublicAPIEndpoint;
import org.openplacereviews.osm.parser.OsmLocationTool;

import com.github.filosganga.geogson.model.Feature;
import com.github.filosganga.geogson.model.Point;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.openlocationcode.OpenLocationCode;

public class OprSummaryPlaceDataProvider extends BaseOprPlaceDataProvider {
	
	private static final int TILE_INFO_SUBSET = 4;
	
	@Override
	public OprMapCollectionApiResult getContent(MapCollectionParameters params) {
		OpBlockChain blc = blocksManager.getBlockchain();
		OpBlockChain.ObjectsSearchRequest r = new OpBlockChain.ObjectsSearchRequest();
		r.requestOnlyKeys = true;
		blc.fetchAllObjects("opr.place", r);
		List<CompoundKey> ks = r.keys;
		Map<String, Integer> counts = new HashMap<>();
		OprMapCollectionApiResult m = new OprMapCollectionApiResult();
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
			ImmutableMap<String, JsonElement> props = 
					ImmutableMap.of(
							"title", new JsonPrimitive(
									areaCode + " " + counts.get(areaCode) + " places"),
							"counts", new JsonPrimitive(counts.get(areaCode)),
							"code", new JsonPrimitive(areaCode));
			Feature f = new Feature(p, props, Optional.absent());
			m.geo.features().add(f);
		}

		return m;
	}
	
	@Override
	public List<MapCollectionParameters> getKeysToCache(PublicAPIEndpoint<MapCollectionParameters, OprMapCollectionApiResult> api) {
		return Collections.singletonList(new MapCollectionParameters());
	}
	

}
