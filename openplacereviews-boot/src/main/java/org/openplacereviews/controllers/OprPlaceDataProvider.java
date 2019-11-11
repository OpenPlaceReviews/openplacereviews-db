package org.openplacereviews.controllers;



import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpIndexColumn;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.DBSchemaManager;
import org.openplacereviews.opendb.service.IPublicDataProvider;
import org.openplacereviews.opendb.service.PublicDataManager.CacheHolder;
import org.openplacereviews.opendb.service.PublicDataManager.PublicAPIEndpoint;
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
import com.google.common.collect.ImmutableMap.Builder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class OprPlaceDataProvider implements IPublicDataProvider<String, MapCollection> {
	
	protected static final String PARAM_TILE_ID = "tileid";
	protected static final int INDEXED_TILEID = 6;
	
	// TODO delete
	public static final String OSM_ID = "osm_id";
	// TODO delete
	public static final String OSM_TYPE = "osm_type";
	
	public static final String TITLE = "title";
	public static final String SUBTITLE = "subtitle";
	public static final String COLOR = "color";
	public static final String PLACE_TYPE = "place_type";
	public static final String OPR_ID = "opr_id";
	public static final String OSM_VALUE = "osm_value";
	
	public static final String SOURCES = "sources";
	public static final String TAGS = "tags";
	
	public static final String ID = "id";
	public static final String TYPE = "type";
	public static final String VERSION = "version";
	public static final String CHANGESET = "changeset";
	public static final String SOURCE_TYPE = "source_type";
	public static final String SOURCE_INDEX = "source_ind";
	
	protected Gson geoJson;
	
	@Autowired
	protected BlocksManager blocksManager;
	
	// TODO store in blockchain mapping + translations
	public Map<String, String> placeTypes = new TreeMap<String, String>();
	
	public OprPlaceDataProvider() {
		geoJson = new GsonBuilder().registerTypeAdapterFactory(new GeometryAdapterFactory()).create();
		initPlaceTypes();
	}
	
	private void initPlaceTypes() {
		placeTypes.put("ice_cream", "Ice cream");
		placeTypes.put("cafe", "Cafe");
		placeTypes.put("bar", "Bar");
		placeTypes.put("restaurant", "Restaurant");
		placeTypes.put("biergarten", "Biergarten");
		placeTypes.put("fast_food", "Fast food");
		placeTypes.put("food_court", "Food court");
		placeTypes.put("pub", "Pub");
		placeTypes.put("hotel", "Hotel");
		placeTypes.put("motel", "Motel");
		placeTypes.put("hostel", "Hostel");
		placeTypes.put("apartment", "Apartment");
		placeTypes.put("guest_house", "Guest house");
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
			if (o.isDeleted()) {
				continue;
			}
			List<Map<String, Object>> osmList = o.getField(null, "source", "osm");
			if (osmList.size() == 0) {
				continue;
			}
			Map<String, Object> mainOSM = osmList.get(0);
			double lat = (double) mainOSM.get(ATTR_LATITUDE);
			double lon = (double) mainOSM.get(ATTR_LONGITUDE);
			Point p = Point.from(lon, lat);
			ImmutableMap.Builder<String, JsonElement> bld = ImmutableMap.builder();
			bld.put(OPR_ID, new JsonPrimitive(o.getId().get(0) + "," + o.getId().get(1)));
			bld.put(PLACE_TYPE, new JsonPrimitive((String) mainOSM.get(OSM_VALUE)));
			bld.put(TITLE, new JsonPrimitive(getTitle(mainOSM)));
			bld.put(SUBTITLE, new JsonPrimitive(getSubTitle(mainOSM)));
			JsonObject mainTags = new JsonObject();
			
			
			
			JsonArray sources = new JsonArray();
			Map<String, List<Map<String, Object>>> sourcesObj = o.getField(null, "source");
			for(String tp : sourcesObj.keySet()) {
				List<Map<String, Object>> listValues = sourcesObj.get(tp);
				for(int ind = 0; ind < listValues.size(); ind++) {
					JsonObject obj = new JsonObject();
					Map<String, Object> sourceObj = listValues.get(ind);
					obj.add(SOURCE_TYPE, new JsonPrimitive(tp));
					obj.add(SOURCE_INDEX, new JsonPrimitive(ind));
					put(obj, ID, sourceObj);
					put(obj, TYPE, sourceObj);
					put(obj, VERSION, sourceObj);
					put(obj, CHANGESET, sourceObj);
					put(obj, ATTR_LATITUDE, sourceObj);
					put(obj, ATTR_LONGITUDE, sourceObj);
					Map<String, Object> tagsValue = (Map<String, Object>) sourceObj.get(TAGS);
					if (tagsValue != null) {
						JsonObject tagsObj = new JsonObject();
						Iterator<Entry<String, Object>> it = tagsValue.entrySet().iterator();
						while (it.hasNext()) {
							Entry<String, Object> e = it.next();
							tagsObj.add(e.getKey(), new JsonPrimitive(e.getValue().toString()));
							// TODO refactor to be more specific
							if(ind == 0 && tp.equals("osm")) {
								JsonObject val = new JsonObject();
								val.add("value", new JsonPrimitive(e.getValue().toString()));
								val.add("source", new JsonPrimitive(tp));
								mainTags.add(e.getKey(), val);
							}
						}
						
						obj.add(TAGS, tagsObj);
					}
					
					sources.add(obj);
				}
			}
			bld.put(SOURCES, sources);
			
			
			bld.put(TAGS, mainTags);
			Feature f = new Feature(p, bld.build(), Optional.absent());
			fc.features().add(f);
		}
	}

	private void put(JsonObject obj, String key, Map<String, Object> sourceObj) {
		Object o = sourceObj.get(key);
		if(o instanceof Number) {
			obj.add(key, new JsonPrimitive((Number) o));
		} else if(o instanceof String) {
			obj.add(key, new JsonPrimitive((String) o));
		} else if(o instanceof Boolean) {
			obj.add(key, new JsonPrimitive((Boolean) o));
		} else if(o instanceof List) {
			JsonArray jsonArray = new JsonArray();
			for (String object : (List<String>)o) {
				jsonArray.add(object);
			}
			obj.add(key, jsonArray);
		} else {
			if(o != null) {
				throw new UnsupportedOperationException();
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected String getTitle(Map<String, Object> osm) {
		Map<String, Object> tagsValue = (Map<String, Object>) osm.get("tags");
		if(tagsValue.containsKey("name")) {
			String name = (String) tagsValue.get("name");
			return name;
		}
		return getSubTitle(osm);
	}
	
	@SuppressWarnings("unchecked")
	protected String getSubTitle(Map<String, Object> osm) {
		Map<String, Object> tagsValue = (Map<String, Object>) osm.get("tags");
		String osmValue = (String) osm.get(OSM_VALUE);
		if(placeTypes.containsKey(osmValue)) {
			osmValue = placeTypes.get(osmValue);
		}
		return osmValue;
	}
	
	private String formatTile(String string) {
		if(string.length() > INDEXED_TILEID) {
			return string.substring(0, INDEXED_TILEID);
		} else {
			return string;
		}
	}

	@Override
	public AbstractResource getMetaPage(Map<String, String[]> params) {
		return new InputStreamResource(OprPlaceDataProvider.class.getResourceAsStream("/mapall.html"));
	}

	@Override
	public List<String> getKeysToCache(PublicAPIEndpoint<String, MapCollection> api) {
		List<String> cacheKeys = new ArrayList<String>(api.getCacheKeys());
		Iterator<String> it = cacheKeys.iterator();
//		long now = api.getNow();
		while(it.hasNext()) {
			String key = it.next();
			CacheHolder<MapCollection> cacheHolder = api.getCacheHolder(key);
			// 1 hour
//			if(now - cacheHolder.access < 3600l) {
//			}
			if(cacheHolder != null && cacheHolder.access == 0 ) {
				it.remove();
				api.removeCacheHolder(key);
			}
		}
		cacheKeys.add("");
		return cacheKeys;
	}

	@Override
	public MapCollection getContent(String tile) {
		MapCollection m = new MapCollection();
		m.tileBased = true;
		m.placeTypes = placeTypes;
		if(!tile.equals("")) {
			fetchObjectsByTileId(formatTile(tile), m.geo);
		}
		return m;
	}

	@Override
	public AbstractResource formatContent(MapCollection m) {
		return new InMemoryResource(geoJson.toJson(m));
	}

	@Override
	public String formatParams(Map<String, String[]> params) {
		String[] tls = params.get(PARAM_TILE_ID);
		if(tls != null && tls.length == 1 && tls[0] != null) {
			return tls[0];
		}
		return "";
	}
	
	@Override
	public String serializeValue(MapCollection v) {
		return geoJson.toJson(v);
	}
	
	@Override
	public MapCollection deserializeValue(String key) {
		return geoJson.fromJson(key, MapCollection.class);
	}

}
