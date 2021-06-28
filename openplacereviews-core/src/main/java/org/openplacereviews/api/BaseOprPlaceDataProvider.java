package org.openplacereviews.api;

import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_DELETED_OSM;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_DELETED_PLACE;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_IMG_REVIEW;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.api.OprMapCollectionApiResult.MapCollectionParameters;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockChain.ObjectsSearchRequest;
import org.openplacereviews.opendb.ops.OpIndexColumn;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.DBSchemaManager;
import org.openplacereviews.opendb.service.IPublicDataProvider;
import org.openplacereviews.opendb.service.PublicDataManager.CacheHolder;
import org.openplacereviews.opendb.service.PublicDataManager.PublicAPIEndpoint;
import org.openplacereviews.opendb.util.OUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.InputStreamResource;

import com.github.filosganga.geogson.gson.GeometryAdapterFactory;
import com.github.filosganga.geogson.model.Feature;
import com.github.filosganga.geogson.model.FeatureCollection;
import com.github.filosganga.geogson.model.Point;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public abstract class BaseOprPlaceDataProvider
		implements IPublicDataProvider<MapCollectionParameters, OprMapCollectionApiResult> {

	private static final Log LOGGER = LogFactory.getLog(BaseOprPlaceDataProvider.class);
	protected static final int INDEXED_TILEID = 6;

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

	public static final String IMG_REVIEW_SIZE = "img_review_size";
	public static final String PLACE_DELETED = "place_deleted";
	public static final String PLACE_DELETED_OSM = "place_deleted_osm";

	protected Gson geoJson;

	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

	@Autowired
	protected BlocksManager blocksManager;

	protected Map<String, String> placeTypes = new LinkedHashMap<String, String>();

	public BaseOprPlaceDataProvider() {
		geoJson = new GsonBuilder().registerTypeAdapterFactory(new GeometryAdapterFactory()).create();
	}

	@Override
	public AbstractResource getMetaPage(Map<String, String[]> params) {
		return new InputStreamResource(BaseOprPlaceDataProvider.class.getResourceAsStream("/mapall.html"));
	}

	public boolean cacheNeedsToBeDeleted(MapCollectionParameters key,
			CacheHolder<OprMapCollectionApiResult> cacheHolder,
			PublicAPIEndpoint<MapCollectionParameters, OprMapCollectionApiResult> api) {
		// long now = api.getNow();
		// we could delete if cache wasn't accessed in 1 hour
		// if(now - cacheHolder.access < 3600l) {
		// }
		return cacheHolder.access == 0;
	}

	@Override
	public List<MapCollectionParameters> getKeysToCache(
			PublicAPIEndpoint<MapCollectionParameters, OprMapCollectionApiResult> api) {
		List<MapCollectionParameters> cacheKeys = new ArrayList<MapCollectionParameters>(api.getCacheKeys());
		Iterator<MapCollectionParameters> it = cacheKeys.iterator();
		while (it.hasNext()) {
			MapCollectionParameters key = it.next();
			CacheHolder<OprMapCollectionApiResult> cacheHolder = api.getCacheHolder(key);

			if (cacheHolder != null && cacheNeedsToBeDeleted(key, cacheHolder, api)) {
				it.remove();
				api.removeCacheHolder(key);
			}
		}
		return cacheKeys;
	}

	@Override
	public MapCollectionParameters formatParams(Map<String, String[]> params) {
		MapCollectionParameters res = new MapCollectionParameters();
		res.tileId = getParam(params, OprMapCollectionApiResult.PARAM_TILE_BASED_KEY);
		res.requestFilter = getParam(params, OprMapCollectionApiResult.PARAM_REQUEST_FILTER);
		res.date = parseDate(getParam(params, OprMapCollectionApiResult.PARAM_DATE_KEY));
		res.date2 = parseDate(getParam(params, OprMapCollectionApiResult.PARAM_DATE2_KEY));
		return res;
	}

	protected Date parseDate(String param) {
		try {
			if (param != null) {
				return DATE_FORMAT.parse(param);
			}
		} catch (ParseException e) {
			LOGGER.warn(e.getMessage(), e);
		}
		return null;
	}

	private String getParam(Map<String, String[]> params, String key) {
		String v = null;
		String[] tls = params.get(key);
		if (tls != null && tls.length == 1 && tls[0] != null) {
			v = tls[0];
		}
		return v;
	}

	@Override
	public AbstractResource formatContent(OprMapCollectionApiResult m) {
		return new ByteArrayResource(geoJson.toJson(m).getBytes());
	}

	@Override
	public String serializeValue(OprMapCollectionApiResult v) {
		return geoJson.toJson(v);
	}

	@Override
	public OprMapCollectionApiResult deserializeValue(String key) {
		return geoJson.fromJson(key, OprMapCollectionApiResult.class);
	}

	@SuppressWarnings("unchecked")
	public Map<String, String> placeTypes() {
		if (placeTypes.isEmpty() && blocksManager != null && blocksManager.getBlockchain() != null) {
			ObjectsSearchRequest r = new ObjectsSearchRequest();
			Map<String, String> res = new LinkedHashMap<>();
			blocksManager.getBlockchain().fetchAllObjects("sys.bot", r);
			for (OpObject o : r.result) {
				Map<String, Object> obj = (Map<String, Object>) o.getFieldByExpr("config.osm-tags");
				if (obj != null) {
					for (String typeKey : obj.keySet()) {
						Map<String, Object> values = (Map<String, Object>) obj.get(typeKey);
						List<String> types = (List<String>) values.get("values");
						if (types != null) {
							for (String pt : types) {
								String name = OUtils.capitalizeFirstLetter(pt).replace('_', ' ');
								name = OUtils.capitalizeFirstLetter(typeKey) + " - " + name;
								res.put(pt, name);
							}
						}
					}
				}
			}
			final Comparator<String> c = Comparator.naturalOrder();
			List<String> sortedList = new ArrayList<>(res.keySet());
			sortedList.sort(new Comparator<String>() {

				@Override
				public int compare(String o1, String o2) {
					return c.compare(res.get(o1), res.get(o2));
				}
			});
			Map<String, String> finalRes = new LinkedHashMap<>();
			for (String sortedKey : sortedList) {
				finalRes.put(sortedKey, res.get(sortedKey));
			}
			placeTypes = finalRes;

		}
		return placeTypes;

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

			Map<String, Object> mainOSM = getMainOsmFromList(o);
			if (mainOSM == null) {
				continue;
			}
			ImmutableMap.Builder<String, JsonElement> bld = ImmutableMap.builder();
			if (mainOSM.containsKey(F_DELETED_OSM)) {
				bld.put(PLACE_DELETED_OSM, new JsonPrimitive(F_DELETED_OSM));
			}
			bld.put(OPR_ID, new JsonPrimitive(o.getId().get(0) + "," + o.getId().get(1)));

			Object imgReviewField = o.getFieldByExpr(F_IMG_REVIEW);
			if (imgReviewField != null) {
				bld.put(IMG_REVIEW_SIZE, new JsonPrimitive(String.valueOf(((List<?>) imgReviewField).size())));
			}
			Object deletedPlaceField = o.getField(null, F_DELETED_PLACE);
			if (deletedPlaceField != null) {
				bld.put(PLACE_DELETED, new JsonPrimitive(String.valueOf(deletedPlaceField)));
			}

			double lat = (double) mainOSM.get(ATTR_LATITUDE);
			double lon = (double) mainOSM.get(ATTR_LONGITUDE);
			Point p = Point.from(lon, lat);
			bld.put(PLACE_TYPE, new JsonPrimitive((String) mainOSM.get(OSM_VALUE)));
			bld.put(TITLE, new JsonPrimitive(getTitle(mainOSM)));
			bld.put(SUBTITLE, new JsonPrimitive(getSubTitle(mainOSM)));
			JsonObject mainTags = new JsonObject();

			JsonArray sources = new JsonArray();
			Map<String, List<Map<String, Object>>> sourcesObj = o.getField(null, "source");
			Set<String> mainTagKeys = new TreeSet<String>();
			for (String tp : sourcesObj.keySet()) {
				List<Map<String, Object>> listValues = sourcesObj.get(tp);
				for (int ind = 0; ind < listValues.size(); ind++) {
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
							// for now specify main tags from 1st source
							if (mainTagKeys.add(e.getKey())) {
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

	private Map<String, Object> getMainOsmFromList(OpObject o) {
		List<Map<String, Object>> osmList = o.getField(null, "source", "osm");
		if (osmList == null) {
			return null;
		}
		Map<String, Object> main = null;
		for (Map<String, Object> m : osmList) {
			if (m.containsKey(ATTR_LATITUDE) && m.containsKey(ATTR_LONGITUDE) && m.containsKey(OSM_VALUE)) {
				if(!m.containsKey(F_DELETED_OSM)) {
					return m;
				}
				if (main != null) {
					main = m;
				}
			}
		}
		return main;
	}

	@SuppressWarnings("unchecked")
	private void put(JsonObject obj, String key, Map<String, Object> sourceObj) {
		Object o = sourceObj.get(key);
		if (o instanceof Number) {
			obj.add(key, new JsonPrimitive((Number) o));
		} else if (o instanceof String) {
			obj.add(key, new JsonPrimitive((String) o));
		} else if (o instanceof Boolean) {
			obj.add(key, new JsonPrimitive((Boolean) o));
		} else if (o instanceof List) {
			JsonArray jsonArray = new JsonArray();
			for (String object : (List<String>) o) {
				jsonArray.add(object);
			}
			obj.add(key, jsonArray);
		} else {
			if (o != null) {
				throw new UnsupportedOperationException();
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected String getTitle(Map<String, Object> osm) {
		Map<String, Object> tagsValue = (Map<String, Object>) osm.get("tags");
		if (tagsValue != null && tagsValue.containsKey("name")) {
			String name = (String) tagsValue.get("name");
			return name;
		}
		return getSubTitle(osm);
	}

	protected String getSubTitle(Map<String, Object> osm) {
		String osmValue = (String) osm.get(OSM_VALUE);
		if (placeTypes().containsKey(osmValue)) {
			osmValue = placeTypes().get(osmValue);
		}
		return osmValue;
	}

	protected String formatTile(String string) {
		if (string.length() > INDEXED_TILEID) {
			return string.substring(0, INDEXED_TILEID);
		} else {
			return string;
		}
	}
}
