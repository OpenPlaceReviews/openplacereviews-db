package org.openplacereviews.api;

import static org.openplacereviews.opendb.ops.OpObject.F_CHANGE;
import static org.openplacereviews.osm.model.Entity.ATTR_ID;
import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.*;

import java.text.ParseException;
import java.util.*;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.api.OprMapCollectionApiResult.MapCollectionParameters;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.PublicDataManager.CacheHolder;
import org.openplacereviews.opendb.service.PublicDataManager.PublicAPIEndpoint;
import org.openplacereviews.osm.model.OsmMapUtils;
import org.openplacereviews.osm.util.PlaceOpObjectHelper;

import com.github.filosganga.geogson.model.Feature;
import com.github.filosganga.geogson.model.FeatureCollection;
import com.github.filosganga.geogson.model.Point;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class OprHistoryChangesProvider extends BaseOprPlaceDataProvider {

	private static final Log LOGGER = LogFactory.getLog(OprHistoryChangesProvider.class);
	
	private enum RequestFilter {
		REVIEW_IMAGES("Review new images"),
		POSSIBLE_MERGE("Review duplicate places");
		
		private String hm;
		RequestFilter(String hm) {
			this.hm = hm;
		}
		
		String humanString() {
			return hm;
		}
	}

	public static final String OSM_ID = "osm_id";
	public static final String OSM_TYPE = "osm_type";
	
	public static final String OSM_INDEX = "osm_index";
	public static final String BLOCK_TIMESTAMP = "block_timestamp";
	public static final String BLOCK_HASH = "block_hash";
	public static final String BLOCK_ID = "block_id";
	public static final String OP_HASH = "op_hash";
	public static final String OPR_PLACE = "opr.place";
	public static final String OBJ_PREV = "prev";
	public static final String OBJ_NEXT = "next";

	public static final String ATTR_TYPE = "type";
	public static final String ATTR_SET = "set";

	// constants ? or human strings?
	public static final String OBJ_CREATED = "Created";
	public static final String OBJ_EDITED = "Edited";
	public static final String OBJ_REMOVED = "Removed";
	
	protected static final String COLOR_GREEN = "green";
	protected static final String COLOR_BLUE = "blue";
	protected static final String COLOR_RED = "red";

	long resCreated = 0;
	long resEdited = 0;
	long resFilterMerge = 0;
	long resFilterObjects = 0;

	@Override
	public OprMapCollectionApiResult getContent(MapCollectionParameters params) {
		OprMapCollectionApiResult fc = new OprMapCollectionApiResult();
		// fc.parameters.put(OprMapCollectionApiResult.PARAM_TILE_BASED_KEY, true);
		fc.parameters.put(OprMapCollectionApiResult.PARAM_PLACE_FILTER, placeTypes());
		Map<String, String> requestFilter = new TreeMap<String, String>();
		for (RequestFilter f : RequestFilter.values()) {
			requestFilter.put(f.name(), f.humanString());
		}
		fc.parameters.put(OprMapCollectionApiResult.PARAM_REQUEST_FILTER, requestFilter);
		fc.parameters.put(OprMapCollectionApiResult.PARAM_DATE_KEY, true);
		fc.parameters.put(OprMapCollectionApiResult.PARAM_DATE2_KEY, true);
		if (params.date != null) {
			try {
				retrievePlacesByDate(params.date, params.date2, params.requestFilter, fc.geo);
			} catch (ParseException e) {
				LOGGER.error("Incorrect 'date' format", e);
			}
		}
		return fc;
	}

	
	public void retrievePlacesByDate(Date date, Date date2, String requestFilter, FeatureCollection fc) throws ParseException {
		List<OpBlock> listBlocks = blocksManager.getBlockchain().getBlockHeaders(-1);
		List<OpBlock> blocksByDate = new LinkedList<>();
		Date nextDate = date2 != null ? DateUtils.addDays(date2, 1) : DateUtils.addDays(date, 1) ; 
		for (OpBlock opBlock : listBlocks) {
			Date blockDate = OpBlock.dateFormat.parse(opBlock.getDateString());
			if (date.getTime() <= blockDate.getTime() && blockDate.getTime() <= nextDate.getTime()) {
				blocksByDate.add(opBlock);
			}
		}
		RequestFilter r = null;
		if (requestFilter != null && requestFilter.length() > 0 && !requestFilter.equals("all")) {
			r = RequestFilter.valueOf(requestFilter);
		}
		Set<String> placeIdsAdded = new TreeSet<>();
		OpBlockChain blc = blocksManager.getBlockchain();
		LOGGER.info("Get history started...");
		LOGGER.info("BlocksByDate size = " + blocksByDate.size());
		for (OpBlock block : blocksByDate) {
			OpBlock fullBlock = blc.getFullBlockByRawHash(block.getRawHash());
			List<OpOperation> opOperations = fullBlock.getOperations();
			long startTime4 = System.currentTimeMillis();
			LOGGER.info("OpOperations size = " + opOperations.size());
			for (OpOperation opOperation : opOperations) {
				if (opOperation.getType().equals(OPR_PLACE)) {
					filterObjects(r, fc, block, opOperation, placeIdsAdded);
				}
			}
			long endTime4 = System.currentTimeMillis();
			resFilterObjects = resFilterObjects + (endTime4 - startTime4);
		}
		LOGGER.info("Get opOperation.getCreated() = " + resCreated + " ms");
		LOGGER.info("Get opOperation.getEdited() = " + resEdited + " ms");
		LOGGER.info("Get result merge places = " + resFilterMerge + " ms");
		LOGGER.info("Get filterObjects = " + resFilterObjects + "ms");
	}


	private void filterObjects(RequestFilter filter, FeatureCollection fc, OpBlock opBlock, OpOperation opOperation,
			Set<String> placeIdsAdded) {
		String opHash = opOperation.getRawHash();
		Map<String, List<Feature>> createdObjects = new TreeMap<>();
		Map<String, List<Feature>> deletedObjects = new TreeMap<>();
		Map<String, String> additionalFieldsMap = new HashMap<>();
		long startTime = System.currentTimeMillis();
		LOGGER.info("opOperation.getCreated() started...");
		for (OpObject opObject : opOperation.getCreated()) {
			if (filter == RequestFilter.POSSIBLE_MERGE) {
				OpObject nObj = blocksManager.getBlockchain().getObjectByName(OPR_PLACE, opObject.getId());
				if (nObj != null) {
					addDeletedPlaceField(additionalFieldsMap, nObj);
					generateEntity(createdObjects, opBlock, opHash, opObject, OBJ_CREATED, COLOR_GREEN, additionalFieldsMap);
				}
			}
		}
		LOGGER.info("opOperation.getCreated() finished...");
		long endTime = System.currentTimeMillis();
		resCreated = resCreated + (endTime - startTime);
		long startTime2 = System.currentTimeMillis();
		for (OpObject opObject : opOperation.getEdited()) {
			Map<String, Object> change = opObject.getStringObjMap(F_CHANGE);
			
			changeKeys: for (String changeKey : change.keySet()) {
				if (filter == RequestFilter.REVIEW_IMAGES) {
					if (changeKey.startsWith(F_IMG_REVIEW)) {
						OpObject nObj = blocksManager.getBlockchain().getObjectByName(OPR_PLACE, opObject.getId());
						if (nObj != null && placeIdsAdded.add(generateStringId(nObj))) {
							addImgReviewField(additionalFieldsMap, nObj);
							generateEntity(createdObjects, opBlock, opHash, nObj, OBJ_CREATED, COLOR_GREEN, additionalFieldsMap);
						}
						break changeKeys;
					}	
				} else if (filter == RequestFilter.POSSIBLE_MERGE) {
					// "source.osm[0]": "delete"
	                // "source.osm[0].deleted": {
		            // 		"set": "2021-02-08T17:18:40.393+0000"
		            // }

					int ind = getOsmSourceIndexDeleted(changeKey);
					if (ind != -1) {
						OpObject nObj = blocksManager.getBlockchain().getObjectByName(OPR_PLACE, opObject.getId());
						if (nObj != null) {
							List<Map<String, Object>> osmSources = nObj.getField(null, F_SOURCE, F_OSM);
							Map<String, Object> osm = null;
							boolean allOsmRefsDeleted = true;
							for (int i = 0; i < osmSources.size(); i++) {
								Map<String, Object> lmp = osmSources.get(i);
								if (!lmp.containsKey(PlaceOpObjectHelper.F_DELETED_OSM)) {
									allOsmRefsDeleted = false;
								}
								if (i == ind) {
									osm = lmp;
								}
							}
							if (allOsmRefsDeleted && osm != null && nObj.getField(null, F_DELETED_PLACE) == null) {
								addDeletedPlaceField(additionalFieldsMap, nObj);
								addDeletedFeature(deletedObjects, ind, osm, opBlock, opHash, opObject, additionalFieldsMap);
								break changeKeys;
							}
						}
					}
				} else {
					throw new UnsupportedOperationException();
				}
			}				
		}
		long endTime2 = System.currentTimeMillis();
		resEdited = resEdited + (endTime2 - startTime2);
		// ignore deleted cause they are typical merge action
//		for (List<String> objId : opOperation.getDeleted()) {
//			addRemovedEntityFromOpObject(objId, deletedObjects, opBlock, opHash);
//		}
		long startTime3 = System.currentTimeMillis();
		if (filter == RequestFilter.POSSIBLE_MERGE) {
			Set<String> tiles = createdObjects.keySet();
			for (String tileId : tiles) {
				List<Feature> delList = deletedObjects.get(tileId);
				List<Feature> cList = createdObjects.get(tileId);
				if (cList == null || delList == null) {
					continue;
				}
				// make linked list for quick delete in the middle
				LinkedList<Feature> deletedPoints = new LinkedList<Feature>(delList);
				LinkedList<Feature> createdPoints = new LinkedList<Feature>(cList);
				while (!createdPoints.isEmpty()) {
					List<Feature> merged = new ArrayList<>();
					Feature fnew = createdPoints.poll();
					Point pnew = (Point) fnew.geometry();
					findNearestPointAndDelete(deletedPoints, merged, pnew);
					if (!merged.isEmpty()) {
						merged.add(0, fnew);
					}
					if (merged.size() > 0) {
						// find other created points within distance of 150m
						findNearestPointAndDelete(createdPoints, merged, pnew);
						// ! always make sure that groups are following [new, ..., new, deleted, deleted, ..., deleted]
                        addMergedPlaces(fc.features(), merged);
					}
				}
			}
		} else if (filter == RequestFilter.REVIEW_IMAGES) {
			Set<String> tiles = createdObjects.keySet();
			for (String tileId : tiles) {
				List<Feature> cList = createdObjects.get(tileId);
				fc.features().addAll(cList);
			}
		}
		long endTime3 = System.currentTimeMillis();
		resFilterMerge = resFilterMerge + (endTime3 - startTime3);
	}


	private void addMergedPlaces(List<Feature> features, List<Feature> merged) {
		double latm = merged.get(0).properties().get(ATTR_LATITUDE).getAsDouble();
		double lonm = merged.get(0).properties().get(ATTR_LONGITUDE).getAsDouble();
		int start = -1;
		boolean groupFound = false;
		for (int i = 0; i < features.size(); i++) {
			if (!groupFound) {
				double lat = features.get(i).properties().get(ATTR_LATITUDE).getAsDouble();
				double lon = features.get(i).properties().get(ATTR_LONGITUDE).getAsDouble();
				if (OsmMapUtils.getDistance(lat, lon, latm, lonm) < 150
						&& !features.get(i).properties().containsKey(F_DELETED_PLACE)) {
					groupFound = true;
					start = i;
				}
			} else {
				if (features.get(i).properties().containsKey(F_DELETED_PLACE)) {
					List<Feature> deletedFeatures = new ArrayList<>();
					List<Feature> createdFeatures = new ArrayList<>();
					for (Feature mf : merged) {
						if (mf.properties().containsKey(F_DELETED_PLACE)) {
							deletedFeatures.add(mf);
						} else {
							createdFeatures.add(mf);
						}
					}
					features.addAll(start, createdFeatures);
					features.addAll(i + createdFeatures.size(), deletedFeatures);
					break;
				}
			}
		}
		if (!groupFound) {
			features.addAll(merged);
		}
	}

	private void findNearestPointAndDelete(LinkedList<Feature> list, List<Feature> merged, Point point) {
		Iterator<Feature> it = list.iterator();
		while (it.hasNext()) {
			Feature featureToFind = it.next();
			Point pntToFind = (Point) featureToFind.geometry();
			if (OsmMapUtils.getDistance(point.lat(), point.lon(), pntToFind.lat(), pntToFind.lon()) < 150) {
				merged.add(0, featureToFind);
				it.remove();
			}
		}
	}

	private void addDeletedPlaceField(Map<String, String> fieldsMap, OpObject nObj) {
		Object deletedPlace = nObj.getField(null, F_DELETED_PLACE);
		if (deletedPlace != null) {
			fieldsMap.putAll(Map.of(PLACE_DELETED, deletedPlace.toString()));
		}
	}

	private void addImgReviewField(Map<String, String> fieldsMap, OpObject nObj) {
		Object imgReview = nObj.getFieldByExpr(F_IMG_REVIEW);
		if (imgReview != null) {
			fieldsMap.putAll(Map.of(IMG_REVIEW_SIZE, String.valueOf(((List<?>) imgReview).size())));
		}
	}

	private void addDeletedFeature(Map<String, List<Feature>> deletedObjects, int osmIndex, Map<String, Object> osm,
								   OpBlock opBlock, String opHash, OpObject opObject, Map<String, String> additionalFields) {
		ImmutableMap.Builder<String, JsonElement> bld = ImmutableMap.builder();
		bld.put(OSM_INDEX, new JsonPrimitive(osmIndex));
		bld.put(TITLE, new JsonPrimitive(OBJ_REMOVED + " " + getTitle(osm)));
		bld.put(COLOR, new JsonPrimitive(COLOR_RED));
		bld.put(PLACE_TYPE, new JsonPrimitive((String) osm.get(PlaceOpObjectHelper.F_OSM_VALUE)));
		generateAdditionalFields(additionalFields, bld);
		generateFieldsFromOsmSource(osm, bld);
		generateObjectBlockInfo(opObject, opBlock, opHash, bld);
		Feature f = new Feature(generatePoint(osm), bld.build(), Optional.absent());
		add(deletedObjects, opObject.getId().get(0), f);
	}


	private void generateEntity(Map<String, List<Feature>> objects, OpBlock opBlock, String opHash, OpObject opObject, 
			String status, String color, Map<String, String> additionalFields) {
		List<Map<String, Object>> osmList = opObject.getField(null, F_SOURCE, F_OSM);
		for (int i = 0; i < osmList.size(); i++) {
			Map<String, Object> osm = osmList.get(i);
			ImmutableMap.Builder<String, JsonElement> bld = ImmutableMap.builder();
			bld.put(OSM_INDEX, new JsonPrimitive(i));
			bld.put(TITLE, new JsonPrimitive(status + " " + getTitle(osm)));
			bld.put(COLOR, new JsonPrimitive(color));
			String placeType = (String) osm.get(PlaceOpObjectHelper.F_OSM_VALUE);
			if (placeType == null) {
				continue;
			}
			bld.put(PLACE_TYPE, new JsonPrimitive(placeType));
			generateAdditionalFields(additionalFields, bld);
			generateFieldsFromOsmSource(osm, bld);
			generateObjectBlockInfo(opObject, opBlock, opHash, bld);
			Feature f = new Feature(generatePoint(osm), bld.build(), Optional.absent());
			add(objects, opObject.getId().get(0), f);
		}
	}

	private void generateAdditionalFields(Map<String, String> additionalFields, ImmutableMap.Builder<String, JsonElement> bld) {
		if (additionalFields != null && additionalFields.size() != 0) {
			for (Map.Entry<String, String> entry : additionalFields.entrySet()) {
				bld.put(entry.getKey(), new JsonPrimitive(entry.getValue()));
			}
		}
	}

	private void add(Map<String, List<Feature>> objects, String tileId, Feature f) {
		if (!objects.containsKey(tileId)) {
			objects.put(tileId, new ArrayList<>());
		}
		objects.get(tileId).add(f);
	}
	
	@Override
	public boolean operationAdded(PublicAPIEndpoint<MapCollectionParameters, OprMapCollectionApiResult> api,
			OpOperation op, OpBlock block) {
		boolean changed = false;
		if (op.getType().equals(OPR_PLACE)) {
			// not tile based, so we need to update all caches
//			Set<String> tileIds = new TreeSet<String>();
//			for (OpObject opObject : op.getEdited()) {
//				String tileId = opObject.getId().get(0);
//				tileIds.add(tileId);
//			}
//			for (OpObject opObject : op.getCreated()) {
//				String tileId = opObject.getId().get(0);
//				tileIds.add(tileId);
//			}
//			for (List<String> opObject : op.getDeleted()) {
//				String tileId = opObject.get(0);
//				tileIds.add(tileId);
//			}
			for (MapCollectionParameters p : api.getCacheKeys()) {
//				if (tileIds.contains(p.tileId)) {
					CacheHolder<OprMapCollectionApiResult> holder = api.getCacheHolder(p);
					if (holder != null) {
						holder.forceUpdate = true;
						changed = true;
					}
//				}
			}
		}
		return changed;
	}


	@SuppressWarnings("unchecked")
	private void generateFieldsFromOsmSource(Map<String, Object> osm, ImmutableMap.Builder<String, JsonElement> bld) {
		for (String k : osm.keySet()) {
			if (k.equals(F_TAGS)) {
				continue;
			}
			if (k.equals(ATTR_ID)) {
				bld.put(OSM_ID, new JsonPrimitive((long) osm.get(ATTR_ID)));
				continue;
			}
			if (k.equals(ATTR_TYPE)) {
				bld.put(OSM_TYPE, new JsonPrimitive((String) osm.get(ATTR_TYPE)));
				continue;
			}
			bld.put(k, new JsonPrimitive(osm.get(k).toString()));
		}
		Map<String, Object> tagsValue = (Map<String, Object>) osm.get(F_TAGS);
		generateTagsForEntity(bld, tagsValue);
	}
	
	private int getOsmSourceIndexDeleted(String key) {
		String prefix = F_SOURCE + "." + F_OSM + "["; 
		String suffix = "]." + F_DELETED_OSM;
		if (key.startsWith(prefix) && key.endsWith(suffix)) {
			String intInd = key.substring(prefix.length(), key.length() - suffix.length());
			try {
				return Integer.parseInt(intInd);
			} catch (NumberFormatException ne) {
			}
		}
		return -1;
	}

	private Point generatePoint(Map<String, Object> osm) {
		double lat = ((Number)osm.get(ATTR_LATITUDE)).doubleValue();
		double lon = ((Number)osm.get(ATTR_LONGITUDE)).doubleValue();
		return Point.from(lon, lat);
	}

	
	private void generateObjectBlockInfo(OpObject opObject, OpBlock opBlock, String opHash, ImmutableMap.Builder<String, JsonElement> bld) {
		bld.put(OPR_ID, new JsonPrimitive(generateStringId(opObject)));
		bld.put(BLOCK_TIMESTAMP, new JsonPrimitive(opBlock.getDateString()));
		bld.put(BLOCK_HASH, new JsonPrimitive(opBlock.getRawHash()));
		bld.put(BLOCK_ID, new JsonPrimitive(opBlock.getBlockId()));
		bld.put(OP_HASH, new JsonPrimitive(opHash));
	}

	private String generateStringId(OpObject opObject) {
		return opObject.getId().get(0) + "," + opObject.getId().get(1);
	}


	private void generateTagsForEntity(ImmutableMap.Builder<String, JsonElement> bld, Map<String, Object> tagsValue) {
		if (tagsValue != null) {
			JsonObject obj = new JsonObject();
			for (Map.Entry<String, Object> e : tagsValue.entrySet()) {
				obj.add(e.getKey(), new JsonPrimitive(e.getValue().toString()));
			}
			bld.put(F_TAGS, obj);
		}
	}


}
