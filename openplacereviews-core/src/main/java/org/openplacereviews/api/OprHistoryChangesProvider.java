package org.openplacereviews.api;

import static org.openplacereviews.opendb.ops.OpObject.F_CHANGE;
import static org.openplacereviews.osm.model.Entity.ATTR_ID;
import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;
import static org.openplacereviews.osm.util.MergeUtil.*;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.*;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.api.OprMapCollectionApiResult.MapCollectionParameters;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.PublicDataManager;
import org.openplacereviews.opendb.service.PublicDataManager.CacheHolder;
import org.openplacereviews.opendb.service.PublicDataManager.PublicAPIEndpoint;
import org.openplacereviews.osm.model.OsmMapUtils;
import org.openplacereviews.osm.util.PlaceOpObjectHelper;

import com.github.filosganga.geogson.model.Feature;
import com.github.filosganga.geogson.model.Point;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.springframework.beans.factory.annotation.Autowired;

public class OprHistoryChangesProvider extends BaseOprPlaceDataProvider {

	private static final Log LOGGER = LogFactory.getLog(OprHistoryChangesProvider.class);
	private static final boolean SKIP_INADVANCE_REV_CLOSED_PLACES_REPORT = true;

	@Autowired
	private PublicDataManager dataManager;
	
	public enum RequestFilter {
		REVIEW_IMAGES("Review new images"),
		REVIEW_CLOSED_PLACES("Review closed places");
		
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
	public static final String OPR_PLACE = "opr.place";

	public static final String ATTR_TYPE = "type";

	// constants ? or human strings?
	public static final String OBJ_CREATED = "Created";
	public static final String OBJ_EDITED = "Edited";
	public static final String OBJ_REMOVED = "Removed";
	
	protected static final String COLOR_GREEN = "green";
	protected static final String COLOR_RED = "red";

	@Override
	public OprMapCollectionApiResult getContent(MapCollectionParameters params) {
		OprMapCollectionApiResult fc = new OprMapCollectionApiResult();
		// fc.parameters.put(OprMapCollectionApiResult.PARAM_TILE_BASED_KEY, true);
		fc.parameters.put(OprMapCollectionApiResult.PARAM_PLACE_FILTER, placeTypes());
		Map<String, String> requestFilter = new TreeMap<>();
		for (RequestFilter f : RequestFilter.values()) {
			requestFilter.put(f.name(), f.humanString());
		}
		fc.parameters.put(OprMapCollectionApiResult.PARAM_REQUEST_FILTER, requestFilter);
		fc.parameters.put(OprMapCollectionApiResult.PARAM_DATE_KEY, true);
		fc.parameters.put(OprMapCollectionApiResult.PARAM_DATE2_KEY, true);
		if (params.date != null) {
			RequestFilter filter = null;
			if (params.requestFilter != null && params.requestFilter.length() > 0 && !requestFilter.equals("all")) {
				filter = RequestFilter.valueOf(params.requestFilter);
			}
			Date nextDate = params.date2 != null ? DateUtils.addDays(params.date2, 1) : DateUtils.addDays(params.date, 1) ;
			try {
				retrievePlacesByDate(params.date, nextDate, filter, fc);
			} catch (ParseException e) {
				LOGGER.error("Incorrect 'date' format", e);
			}
		}
		return fc;
	}

	
	@Override
	public int getConcurrentThreadAvailable() {
		// requests could be very long (up to 30 min)
		return 2;
	}
	

	@Override
	public boolean operationAdded(PublicAPIEndpoint<MapCollectionParameters, OprMapCollectionApiResult> api,
			OpOperation op, OpBlock block) {
		boolean changed = false;
		if (op.getType().equals(OPR_PLACE)) {
			for (MapCollectionParameters p : api.getCacheKeys()) {
				CacheHolder<OprMapCollectionApiResult> holder = api.getCacheHolder(p);
				if (holder != null && holder.value != null) {
					OprMapCollectionApiResult r = holder.value;
					addAlreadyReviewedPlaces(RequestFilter.valueOf(p.requestFilter), op, r);
				}
			}
		}
		return changed;
	}
	
	public void retrievePlacesByDate(Date date, Date nextDate, RequestFilter filter, OprMapCollectionApiResult res) throws ParseException {
		List<OpBlock> listBlocks = blocksManager.getBlockchain().getBlockHeaders(-1);
		List<OpBlock> blocksByDate = new LinkedList<>();
		LOGGER.info(String.format("Get history started %s %s - %d blocks...", date, nextDate, blocksByDate.size()));
		for (OpBlock block : listBlocks) {
			Date blockDate = OpBlock.dateFormat.parse(block.getDateString());
			if (date.getTime() <= blockDate.getTime() && blockDate.getTime() <= nextDate.getTime()) {
				blocksByDate.add(block);
			} else if (blockDate.getTime() >= nextDate.getTime()) {
				if (SKIP_INADVANCE_REV_CLOSED_PLACES_REPORT && filter == RequestFilter.REVIEW_CLOSED_PLACES) {
					OpBlock fullBlock = blocksManager.getBlockchain().getFullBlockByRawHash(block.getRawHash());
					List<OpOperation> opOperations = fullBlock.getOperations();
					for (OpOperation opOperation : opOperations) {
						addAlreadyReviewedPlaces(filter, opOperation, res);
					}
				}
			}
		}
		Set<String> placeIdsAdded = new TreeSet<>();
		Map<String, List<Feature>> createdObjectsByTile = new TreeMap<>();
		Map<String, List<Feature>> deletedObjectsByTile = new TreeMap<>();
		
		
		for (OpBlock block : blocksByDate) {
			OpBlockChain blc = blocksManager.getBlockchain();
			OpBlock fullBlock = blc.getFullBlockByRawHash(block.getRawHash());
			List<OpOperation> opOperations = fullBlock.getOperations();
			LOGGER.info(String.format("Get history %s - %s block %d (%d)...", date, nextDate, block.getBlockId(), opOperations.size()));
			for (OpOperation opOperation : opOperations) {
				if (opOperation.getType().equals(OPR_PLACE)) {
					filterObjects(filter, opOperation, placeIdsAdded, createdObjectsByTile, deletedObjectsByTile, res);
				}
			}
		}

		combineGeoJsonResults(filter, res, createdObjectsByTile, deletedObjectsByTile);
		addDataReportPlaces(date, res);
		LOGGER.info(String.format("Get history %s - %s finished.", date, nextDate));
	}

	private void addDataReportPlaces(Date date, OprMapCollectionApiResult res) {
		if (date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate().getMonthValue() <= LocalDate.now().getMonthValue() - 1) {
			List<Feature> features = res.geo.features();
			Map<String, List<Feature>> fDataReport = new TreeMap<>();
			List<List<Feature>> mergeGroups = getMergeGroups(features);
			for (List<Feature> mergeGroup : mergeGroups) {
				for (Feature f : mergeGroup) {
					List<Feature> closedPlaces = new ArrayList<>();
					List<Feature> groupPlacesToMerge = new ArrayList<>();
					if (res.alreadyReviewedPlaceIds.contains(getOprGenId(f))) {
						continue;
					}
					if (f.properties().containsKey(F_DELETED_OSM)) {
						closedPlaces.add(f);
					} else {
						groupPlacesToMerge.add(f);
					}

					if (groupPlacesToMerge.isEmpty()) {
						for (Feature delF : closedPlaces) {
							OprMapCollectionApiResult resDataReport = getDataReport(getTileIdByFeature(delF), dataManager);
							Point currentPoint = (Point) delF.geometry();
							if (resDataReport != null && resDataReport.geo.features() != null) {
								for (Feature feature : resDataReport.geo.features()) {
									Point dataPoint = (Point) delF.geometry();
									if (!feature.properties().containsKey(PLACE_DELETED)
											&& !feature.properties().containsKey(PLACE_DELETED_OSM)
											&& getDistance(currentPoint.lat(), currentPoint.lon(), dataPoint.lat(), dataPoint.lon()) <= 150
											&& hasSimilarNameByFeatures(feature, delF)) {
										addObject(fDataReport, getCurrentObject(feature, blocksManager), OBJ_EDITED, COLOR_GREEN);
									}
								}
							}
						}
					}
				}
			}

			if (!fDataReport.isEmpty()) {
				Set<String> tiles = fDataReport.keySet();
				for (String tileId : tiles) {
					List<Feature> fList = fDataReport.get(tileId);
					addMergedPlaces(res.geo.features(), fList);
				}
			}
		}
	}

	private void combineGeoJsonResults(RequestFilter filter, OprMapCollectionApiResult res,
			Map<String, List<Feature>> createdObjectsByTile, Map<String, List<Feature>> deletedObjectsByTile) {
		if (filter == RequestFilter.REVIEW_CLOSED_PLACES) {
			Set<String> tiles = createdObjectsByTile.keySet();
			for (String tileId : tiles) {
				List<Feature> delList = deletedObjectsByTile.get(tileId);
				List<Feature> cList = createdObjectsByTile.get(tileId);
				if (delList == null) {
					continue;
				}
				// make linked list for quick delete in the middle
				LinkedList<Feature> deletedPoints = new LinkedList<>(delList);
				LinkedList<Feature> createdPoints = new LinkedList<>(cList == null ? Collections.emptyList() : cList);
				while (!deletedPoints.isEmpty()) {
					List<Feature> merged = new ArrayList<>();
					Feature fdel = deletedPoints.poll();
					Point pdel = (Point) fdel.geometry();
					findNearestPointAndDelete(createdPoints, merged, pdel);
					// group id could be set later
					merged.add(0, fdel);
					// find other deleted points within distance of 150m
					findNearestPointAndDelete(deletedPoints, merged, pdel);
					// ! always make sure that groups are following [deleted, deleted, ..., deleted, new, ..., new] - new could not be empty
					// probably later we could have new list empty
					addMergedPlaces(res.geo.features(), merged);
				}
			}
		} else if (filter == RequestFilter.REVIEW_IMAGES) {
			Set<String> tiles = createdObjectsByTile.keySet();
			for (String tileId : tiles) {
				List<Feature> cList = createdObjectsByTile.get(tileId);
				res.geo.features().addAll(cList);
			}
		}
	}



	private void filterObjects(RequestFilter filter, OpOperation opOperation,
			Set<String> placeIdsAdded, Map<String, List<Feature>> createdObjects, Map<String, List<Feature>> deletedObjects, OprMapCollectionApiResult res) {

		for (OpObject opObject : opOperation.getCreated()) {
			if (filter == RequestFilter.REVIEW_CLOSED_PLACES) {
				// add any place as a potential merge (the data could be outdated and will be checked later)
				addObject(createdObjects, opObject, OBJ_CREATED, COLOR_GREEN);
			}
		}
		for (List<String> opObject : opOperation.getDeleted()) {
			if (filter == RequestFilter.REVIEW_CLOSED_PLACES) {
				res.alreadyReviewedPlaceIds.add(generateStringId(opObject));
			}
		}
		
		for (OpObject opObject : opOperation.getEdited()) {
			Map<String, Object> change = opObject.getStringObjMap(F_CHANGE);
			// skip already reviewed place ids
			if (res.alreadyReviewedPlaceIds.contains(generateStringId(opObject))) {
				// only possible if there we collected in before loop
				if (!SKIP_INADVANCE_REV_CLOSED_PLACES_REPORT) {
					throw new IllegalStateException();
				}
				continue;
			}
			changeKeys: for (String changeKey : change.keySet()) {
				if (filter == RequestFilter.REVIEW_IMAGES) {
					if (changeKey.startsWith(F_IMG_REVIEW)) {
						OpObject nObj = blocksManager.getBlockchain().getObjectByName(OPR_PLACE, opObject.getId());
						boolean newObject = placeIdsAdded.add(generateStringId(opObject.getId()));
						if (nObj != null && newObject) {
							addObject(createdObjects, nObj, OBJ_CREATED, COLOR_GREEN);
						}
						break changeKeys;
					}	
				} else if (filter == RequestFilter.REVIEW_CLOSED_PLACES) {
					// osm places is deleted
					// "source.osm[0]": "delete"
	                // "source.osm[0].deleted": {
		            // 		"set": "2021-02-08T17:18:40.393+0000"
		            // }

					int ind = getOsmSourceIndexDeleted(changeKey);
					if (ind != -1) {
						OpObject nObj = blocksManager.getBlockchain().getObjectByName(OPR_PLACE, opObject.getId());
						if (nObj != null) {
							Map<String, Object> osm = getMainOsmFromList(nObj);
							boolean allOsmRefsDeleted = osm != null && osm.containsKey(PlaceOpObjectHelper.F_DELETED_OSM);
							// double check place is not reviewed yet
							boolean newObject = placeIdsAdded.add(generateStringId(opObject.getId()));
							if (osm != null && allOsmRefsDeleted && nObj.getField(null, F_DELETED_PLACE) == null && newObject) {
								addObject(deletedObjects, nObj, OBJ_REMOVED, COLOR_RED);
								break changeKeys;
							}
						}
					}
				} else {
					throw new UnsupportedOperationException();
				}
			}				
		}

	}


	@SuppressWarnings("unchecked")
	private void addAlreadyReviewedPlaces(RequestFilter filter, OpOperation opOperation, OprMapCollectionApiResult res) {
		// place was merged / deleted
		for (List<String> opObject : opOperation.getDeleted()) {
			res.alreadyReviewedPlaceIds.add(generateStringId(opObject));
		}
		for (OpObject opObject : opOperation.getEdited()) {
			Map<String, Object> change = opObject.getStringObjMap(F_CHANGE);
			changeKeys:
			for (var changeKey : change.keySet()) {
				if (filter == RequestFilter.REVIEW_CLOSED_PLACES) {
					//
					// "deleted": {
					// 		"set": "2021-02-08T17:18:40.393+0000"
					// }
					// place was permanently closed
					if (changeKey.equals(PlaceOpObjectHelper.F_DELETED_PLACE)) {
						res.alreadyReviewedPlaceIds.add(generateStringId(opObject));
						break changeKeys;
					}
					// place was merged / deleted
					//	"change": {
					//	"source.osm": {
					//		"append": {
					if (changeKey.equals("source.osm")) {
						Map<String, ?> changeFields = ((Map<String, ?>) change.get(changeKey));
						if (changeFields.containsKey("append") || changeFields.containsKey("appendmany")) {
							res.alreadyReviewedPlaceIds.add(generateStringId(opObject));
							break changeKeys;
						}
					}
				} else if (filter == RequestFilter.REVIEW_IMAGES) {
					if (changeKey.contains("images")) {
						OpObject nObj = blocksManager.getBlockchain().getObjectByName(OPR_PLACE, opObject.getId());
						Object imgReview = nObj.getFieldByExpr(F_IMG_REVIEW);
						if (imgReview != null && ((List<?>) imgReview).isEmpty()) {
							res.alreadyReviewedPlaceIds.add(generateStringId(opObject));
						}
						break changeKeys;
					}
				} else {
					throw new UnsupportedOperationException();
				}
			}
		}
	}


	private void addMergedPlaces(List<Feature> features, List<Feature> newMergedGroup) {
		Point mPoint = (Point) newMergedGroup.get(0).geometry();
		int start = -1;
		boolean groupFound = false;
		for (int i = 0; i < features.size(); i++) {
			if (!groupFound) {
				Point currentPoint = (Point) features.get(i).geometry();
				if (OsmMapUtils.getDistance(currentPoint.lat(), currentPoint.lon(), mPoint.lat(), mPoint.lon()) < 150
						&& features.get(i).properties().containsKey(F_DELETED_PLACE)) {
					groupFound = true;
					start = i;
				}
			} else {
				if (!features.get(i).properties().containsKey(F_DELETED_PLACE)) {
					List<Feature> deletedFeatures = new ArrayList<>();
					List<Feature> createdFeatures = new ArrayList<>();
					for (Feature mf : newMergedGroup) {
						if (mf.properties().containsKey(F_DELETED_PLACE)) {
							deletedFeatures.add(mf);
						} else {
							createdFeatures.add(mf);
						}
					}
					features.addAll(start, deletedFeatures);
					features.addAll(i + deletedFeatures.size(), createdFeatures);
					break;
				}
			}
		}
		if (!groupFound) {
			features.addAll(newMergedGroup);
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
	
	private void addObject(Map<String, List<Feature>> objects, OpObject opObject,
			String status, String color) {
		Map<String, Object> osm = getMainOsmFromList(opObject);
		if (osm != null) {
			ImmutableMap.Builder<String, JsonElement> bld = ImmutableMap.builder();
			bld.put(TITLE, new JsonPrimitive(status + " " + getTitle(osm)));
			bld.put(COLOR, new JsonPrimitive(color));
			bld.put(PLACE_TYPE, new JsonPrimitive((String) osm.get(PlaceOpObjectHelper.F_OSM_VALUE)));
			Object deletedPlace = opObject.getField(null, F_DELETED_PLACE);
			if (deletedPlace != null) {
				bld.put(PLACE_DELETED, new JsonPrimitive(deletedPlace.toString()));
			}
			Object imgReview = opObject.getFieldByExpr(F_IMG_REVIEW);
			if (imgReview != null) {
				bld.put(IMG_REVIEW_SIZE, new JsonPrimitive(String.valueOf(((List<?>) imgReview).size())));
			}
			generateFieldsFromOsmSource(osm, bld);
			bld.put(OPR_ID, new JsonPrimitive(generateStringId(opObject)));
			Feature f = new Feature(generatePoint(osm), bld.build(), Optional.absent());
			add(objects, opObject.getId().get(0), f);
		}
	}



	private void add(Map<String, List<Feature>> objects, String tileId, Feature f) {
		if (!objects.containsKey(tileId)) {
			objects.put(tileId, new ArrayList<>());
		}
		objects.get(tileId).add(f);
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

	private String generateStringId(OpObject opObject) {
		return generateStringId(opObject.getId());
	}
	
	private String generateStringId(List<String> list) {
		return list.get(0) + "," + list.get(1);
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
