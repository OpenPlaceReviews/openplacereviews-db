package org.openplacereviews.osm.service;

import static org.openplacereviews.api.BaseOprPlaceDataProvider.OPR_ID;
import static org.openplacereviews.api.OprHistoryChangesProvider.OPR_PLACE;
import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;
import static org.openplacereviews.osm.util.MergeUtil.*;
import static org.openplacereviews.osm.util.MergeUtil.MatchType.NAME_MATCH;
import static org.openplacereviews.osm.util.MergeUtil.MatchType.OTHER_NAME_MATCH;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_DELETED_OSM;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_DELETED_PLACE;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

import com.github.filosganga.geogson.model.Point;


import org.openplacereviews.api.OprHistoryChangesProvider;
import org.openplacereviews.api.OprMapCollectionApiResult;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.PublicDataManager;
import org.openplacereviews.opendb.service.bots.GenericMultiThreadBot;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.openplacereviews.osm.model.LatLon;
import org.openplacereviews.osm.model.OsmMapUtils;
import org.openplacereviews.osm.util.MergeUtil;
import org.springframework.beans.factory.annotation.Autowired;

import com.github.filosganga.geogson.model.Feature;

public class MergePlaceBot extends GenericMultiThreadBot<MergePlaceBot> {

    private static final int SIMILAR_PLACE_DISTANCE = 100;
    private static final String IMAGES = "images";
    private static final String SOURCE = "source";
    private static final String SET = "set";
    private static final String APPEND = "append";
    private static final String APPEND_MANY = "appendmany";
    private static final String REQUEST_FILTER = OprHistoryChangesProvider.RequestFilter.REVIEW_CLOSED_PLACES.name();
    private static final String START_DATE = "date";
    private static final String END_DATE = "date2";
    private static final String FILTER = "requestFilter";
    private static final String HISTORY = "history";
    private static final String COMMA = ",";
	private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.[SSSZ][SSSSZ]";
    
    public static final int MONTHS_TO_CHECK = 9;
    public boolean TRACE = true;

    @Autowired
    private PublicDataManager dataManager;

    @Autowired
    protected BlocksManager blocksManager;
    
    private int totalCnt = 1;
	private int progress = 0;

    public MergePlaceBot(OpObject botObject) {
        super(botObject);
    }
    
    protected MergePlaceBot() {
    	// test
    	super("0");
	}

    @Override
    public String getTaskDescription() {
        return "Merge places";
    }

    @Override
    public String getTaskName() {
        return "place-merge";
    }

    public String objectType() {
        return OPR_PLACE;
    }
    
    protected static class MergeInfo {
    	List<List<String>> deleted = new ArrayList<>();
		List<OpObject> edited = new ArrayList<>();
		List<OpObject> closed = new ArrayList<>();
		int mergedGroupSize;
		int closedPlaces;
		int similarPlacesCnt;
		int mergedPlacesCnt;
		int closedPlacesCnt;
    }

    @Override
    public MergePlaceBot call() throws Exception {
        addNewBotStat();
        try {
        	initVars();
            info("Merge places has started: " + new Date());
            PublicDataManager.PublicAPIEndpoint<?, ?> apiEndpoint = dataManager.getEndpoint(HISTORY);
            if (apiEndpoint == null) {
            	setFailedState();
                info("Merge has failed: no history provider");
                return this;
            }
            progress = 1;
            totalCnt = MONTHS_TO_CHECK + 1;
            Map<String, String[]> params = new TreeMap<>();
			for (int i = 0; i < MONTHS_TO_CHECK; i++) {
				MergeInfo info = new MergeInfo();
				LocalDate dt = LocalDate.now().minusMonths(i);
				LocalDate start = LocalDate.of(dt.getYear(), dt.getMonth(), 1);
				LocalDate dts = LocalDate.now().minusMonths(i - 1);
				LocalDate end = LocalDate.of(dts.getYear(), dts.getMonth(), 1).minusDays(1);
				params.put(START_DATE, new String[] { start.toString() });
				params.put(END_DATE, new String[] { end.toString() });
				params.put(FILTER, new String[] { REQUEST_FILTER });
				OprMapCollectionApiResult res = getReport(apiEndpoint, params);
				mergeAndClosePlaces(res, info);
				int cnt = addOperations(info.deleted, info.edited, info.closed);
				info(String.format(
						"Merge places has finished for %s - %s: place groups %d, closed places %d, found similar places %d - merged %d, perm closed %d operations %d", 
						start, end, info.mergedGroupSize, info.closedPlaces, info.similarPlacesCnt, info.mergedPlacesCnt, info.closedPlacesCnt, cnt));
				progress++;
			}
			info("Merge places completed: " + new Date());
            setSuccessState();
        } catch (Exception e) {
            setFailedState();
            info("Merge has failed: " + e.getMessage(), e);
            throw e;
        } finally {
            super.shutdown();
        }
        return this;
    }

	protected void mergeAndClosePlaces(OprMapCollectionApiResult res, MergeInfo info) {
		List<List<Feature>> mergeGroups = getMergeGroups(res.geo.features());
		info.mergedGroupSize = mergeGroups.size();
		List<OpObject> closedPlaces = new ArrayList<>();
		List<OpObject> groupPlacesToMerge = new ArrayList<>();
		Set<String> closedPlacesSet = new TreeSet<>();
		for (List<Feature> mergeGroup : mergeGroups) {
			groupPlacesToMerge.clear();
			closedPlaces.clear();
			for (Feature f : mergeGroup) {
				// skip already reviewed
				if (res.alreadyReviewedPlaceIds.contains(getOprGenId(f))) {
					continue;
				}
				OpObject obj = getCurrentObject(f, blocksManager);
				Map<String, Object> mainOsm = getMainOsmFromList(obj);
				if (mainOsm != null) {
					boolean delOsm = mainOsm.containsKey(F_DELETED_OSM);
					if (delOsm) {
						addObj(closedPlaces, obj);
					} else {
						addObj(groupPlacesToMerge, obj);
					}
				} 
			}
			for (OpObject deleted : closedPlaces) {
				try {
					LatLon l = getLatLon(deleted);
					info.closedPlaces++;
					List<OpObject> placesToMerge = new ArrayList<>(groupPlacesToMerge);
					Iterator<OpObject> it = placesToMerge.iterator();
					while (it.hasNext()) {
						OpObject o = it.next();
						LatLon l2 = getLatLon(o);
						if (l2 == null || OsmMapUtils.getDistance(l, l2) > SIMILAR_PLACE_DISTANCE) {
							it.remove();
						}

					}
					if (placesToMerge.isEmpty()) {
						if (wasDeletedMoreThanTenDaysAgo(deleted)
								&& (closedPlacesSet.isEmpty() || !closedPlacesSet.contains(deleted.getId().toString()))) {
							if (closeDeletedPlace(deleted, info.closed)) {
								info.closedPlacesCnt++;
								closedPlacesSet.add(deleted.getId().toString());
							}
						}
					} else {
						info.similarPlacesCnt++;
						EnumSet<MergeUtil.MatchType> es = closedPlaces.size() == 1 ? EnumSet.allOf(MergeUtil.MatchType.class)
								: EnumSet.range(NAME_MATCH, OTHER_NAME_MATCH);
						OpObject deletedMerged = mergePlaces(es, deleted, placesToMerge, info.deleted, info.edited);
						if (deletedMerged != null) {
							groupPlacesToMerge.remove(deletedMerged);
							info.mergedPlacesCnt++;
						}
					}
				} catch (RuntimeException e) {
					info(String.format(
							"Error processing deleted object '%s': %s", deleted.getId().toString(), e.getMessage()));
					throw e;
				}
			}

		}
	}

	private boolean closeDeletedPlace(OpObject deleted, List<OpObject> edited) {
		OprMapCollectionApiResult res = getDataReport(deleted.getId().get(0), dataManager);
		if (res != null && res.geo.features() != null && !hasSimilarCloseByActivePlaces(res.geo.features(), deleted)) {
			if (TRACE) {
				info(String.format("Close %s - %s", deleted.getId(), getMainOsmFromList(deleted)));
			}
			OpObject newObj = new OpObject(deleted, false);
			newObj.setFieldByExpr(F_DELETED_PLACE, Instant.now().toString());
			addObjToOperation(deleted, newObj, null, edited);
			return true;
		}
		return false;
	}


	private boolean hasSimilarCloseByActivePlaces(List<Feature> features, OpObject deleted) {
		LatLon latLonDeleted = getLatLon(deleted);
		if (latLonDeleted != null) {
			for (Feature feature : features) {
				if (isNonDeleted(feature) && getDistance(latLonDeleted, feature) <= SIMILAR_PLACE_DISTANCE
						&& hasSimilarName(feature, deleted)) {
					return true;
				}
			}
		}
		return false;
	}

	private boolean isNonDeleted(Feature feature) {
		if (!feature.properties().containsKey(F_DELETED_PLACE)) {
			Map<String, Object> osm = getMainOsmFromList(getCurrentObject(feature, blocksManager));
			return osm != null && !osm.containsKey(F_DELETED_OSM);
		}
		return false;
	}

	private double getDistance(LatLon latLonDeleted, Feature feature) {
		Point featurePoint = (Point) feature.geometry();
		return OsmMapUtils.getDistance(latLonDeleted.getLatitude(), latLonDeleted.getLongitude(),
				featurePoint.lat(), featurePoint.lon());
	}

	private boolean hasSimilarName(Feature feature, OpObject deleted) {
		Map<String, String> oldOsmTags = getMainOsmTags(deleted);
		OpObject objToMerge = getCurrentObject(feature, blocksManager);
		Map<String, String> placeToMergeTags = getMainOsmTags(objToMerge);
		if (oldOsmTags != null && placeToMergeTags != null) {
			for (MatchType mt : EnumSet.allOf(MatchType.class)) {
				if (match(mt, oldOsmTags, placeToMergeTags)) {
					return true;
				}
			}
		}
		return false;
	}

	private LocalDate getDeletedDate(OpObject deleted) {
		Map<String, Object> mainOsm = getMainOsmFromList(deleted);
		if (mainOsm != null) {
			for (Map.Entry<String, Object> entry : mainOsm.entrySet()) {
				if (entry.getKey().equals(F_DELETED_OSM)) {
					String date = (String) entry.getValue();
					return LocalDate.parse(date, DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT));
				}
			}
		}
		return null;
	}

	private boolean wasDeletedMoreThanTenDaysAgo(OpObject deleted) {
		LocalDate dateToday = LocalDate.now();
		LocalDate dateDeleted = getDeletedDate(deleted);
		if (dateDeleted != null) {
			return Duration.between(dateDeleted.atStartOfDay(), dateToday.atStartOfDay()).toDays() >= 10;
		} else {
			return false;
		}
	}
	
	private void addObj(List<OpObject> ar, OpObject obj) {
		for (OpObject o : ar) {
			if (o.getId().equals(obj.getId())) {
				// duplicate
				return;
			}
		}
		ar.add(obj);
	}

	private LatLon getLatLon(OpObject o) {
		Map<String, Object> mp = getMainOsmFromList(o);
		if(mp != null && mp.containsKey(ATTR_LATITUDE) && mp.containsKey(ATTR_LONGITUDE)) {
			return new LatLon((double) mp.get(ATTR_LATITUDE), (double) mp.get(ATTR_LONGITUDE));
		}
		return null;
	}

    @Override
    public int progress() {
    	return progress;
    }
    
    @Override
    public int total() {
    	return totalCnt;
    }

    private int addOperations(List<List<String>> deleted, List<OpObject> edited, List<OpObject> closed) throws FailedVerificationException {
        int batch = 0;
        int cnt = 0;
        OpOperation op = initOpOperation(objectType());
        if (deleted.size() != edited.size()) {
            throw new IllegalStateException();
        }

	    for (OpObject object : closed) {
		    if (batch >= placesPerOperation) {
			    batch = 0;
			    addOpIfNeeded(op, true);
			    op = initOpOperation(objectType());
			    cnt++;
		    }
		    op.addEdited(object);
		    batch++;
	    }

        for (int i = 0; i < deleted.size() && i < edited.size(); i++) {
            if (batch >= placesPerOperation) {
                batch = 0;
                addOpIfNeeded(op, true);
                op = initOpOperation(objectType());
                cnt++;
            }
            op.addDeleted(deleted.get(i));
            op.addEdited(edited.get(i));
            batch++;
        }
        if (batch > 0) {
        	addOpIfNeeded(op, true);
        	cnt++;
        }
        return cnt;
    }

    private OprMapCollectionApiResult getReport(PublicDataManager.PublicAPIEndpoint<?, ?> apiEndpoint, Map<String, String[]> params) {
        return (OprMapCollectionApiResult) apiEndpoint.getContentObject(params);
    }

    protected OpObject mergePlaces(EnumSet<MergeUtil.MatchType> matchTypes, OpObject oldObj, List<OpObject> placesToMerge, List<List<String>> deleted, List<OpObject> edited) {
    	List<Map<String, String>> placesToMergeTags = new ArrayList<>();
    	Map<String, String> oldOsmTags = getMainOsmTags(oldObj);
		for (int i = 0; i < placesToMerge.size(); i++) {
			placesToMergeTags.add(getMainOsmTags(placesToMerge.get(i)));
		}
		for (MergeUtil.MatchType mt : matchTypes) {
			int matched = -1;
			for (int i = 0; i < placesToMerge.size(); i++) {
				if (match(mt, oldOsmTags, placesToMergeTags.get(i))) {
					if (matched >= 0) {
						// 2 places match
						if (mt.allow2PlacesMerge) {
							matched = i;
						} else {
							// System.out.println("Not merge (2>): " + oldOsmTags + " != " + placesToMergeTags);
							return null;
						}
					} else {
						matched = i;
					}
				}
			}
			if (matched >= 0) {
				if (TRACE) {
					info(String.format("Merge - %s with %s", oldOsmTags, placesToMergeTags.get(matched)));
				}
				addObjToOperation(oldObj, placesToMerge.get(matched), deleted, edited);
				return placesToMerge.remove(matched);
			}
		}
		// System.out.println("Not merge:" + oldOsmTags + " != " + placesToMergeTags);
		return null;
    }

    //for test
	protected OpObject getCurrentObjectInBot(Feature newPlace) {
		OpObject obj = blocksManager.getBlockchain().getObjectByName(OPR_PLACE, getPlaceId(newPlace));
		if (obj != null && obj.getField(null, F_DELETED_PLACE) != null) {
			return null;
		}
		return obj;
	}

	@SuppressWarnings("unchecked")
	private Map<String, String> getMainOsmTags(OpObject o) {
		Map<String, Object> m = getMainOsmFromList(o);
		if (m != null) {
			return (Map<String, String>) m.get("tags");
		}
		return null;
	}

    protected List<String> getPlaceId(Feature feature) {
        return new ArrayList<>(Arrays.asList(getOprGenId(feature).split(COMMA)));
    }

	private String getOprGenId(Feature feature) {
		return feature.properties().get(OPR_ID).getAsString();
	}

    private void addObjToOperation(OpObject oldObj, OpObject newObj, List<List<String>> deleted, List<OpObject> edited) {
        OpObject editObj = new OpObject();
        editObj.setId(oldObj.getId().get(0), oldObj.getId().get(1));
        TreeMap<String, Object> current = new TreeMap<>();
		TreeMap<String, Object> changed = new TreeMap<>();

		if (deleted == null) {
			permClosed(F_DELETED_PLACE, oldObj, newObj, current, changed);
		} else {
			mergeFields(SOURCE, oldObj, newObj, current, changed);
			mergeFields(IMAGES, oldObj, newObj, current, changed);
		}

        editObj.putObjectValue(OpObject.F_CHANGE, changed);
        editObj.putObjectValue(OpObject.F_CURRENT, current);

		if (deleted != null) {
			deleted.add(newObj.getId());
		}

		edited.add(editObj);
    }

    private void permClosed(String type, OpObject oldObj, OpObject newObj,
							TreeMap<String, Object> current, TreeMap<String, Object> changed) {
		String newField = newObj.getField(null, type);
		String oldField = oldObj.getField(null, type);
		if (newField != null) {
			TreeMap<String, Object> appendObj = new TreeMap<>();
			if (oldField == null) {
				appendObj.put(SET, newField);
			}
			current.put(F_DELETED_PLACE, oldField);
			changed.put(F_DELETED_PLACE, appendObj);
		}
	}

    @SuppressWarnings("unchecked")
	private void mergeFields(String type, OpObject oldObj, OpObject newObj,
                             TreeMap<String, Object> current, TreeMap<String, Object> changed) {
        Map<String, Object> newFields = newObj.getField(null, type);
        Map<String, Object> oldFields = oldObj.getField(null, type);
        if (newFields != null) {
            for (Map.Entry<String, Object> newf : newFields.entrySet()) {
                TreeMap<String, Object> appendObj = new TreeMap<>();
                String category = type + "." + newf.getKey();
                List<Map<String, Object>> newCategoryList = (List<Map<String, Object>>) newf.getValue();
                if(!newCategoryList.isEmpty()) {
                    if (oldFields == null || !oldFields.containsKey(newf.getKey())) {
                        appendObj.put(SET, newCategoryList);
                    } else {
                        if (newCategoryList.size() > 1) {
                            appendObj.put(APPEND_MANY, newCategoryList);
                        } else {
                            appendObj.put(APPEND, newCategoryList.get(0));
                        }
                        current.put(category, oldFields.get(newf.getKey()));
                    }
                    changed.put(category, appendObj);
                }
            }
        }
    }

}
