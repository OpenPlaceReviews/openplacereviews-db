package org.openplacereviews.osm.service;

import static org.openplacereviews.api.BaseOprPlaceDataProvider.OPR_ID;
import static org.openplacereviews.api.OprHistoryChangesProvider.OPR_PLACE;
import static org.openplacereviews.api.OprHistoryChangesProvider.OSM_ID;
import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_DELETED_OSM;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_DELETED_PLACE;

import java.text.Collator;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

import com.github.filosganga.geogson.model.Point;
import org.apache.catalina.util.ParameterMap;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.openplacereviews.api.OprHistoryChangesProvider;
import org.openplacereviews.api.OprMapCollectionApiResult;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.PublicDataManager;
import org.openplacereviews.opendb.service.bots.GenericMultiThreadBot;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.openplacereviews.osm.model.LatLon;
import org.openplacereviews.osm.model.OsmMapUtils;
import org.openplacereviews.osm.util.PlaceOpObjectHelper;
import org.springframework.beans.factory.annotation.Autowired;

import com.github.filosganga.geogson.model.Feature;

public class MergePlaceBot extends GenericMultiThreadBot<MergePlaceBot> {

    private static final int SIMILAR_PLACE_DISTANCE = 100;
    private static final String IMAGES = "images";
    private static final String SOURCE = "source";
    private static final String SET = "set";
    private static final String APPEND = "append";
    private static final String APPEND_MANY = "appendmany";
    private static final String PLACE_NAME = "name";
    private static final String WIKIDATA = "wikidata";
    private static final String WEBSITE = "website";
    private static final String REQUEST_FILTER = OprHistoryChangesProvider.RequestFilter.REVIEW_CLOSED_PLACES.name();
    private static final String START_DATE = "date";
    private static final String END_DATE = "date2";
	private static final String TILE_ID = "tileid";
    private static final String FILTER = "requestFilter";
    private static final String HISTORY = "history";
	private static final String GEO = "geo";
    private static final String ALL_PUNCTUATION = "^\\p{Punct}+|\\p{Punct}+$";
    private static final String SPACE = " ";
    private static final String COMMA = ",";
    private static final String OLD_NAME = "old_name";
    
    public static final int MONTHS_TO_CHECK = 6;
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
    }
    
    protected enum MatchType {
    	NAME_MATCH(true),
    	OTHER_TAGS_MATCH(true),
    	OTHER_NAME_MATCH(true),
    	EMPTY_NAME_MATCH(false);
    	
    	public final boolean allow2PlacesMerge;

		private MatchType(boolean allow2PlacesMerge) {
			this.allow2PlacesMerge = allow2PlacesMerge;
    	}
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
            Map<String, String[]> params = new ParameterMap<>();
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
				mergePlaces(res, info);
				int cnt = addOperations(info.deleted, info.edited, info.closed);
				info(String.format("Merge places has finished for %s - %s: place groups %d, closed places %d, found similar places %d, merged %d, operations %d", 
						start.toString(), end.toString(), info.mergedGroupSize, info.closedPlaces, info.similarPlacesCnt, info.mergedPlacesCnt, cnt));
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

	protected void mergePlaces(OprMapCollectionApiResult res, MergeInfo info) {
		List<List<Feature>> mergeGroups = getMergeGroups(res.geo.features());
		info.mergedGroupSize = mergeGroups.size();
		List<OpObject> closedPlaces = new ArrayList<>();
		List<OpObject> groupPlacesToMerge = new ArrayList<>();
		for (List<Feature> mergeGroup : mergeGroups) {
			groupPlacesToMerge.clear();
			closedPlaces.clear();
			for (Feature f : mergeGroup) {
				// skip already reviewed
				if (res.alreadyReviewedPlaceIds.contains(getOprGenId(f))) {
					continue;
				}
				OpObject obj = getCurrentObject(f);
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
				if (deleted == null || placesToMerge.isEmpty()) {
					if (closeDeletedPlace(deleted, info.closed)) {
						info.mergedPlacesCnt++;
					}
					continue;
				}
				info.similarPlacesCnt++;
				EnumSet<MatchType> es = closedPlaces.size() == 1 ? EnumSet.allOf(MatchType.class)
						: EnumSet.range(MatchType.NAME_MATCH, MatchType.OTHER_NAME_MATCH);
				OpObject deletedMerged = mergePlaces(es, deleted, placesToMerge, info.deleted, info.edited);
				if (deletedMerged != null) {
					groupPlacesToMerge.remove(deletedMerged);
					info.mergedPlacesCnt++;
				}
			}

		}
	}

	private boolean closeDeletedPlace(OpObject deleted, List<OpObject> edited) {
		if (wasDeletedMoreThanTenDaysAgo(deleted)) {
			OprMapCollectionApiResult res = getDataReport(deleted);
			if (res != null && res.geo.features() != null) {
				List<Feature> features = res.geo.features();
				List<Feature> placesToMerge = getNoDeletedPlaces(features);
				placesToMerge = filterByDistance(placesToMerge, deleted);
				placesToMerge = filterByOsmId(placesToMerge, deleted);
				if (placesToMerge.isEmpty() || !foundPlaceWithSameName(placesToMerge, deleted)) {
					return permanentlyClosePlace(deleted, edited);
				}
			}
		}
		return false;
	}

	private boolean permanentlyClosePlace(OpObject oldObj, List<OpObject> edited) {
		OpObject newObj = new OpObject(oldObj, false);
		newObj.setFieldByExpr(F_DELETED_PLACE, Instant.now().toString());
		addObjToOperation(oldObj, newObj, null, edited);
		return true;
	}

	private List<Feature> filterByDistance(List<Feature> placesToMerge, OpObject deleted) {
		LatLon latLonDeleted = getLatLon(deleted);
		List<Feature> res = new ArrayList<>();
		if (latLonDeleted != null) {
			for (Feature feature : placesToMerge) {
				Point featurePoint = (Point) feature.geometry();
				if (OsmMapUtils.getDistance(latLonDeleted.getLatitude(), latLonDeleted.getLongitude(), featurePoint.lat(), featurePoint.lon()) < 150) {
					res.add(feature);
				}
			}
		}
		return res;
	}

	private List<Feature> filterByOsmId(List<Feature> placesToMerge, OpObject deleted) {
		String osmIdDeleted = getOsmId(deleted);
		List<Feature> res = new ArrayList<>();
		if (osmIdDeleted != null) {
			for (Feature feature : placesToMerge) {
				if (!feature.properties().get(OSM_ID).toString().equals(osmIdDeleted)) {
					res.add(feature);
				}
			}
		}
		return res;
	}

	private boolean foundPlaceWithSameName(List<Feature> placesToMerge, OpObject deleted) {
		List<Map<String, String>> placesToMergeTags = new ArrayList<>();
		Map<String, String> oldOsmTags = getMainOsmTags(deleted);
		if (oldOsmTags != null) {
			List<OpObject> objToMerge = new ArrayList<>();
			for (Feature feature : placesToMerge) {
				objToMerge.add(getCurrentObject(feature));
			}
			for (int i = 0; i < placesToMerge.size(); i++) {
				placesToMergeTags.add(getMainOsmTags(objToMerge.get(i)));
			}
			for (MatchType mt : EnumSet.allOf(MatchType.class)) {
				int matched = -1;
				for (int i = 0; i < placesToMerge.size(); i++) {
					if (match(mt, oldOsmTags, placesToMergeTags.get(i))) {
						if (matched >= 0) {
							if (mt.allow2PlacesMerge) {
								matched = i;
							} else {
								return false;
							}
						} else {
							matched = i;
						}
					}
				}
				if (matched >= 0) {
					return true;
				}
			}
		}
		return false;
	}

	private List<Feature> getNoDeletedPlaces(List<Feature> features) {
		List<Feature> res = new ArrayList<>();
		for (Feature feature : features) {
			if (!feature.properties().containsKey(F_DELETED_PLACE)) {
				Map<String, Object> osm = getMainOsmFromList(getCurrentObject(feature));
				if (osm != null && !osm.containsKey(F_DELETED_OSM)) {
					res.add(feature);
				}
			}
		}
		return res;
	}

	private LocalDate transformStringDateToLocaleDate(String date) {
		return LocalDate.parse(date.substring(0, date.indexOf("+")), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
	}

	private LocalDate getDeletedDate(OpObject deleted) {
		Map<String, Object> mainOsm = getMainOsmFromList(deleted);
		if (mainOsm != null) {
			for (Map.Entry<String, Object> entry : mainOsm.entrySet()) {
				if (entry.getKey().equals(F_DELETED_OSM)) {
					return transformStringDateToLocaleDate((String) entry.getValue());
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

	private OprMapCollectionApiResult getDataReport(OpObject deleted) {
		PublicDataManager.PublicAPIEndpoint<?, ?> apiEndpoint = dataManager.getEndpoint(GEO);
		if (apiEndpoint != null) {
			Map<String, String[]> dataParams = new LinkedHashMap<>();
			dataParams.put(TILE_ID, new String[]{deleted.getId().get(0)});
			return getReport(apiEndpoint, dataParams);
		}
		return null;
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

	private String getOsmId(OpObject o) {
		Map<String, Object> mp = getMainOsmFromList(o);
		if (mp != null && mp.containsKey(OSM_ID)) {
			return (String) mp.get(OSM_ID);
		}
		return null;
	}

	protected OpObject getCurrentObject(Feature newPlace) {
		OpObject obj = blocksManager.getBlockchain().getObjectByName(OPR_PLACE, getPlaceId(newPlace));
		if (obj != null && obj.getField(null, F_DELETED_PLACE) != null) {
			return null;
		}
		return obj;
	}
    
    @Override
    public int progress() {
    	return progress;
    }
    
    @Override
    public int total() {
    	return totalCnt;
    }

	private List<List<Feature>> getMergeGroups(List<Feature> list) {
		List<List<Feature>> mergeGroups = new ArrayList<>();
		if (list == null) {
			return mergeGroups;
		}
		int currentGroupBeginIndex = 0;
		for (int i = 1; i < list.size() - 1; i++) {
			if (isDeleted(list, i) && !isDeleted(list, i - 1)) {
				mergeGroups.add(list.subList(currentGroupBeginIndex, i));
				currentGroupBeginIndex = i;
			}
		}
		mergeGroups.add(list.subList(currentGroupBeginIndex, list.size()));
		return mergeGroups;
	}

    private boolean isDeleted(List<Feature> list, int i) {
        return list.get(i).properties().containsKey(F_DELETED_OSM);
    }

    private int addOperations(List<List<String>> deleted, List<OpObject> edited, List<OpObject> closed) throws FailedVerificationException {
        int batch = 0;
        int cnt = 0;
        OpOperation op = initOpOperation(objectType());
        if (deleted.size() != edited.size()) {
            throw new IllegalStateException();
        }

        for (OpObject object:closed) {
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
        OprMapCollectionApiResult collection = (OprMapCollectionApiResult) apiEndpoint.getContentObject(params);
        return collection;
    }

    protected OpObject mergePlaces(EnumSet<MatchType> matchTypes, OpObject oldObj, List<OpObject> placesToMerge, List<List<String>> deleted, List<OpObject> edited) {
    	List<Map<String, String>> placesToMergeTags = new ArrayList<>();
    	Map<String, String> oldOsmTags = getMainOsmTags(oldObj);
		for (int i = 0; i < placesToMerge.size(); i++) {
			placesToMergeTags.add(getMainOsmTags(placesToMerge.get(i)));
		}
		for (MatchType mt : matchTypes) {
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
    
    protected boolean match(MatchType mt, Map<String, String> oldOsmTags, Map<String, String> newOsmTags) {
    	String oldName = (String) oldOsmTags.get(PLACE_NAME);
    	String newName = (String) newOsmTags.get(PLACE_NAME);
		if (mt == MatchType.OTHER_TAGS_MATCH) {
			if (equalsNotEmptyStringValue(oldOsmTags.get(WIKIDATA), newOsmTags.get(WIKIDATA))) {
				return true;
			}
			if (equalsNotEmptyStringValue(oldOsmTags.get(WEBSITE), newOsmTags.get(WEBSITE))) {
				return true;
			}
		} else if (mt == MatchType.NAME_MATCH) {
    		return checkNames(oldName, newName);
    	} else if(mt == MatchType.OTHER_NAME_MATCH) {
    		List<String> otherNames = getOtherPlaceName(newOsmTags);
    		List<String> otherOldNames = getOtherPlaceName(oldOsmTags);
			for (String name : otherNames) {
				for (String name2 : otherOldNames) {
					if (checkNames(name2, name)) {
						return true;
					}
				}
			}
		} else if (mt == MatchType.EMPTY_NAME_MATCH) {
			// all names null
			if (OUtils.isEmpty(oldName) && OUtils.isEmpty(newName)) {
				return true;
			}
			// if name appeared
			if (OUtils.isEmpty(oldName)) {
				return true;
			}
		}
		return false;
	}

	private boolean equalsNotEmptyStringValue(String s1, String s2) {
		if (OUtils.isEmpty(s1) || OUtils.isEmpty(s2)) {
			return false;
		}
		return OUtils.equalsStringValue(s1, s2);
	}

	@SuppressWarnings("unchecked")
	private Map<String, String> getMainOsmTags(OpObject o) {
		Map<String, Object> m = getMainOsmFromList(o);
		if (m != null) {
			return (Map<String, String>) m.get("tags");
		}
		return null;
	}
	
	private Map<String, Object> getMainOsmFromList(OpObject o) {
		if (o == null) {
			return null;
		}
		List<Map<String, Object>> osmList = o.getField(null, "source", "osm");
		if (osmList == null) {
			return null;
		}
		Map<String, Object> main = null;
		for (Map<String, Object> m : osmList) {
			if (m.containsKey(ATTR_LATITUDE) && m.containsKey(ATTR_LONGITUDE) && m.containsKey(PlaceOpObjectHelper.F_OSM_VALUE)) {
				if (!m.containsKey(F_DELETED_OSM)) {
					return m;
				}
				if (main == null) {
					main = m;
				}
			}
		}
		return main;
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

    private List<String> getOtherPlaceName(Map<String, String> tags) {
        List<String> otherNames = new ArrayList<>();
		for (Map.Entry<String, String> tag : tags.entrySet()) {
			if (tag.getKey().startsWith(PLACE_NAME) || tag.getKey().equals(OLD_NAME)) {
				otherNames.add((String) tag.getValue());
			}
		}
        return otherNames;
    }

    private boolean checkNames(String oldName, String newName) {
    	// empty name not equal
		if (OUtils.isEmpty(oldName) || OUtils.isEmpty(newName)) {
			return false;
		}
        Collator collator = Collator.getInstance();
        collator.setStrength(Collator.PRIMARY);

        String oldNameLower = oldName.toLowerCase();
        String newNameLower = newName.toLowerCase();
        //if names equal
        if (collator.compare(oldNameLower, newNameLower) == 0) {
            return true;
        }

        if (oldNameLower.replaceAll("\\s+", "")
                .equals(newNameLower.replaceAll("\\s+", ""))) {
            return true;
        }

        List<String> oldNameList = getWords(oldNameLower);
        List<String> newNameList = getWords(newNameLower);
        Collections.sort(oldNameList);
        Collections.sort(newNameList);
        //if names have the same words in different order
        //or one of the names is part of another name
        return oldNameList.equals(newNameList)
                || isSubCollection(newNameList, oldNameList, collator)
                || isSubCollection(oldNameList, newNameList, collator);
    }

    private List<String> getWords(String name) {
        List<String> wordList = new ArrayList<>();
        for (String word : name.split(SPACE)) {
            String res = word.trim().replaceAll(ALL_PUNCTUATION, "");
            if (!res.equals("")) {
                wordList.add(res);
            }
        }
        return wordList;
    }


    private boolean isSubCollection(List<String> mainList, List<String> subList, Collator collator) {
        int matchedCount = 0;
        for (String wordMain : mainList) {
            for (String wordSub : subList) {
                if (collator.compare(wordMain, wordSub) == 0
                        || new LevenshteinDistance().apply(wordMain, wordSub) <= getMaxLevenshteinDistance(wordMain, wordSub)) {
                    matchedCount++;
                    if (matchedCount == subList.size()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private int getMaxLevenshteinDistance(String wordMain, String wordSub) {
        int wordMainLength = wordMain.length();
        int wordSubLength = wordSub.length();
        int resLength = Math.min(wordMainLength, wordSubLength);
        if (resLength < 3) {
            return 1;
        }
        if (resLength <= 5) {
            return 2;
        }
        if (resLength <= 7) {
            return 3;
        }
        return 4;
    }
}
