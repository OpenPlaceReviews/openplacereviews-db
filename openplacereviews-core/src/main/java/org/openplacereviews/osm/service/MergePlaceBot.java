package org.openplacereviews.osm.service;

import static org.openplacereviews.api.BaseOprPlaceDataProvider.OPR_ID;
import static org.openplacereviews.api.BaseOprPlaceDataProvider.PLACE_DELETED;
import static org.openplacereviews.api.BaseOprPlaceDataProvider.TAGS;
import static org.openplacereviews.api.OprHistoryChangesProvider.OPR_PLACE;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_DELETED_OSM;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_OSM;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_SOURCE;

import java.text.Collator;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.catalina.util.ParameterMap;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.openplacereviews.api.OprMapCollectionApiResult;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.PublicDataManager;
import org.openplacereviews.opendb.service.bots.GenericMultiThreadBot;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.openplacereviews.osm.model.OsmMapUtils;
import org.springframework.beans.factory.annotation.Autowired;

import com.github.filosganga.geogson.model.Feature;
import com.github.filosganga.geogson.model.Point;

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
    private static final String PHONE = "phone";
    private static final String DESCRIPTION = "description";
    private static final String POSSIBLE_MERGE = "POSSIBLE_MERGE";
    private static final String START_DATA = "date";
    private static final String END_DATA = "date2";
    private static final String FILTER = "requestFilter";
    private static final String HISTORY = "history";
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
		int mergedGroupSize;
		int similarPlacesCnt;
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
				params.put(START_DATA, new String[] { start.toString() });
				params.put(END_DATA, new String[] { end.toString() });
				params.put(FILTER, new String[] { POSSIBLE_MERGE });
				List<Feature> list = getFeatures(apiEndpoint, params);
				mergePlaces(list, info);
				int cnt = addOperations(info.deleted, info.edited);
				info(String.format("%Merge places has finished for %s - %s: place groups %d, size=2 and close by %d, merged %d, operations %d", 
						start.toString(), end.toString(), info.mergedGroupSize, info.similarPlacesCnt, info.deleted.size(), cnt));
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

	protected void mergePlaces(List<Feature> list, MergeInfo info) {
		List<List<Feature>> mergeGroups = getMergeGroups(list);
		info.mergedGroupSize = mergeGroups.size();
		mergeGroups.removeIf(mergeGroup -> mergeGroup.size() != 2);
		for (List<Feature> mergeGroup : mergeGroups) {
			Feature newPlace = mergeGroup.get(0);
			Feature oldPlace = mergeGroup.get(1);
			if (areNearbyPlaces(newPlace, oldPlace)) {
				info.similarPlacesCnt++;
				OpObject newObj = getCurrentObject(newPlace);
				OpObject oldObj = getCurrentObject(oldPlace);
				mergePlaces(newObj, oldObj, info.deleted, info.edited);
			}
		}
	}

	protected OpObject getCurrentObject(Feature newPlace) {
		return blocksManager.getBlockchain().getObjectByName(OPR_PLACE, getPlaceId(newPlace));
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
			if (!isDeleted(list, i) && isDeleted(list, i - 1)) {
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

    private int addOperations(List<List<String>> deleted, List<OpObject> edited) throws FailedVerificationException {
        int batch = 0;
        int cnt = 0;
        OpOperation op = initOpOperation(objectType());
        if (deleted.size() != edited.size()) {
            throw new IllegalStateException();
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

    private List<Feature> getFeatures(PublicDataManager.PublicAPIEndpoint<?, ?> apiEndpoint, Map<String, String[]> params) {
        OprMapCollectionApiResult collection = (OprMapCollectionApiResult) apiEndpoint.getContentObject(params);
        return collection.geo.features();
    }

    private boolean areNearbyPlaces(Feature f1, Feature f2) {
        if (!f1.properties().containsKey(PLACE_DELETED) && !f2.properties().containsKey(PLACE_DELETED)) {
            Point p1 = (Point) f1.geometry();
            Point p2 = (Point) f1.geometry();
            return OsmMapUtils.getDistance(p1.lat(), p1.lon(), p2.lat(), p2.lon()) < SIMILAR_PLACE_DISTANCE;
        }
        return false;
    }

    protected void mergePlaces(OpObject newObj, OpObject oldObj, List<List<String>> deleted, List<OpObject> edited) {
        if (newObj != null && oldObj != null) {
            List<Map<String, Object>> newOsmList = newObj.getField(null, F_SOURCE, F_OSM);
            List<Map<String, Object>> oldOsmList = oldObj.getField(null, F_SOURCE, F_OSM);
            if (newOsmList != null && oldOsmList != null
                    && (isMergeByName(newOsmList, oldOsmList) || isMergeByTags(newOsmList, oldOsmList))) {
				if (TRACE) {
					info(String.format("Merge - %s with %s", newOsmList, oldOsmList));
				}
                addObjToOperation(oldObj, newObj, deleted, edited);
            }
        }
    }

    protected boolean isMergeByName(List<Map<String, Object>> newOsmList, List<Map<String, Object>> oldOsmList) {
        Map<String, Object> newTags = (Map<String, Object>) newOsmList.get(newOsmList.size() - 1).get(TAGS);
        Map<String, Object> oldTags = (Map<String, Object>) oldOsmList.get(oldOsmList.size() - 1).get(TAGS);
        String oldName = getTag(oldTags, PLACE_NAME);
        String newName = getTag(newTags, PLACE_NAME);

        //all names null
        if (oldName == null && newName == null) {
            return true;
        }
        //if name appeared
        if (oldName == null) {
            return true;
        }

        if (newName != null) {
            if (checkNames(oldName, newName) || checkOtherNames(newTags, oldName)) {
                return true;
            }
        }

        if (newName == null) {
            return checkOtherNames(newTags, oldName);
        }

        return false;
    }

    protected boolean isMergeByTags(List<Map<String, Object>> newOsmList, List<Map<String, Object>> oldOsmList) {
        Map<String, Object> newTags = (Map<String, Object>) newOsmList.get(newOsmList.size() - 1).get(TAGS);
        Map<String, Object> oldTags = (Map<String, Object>) oldOsmList.get(oldOsmList.size() - 1).get(TAGS);

        if (checkTags(getTag(newTags, WIKIDATA), getTag(oldTags, WIKIDATA))) {
            return true;
        }
        if (checkTags(getTag(newTags, WEBSITE), getTag(oldTags, WEBSITE))) {
            return true;
        }
        return false;
    }

    private boolean checkTags(String tag1, String tag2) {
        if (tag1 != null && tag2 != null) {
            return tag1.equals(tag2);
        }
        return false;
    }

    protected List<String> getPlaceId(Feature feature) {
        return new ArrayList<>(Arrays.asList(feature.properties()
                .get(OPR_ID).getAsString()
                .split(COMMA)));
    }

    private void addObjToOperation(OpObject oldObj, OpObject newObj, List<List<String>> deleted, List<OpObject> edited) {
        OpObject editObj = new OpObject();
        editObj.setId(oldObj.getId().get(0), oldObj.getId().get(1));
        TreeMap<String, Object> current = new TreeMap<>();
        TreeMap<String, Object> changed = new TreeMap<>();

        mergeFields(SOURCE, oldObj, newObj, current, changed);
        mergeFields(IMAGES, oldObj, newObj, current, changed);

        editObj.putObjectValue(OpObject.F_CHANGE, changed);
        editObj.putObjectValue(OpObject.F_CURRENT, current);

        deleted.add(newObj.getId());
        edited.add(editObj);
    }

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

    private String getTag(Map<String, Object> tags, String name) {
        if (tags != null && tags.containsKey(name)) {
            return tags.get(name).toString();
        }
        return null;
    }

    private List<String> getOtherPlaceName(Map<String, Object> tags) {
        List<String> otherNames = new ArrayList<>();
        for (Map.Entry<String, Object> tag : tags.entrySet()) {
            if ((tag.getKey().startsWith(PLACE_NAME) || tag.getKey().equals(OLD_NAME)) && !tag.getKey().equals(PLACE_NAME)) {
                otherNames.add((String) tag.getValue());
            }
        }
        return otherNames;
    }

    private boolean checkNames(String oldName, String newName) {
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

    private boolean checkOtherNames(Map<String, Object> newTags, String oldName) {
        List<String> otherNames = getOtherPlaceName(newTags);
        for (String name : otherNames) {
            if (checkNames(oldName, name)) {
                return true;
            }
        }
        return false;
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
