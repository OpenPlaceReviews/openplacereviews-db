package org.openplacereviews.osm.service;

import com.github.filosganga.geogson.model.Feature;
import com.github.filosganga.geogson.model.Point;

import org.apache.catalina.util.ParameterMap;
import org.openplacereviews.api.OprMapCollectionApiResult;
import org.openplacereviews.opendb.ops.OpObject;

import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.PublicDataManager;
import org.openplacereviews.opendb.service.bots.GenericMultiThreadBot;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.openplacereviews.osm.model.OsmMapUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.text.Collator;
import java.time.LocalDate;
import java.util.*;

import static org.openplacereviews.api.BaseOprPlaceDataProvider.*;
import static org.openplacereviews.api.OprHistoryChangesProvider.OPR_PLACE;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.*;

public class MergePlaceBot extends GenericMultiThreadBot<MergePlaceBot> {

    private static final int SIMILAR_PLACE_DISTANCE = 150;
    private static final String SOURCE_OSM = "source.osm";
    private static final String APPEND = "append";
    private static final String APPEND_MANY = "appendMany";
    private static final String PLACE_NAME = "name";
    private static final String POSSIBLE_MERGE = "POSSIBLE_MERGE";
    private static final String START_DATA = "date";
    private static final String END_DATA = "date2";
    private static final String FILTER = "requestFilter";
    private static final String HISTORY = "history";
    private static final String ALL_PUNCTUATION = "^\\p{Punct}+|\\p{Punct}+$";
    private static final String SPACE = " ";
    private static final String COMMA = ",";
    
    public static final int MONTHS_TO_CHECK = 6;

    @Autowired
    private PublicDataManager dataManager;

    @Autowired
    protected BlocksManager blocksManager;

    public MergePlaceBot(OpObject botObject) {
        super(botObject);
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

    @Override
    public MergePlaceBot call() throws Exception {
        int similarPlacesCnt = 0;
        addNewBotStat();
        try {
            info("Merge places has started");
            PublicDataManager.PublicAPIEndpoint<?, ?> apiEndpoint = dataManager.getEndpoint(HISTORY);
            if (apiEndpoint == null) {
            	setFailedState();
                info("Merge has failed: no history provider");
                return this;
            }
            
            Map<String, String[]> params = new ParameterMap<>();
			for (int i = 0; i < MONTHS_TO_CHECK; i++) {
				LocalDate dt = LocalDate.now().minusMonths(i);
				LocalDate start = LocalDate.of(dt.getYear(), dt.getMonth(), 1);
				LocalDate dts = LocalDate.now().minusMonths(i - 1);
				LocalDate end = LocalDate.of(dts.getYear(), dts.getMonth(), 1).minusDays(1);
				params.put(START_DATA, new String[] { start.toString() });
				params.put(END_DATA, new String[] { end.toString() });
				params.put(FILTER, new String[] { POSSIBLE_MERGE });
				List<Feature> list = getFeatures(apiEndpoint, params);
				List<List<String>> deleted = new ArrayList<>();
				List<OpObject> edited = new ArrayList<>();
				List<List<Feature>> mergeGroups = getMergeGroups(list);
				mergeGroups.removeIf(mergeGroup -> mergeGroup.size() > 2);

				for (List<Feature> mergeGroup : mergeGroups) {
					Feature newPlace = mergeGroup.get(0);
					Feature oldPlace = mergeGroup.get(1);
					if (areNearbyPlaces(newPlace, oldPlace)) {
						similarPlacesCnt++;
						mergePlaces(newPlace, oldPlace, deleted, edited);
					}

					int cnt = addOperations(deleted, edited);
					info(String.format("Merge places finished for %s - %s: similar places %d, merged places %d, operations %d",
							start.toString(), end.toString(), similarPlacesCnt, deleted.size(), cnt));
				}
			} 
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
            }
            op.addDeleted(deleted.get(i));
            op.addEdited(edited.get(i));
            batch++;
            cnt++;
        }
        if (batch > 0) {
        	addOpIfNeeded(op, true);
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

    private void mergePlaces(Feature f1, Feature f2, List<List<String>> deleted, List<OpObject> edited) {
        OpObject newObj = blocksManager.getBlockchain().getObjectByName(OPR_PLACE, getPlaceId(f1));
        OpObject oldObj = blocksManager.getBlockchain().getObjectByName(OPR_PLACE, getPlaceId(f2));

        if (newObj != null && oldObj != null) {
            List<Map<String, Object>> newOsmList = newObj.getField(null, F_SOURCE, F_OSM);
            List<Map<String, Object>> oldOsmList = oldObj.getField(null, F_SOURCE, F_OSM);
            if (newOsmList != null && oldOsmList != null && isMergeByName(newOsmList, oldOsmList)) {
                addOperation(oldObj, newObj, oldOsmList, newOsmList, deleted, edited);
            }
        }
    }

    private List<String> getPlaceId(Feature feature) {
        return new ArrayList<>(Arrays.asList(feature.properties()
                .get(OPR_ID).getAsString()
                .split(COMMA)));
    }

    private void addOperation(OpObject oldObj, OpObject newObj, List<Map<String, Object>> oldOsmList,
                              List<Map<String, Object>> newOsmList, List<List<String>> deleted, List<OpObject> edited) {

        OpObject editObj = new OpObject();
        editObj.setId(oldObj.getId().get(0), oldObj.getId().get(1));
        TreeMap<String, Object> current = new TreeMap<>();
        TreeMap<String, Object> changed = new TreeMap<>();
        TreeMap<String, Object> appendObj = new TreeMap<>();
        if (newOsmList.size() > 1) {
            appendObj.put(APPEND_MANY, newOsmList);
        } else {
            appendObj.put(APPEND, newOsmList.get(0));
        }
        changed.put(SOURCE_OSM, appendObj);
        current.put(F_SOURCE, new TreeMap<>().put(F_OSM, oldOsmList));
        editObj.putObjectValue(OpObject.F_CHANGE, changed);
        editObj.putObjectValue(OpObject.F_CURRENT, current);

        deleted.add(newObj.getId());
        edited.add(editObj);
    }

    private String getPlaceName(List<Map<String, Object>> osmList) {
        for (Map<String, Object> osm : osmList) {
            Map<String, Object> tagsValue = (Map<String, Object>) osm.get(TAGS);
            if (tagsValue != null && tagsValue.containsKey(PLACE_NAME)) {
                return tagsValue.get(PLACE_NAME).toString();
            }
        }
        return null;
    }

    private boolean isMergeByName(List<Map<String, Object>> newOsmList, List<Map<String, Object>> oldOsmList) {
        String oldName = getPlaceName(oldOsmList);
        String newName = getPlaceName(newOsmList);

        Collator collator = Collator.getInstance();
        collator.setStrength(Collator.PRIMARY);

        if (oldName == null && newName != null) {
            return true;
        }

        if (oldName != null && newName != null) {
            String oldNameLower = oldName.toLowerCase();
            String newNameLower = newName.toLowerCase();
            if (collator.compare(oldNameLower, newNameLower) > 0) {
                return true;
            }

            List<String> oldNameList = getWords(oldNameLower);
            List<String> newNameList = getWords(newNameLower);
            Collections.sort(oldNameList);
            Collections.sort(newNameList);

            return oldNameList.equals(newNameList)
                    || isSubCollection(newNameList, oldNameList, collator)
                    || isSubCollection(oldNameList, newNameList, collator);
        }
        return false;
    }

    private List<String> getWords(String name) {
        List<String> wordList = new ArrayList<>();
        for (String word : name.split(SPACE)) {
            wordList.add(word.trim().replaceAll(ALL_PUNCTUATION, ""));
        }
        return wordList;
    }

    private boolean isSubCollection(List<String> mainList, List<String> subList, Collator collator) {
        int matchedCount = 0;
        for (String wordMain : mainList) {
            for (String wordSub : subList) {
                if (collator.compare(wordMain, wordSub) > 0) {
                    matchedCount++;
                    if (matchedCount == subList.size()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}