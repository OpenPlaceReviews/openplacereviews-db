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
import org.openplacereviews.osm.model.OsmMapUtils;
import org.openplacereviews.osm.util.PlaceOpObjectHelper;
import org.springframework.beans.factory.annotation.Autowired;


import java.time.LocalDate;
import java.util.*;

import static org.openplacereviews.osm.util.PlaceOpObjectHelper.*;

public class MergePlaceBot extends GenericMultiThreadBot<MergePlaceBot> {

    public static final String OPR_PLACE = "opr.place";

    private int similarPlacesCnt = 0;
    private int mergedPlacesCnt = 0;
    private int operationsCnt = 0;

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
        return "opr.place";
    }


    @Override
    public MergePlaceBot call() throws Exception {

        PublicDataManager.PublicAPIEndpoint<?, ?> apiEndpoint = dataManager.getEndpoint("history");

        if (apiEndpoint != null) {
            Map<String, String[]> params = new ParameterMap<>();
            addParams(params);
            List<Feature> list = getFeatures(apiEndpoint, params);
            if (list != null) {
                int i;
                List<List<String>> deleted = new ArrayList<>();
                List<OpObject> edited = new ArrayList<>();
                for (i = 0; i < list.size(); i++) {
                    Feature curr = list.get(i);
                    Feature next = null;
                    if (i < (list.size() - 1)) {
                        next = list.get(i + 1);
                    }
                    if (areNearbyPlaces(curr, next)) {
                        similarPlacesCnt++;
                        if (mergePlaces(curr, next, deleted, edited)) {
                            i++;
                        }
                    }
                }

                List<OpOperation> opList = createOperations(deleted, edited);
                mergedPlacesCnt = deleted.size();
                operationsCnt = opList.size();
                LOGGER.info(String.format("Bot finished! Similar places %d, merged places %d, operations %d",
                        similarPlacesCnt, mergedPlacesCnt, operationsCnt));
                for (OpOperation op : opList) {
                    addOpIfNeeded(op, true);
                }
            }
        }
        return this;
    }

    private List<List<OpObject>> getEditedResultList(List<OpObject> list) {
        int n = 10;
        List<List<OpObject>> resultDeleted = new LinkedList<>();
        int size = list.size();
        for (int i = 0; i <= size; i += n) {
            resultDeleted.add(list.subList(i, Math.min(i + n, size)));
        }
        return resultDeleted;
    }

    private List<List<List<String>>> getDeletedResultList(List<List<String>> list) {
        int n = 10;
        List<List<List<String>>> resultDeleted = new LinkedList<>();
        int size = list.size();
        for (int i = 0; i <= size; i += n) {
            resultDeleted.add(list.subList(i, Math.min(i + n, size)));
        }
        return resultDeleted;
    }

    private List<OpOperation> createOperations(List<List<String>> deleted, List<OpObject> edited) {
        List<List<List<String>>> resultDeleted = getDeletedResultList(deleted);
        List<List<OpObject>> resultEdited = getEditedResultList(edited);
        List<OpOperation> opList = new ArrayList<>();
        for (List<List<String>> deletedList : resultDeleted) {
            for (List<String> id : deletedList) {
                OpOperation op = initOpOperation(objectType());
                op.addDeleted(id);
                opList.add(op);
            }
        }
        for (OpOperation op : opList) {
            for (List<OpObject> editedList : resultEdited) {
                for (OpObject obj : editedList) {
                    op.addEdited(obj);
                }
            }
        }

        return opList;
    }

    private void addParams(Map<String, String[]> params) {
        LocalDate date = LocalDate.now();
        //String[] startDate = {date.minusDays(10).toString()};
        //String[] endDate = {date.toString()};
        String[] startDate = {"2021-05-17"};
        String[] endDate = {"2021-05-19"};
        String[] filter = {"POSSIBLE_MERGE"};
        params.put("date", startDate);
        params.put("date2", endDate);
        params.put("requestFilter", filter);
    }

    private List<Feature> getFeatures(PublicDataManager.PublicAPIEndpoint<?, ?> apiEndpoint, Map<String, String[]> params) {
        OprMapCollectionApiResult collection = (OprMapCollectionApiResult) apiEndpoint.getContentObject(params);
        return collection.geo.features();
    }

    private boolean areNearbyPlaces(Feature f1, Feature f2) {
        if (f2 != null && !f2.properties().containsKey("deleted") && f1 != null && !f1.properties().containsKey("deleted")) {
            Point p1 = (Point) f1.geometry();
            Point p2 = (Point) f1.geometry();
            int similarPlaceDistance = 150;
            return OsmMapUtils.getDistance(p1.lat(), p1.lon(), p2.lat(), p2.lon()) < similarPlaceDistance;
        }
        return false;
    }

    private boolean mergePlaces(Feature f1, Feature f2, List<List<String>> deleted, List<OpObject> edited) {
        OpObject obj1 = blocksManager.getBlockchain().getObjectByName(OPR_PLACE, getPlaceId(f1));
        OpObject obj2 = blocksManager.getBlockchain().getObjectByName(OPR_PLACE, getPlaceId(f2));

        if (obj1 != null && obj2 != null) {
            List<Map<String, Object>> osmSourcesObj1 = obj1.getField(null, F_SOURCE, F_OSM);
            List<Map<String, Object>> osmSourcesObj2 = obj2.getField(null, F_SOURCE, F_OSM);
            if (idDeletedPlace(osmSourcesObj1) && !idDeletedPlace(osmSourcesObj2) && hasNewName(osmSourcesObj2, osmSourcesObj1)) {
                addOperation(obj1, obj2, osmSourcesObj1, osmSourcesObj2, deleted, edited);
                return true;
            } else if (!idDeletedPlace(osmSourcesObj1) && idDeletedPlace(osmSourcesObj2) && hasNewName(osmSourcesObj1, osmSourcesObj2)) {
                addOperation(obj2, obj1, osmSourcesObj2, osmSourcesObj1, deleted, edited);
                return true;
            }
        }
        return false;
    }

    private List<String> getPlaceId(Feature feature) {
        return new ArrayList<>(Arrays.asList(feature.properties()
                .get("opr_id").toString()
                .replace("\"", "")
                .split(",")));
    }

    private void addOperation(OpObject oldPlace, OpObject newPlace, List<Map<String, Object>> oldSources,
                              List<Map<String, Object>> newSources, List<List<String>> deleted, List<OpObject> edited) {

        OpObject editObj = new OpObject();
        editObj.setId(oldPlace.getId().get(0), oldPlace.getId().get(1));
        TreeMap<String, Object> current = new TreeMap<>();
        TreeMap<String, Object> changed = new TreeMap<>();
        TreeMap<String, Object> appendObj = new TreeMap<>();
        if (newSources.size() > 1) {
            appendObj.put("appendMany", newSources);
        } else {
            appendObj.put("append", newSources);
        }
        changed.put("source.osm", appendObj);
        current.put("source.osm", oldSources);
        editObj.putObjectValue(OpObject.F_CHANGE, changed);
        editObj.putObjectValue(OpObject.F_CURRENT, current);

        deleted.add(newPlace.getId());
        edited.add(editObj);
    }

    private boolean idDeletedPlace(List<Map<String, Object>> osmSources) {
        boolean allOsmRefsDeleted = true;
        for (Map<String, Object> osm : osmSources) {
            if (!osm.containsKey(PlaceOpObjectHelper.F_DELETED_OSM)) {
                allOsmRefsDeleted = false;
                break;
            }
        }
        return allOsmRefsDeleted;
    }

    private boolean hasNewName(List<Map<String, Object>> newSources, List<Map<String, Object>> oldSources) {
        return hasName(newSources) && !hasName(oldSources);
    }

    private boolean hasName(List<Map<String, Object>> sources) {
        for (Map<String, Object> osm : sources) {
            Map<String, Object> tagsValue = (Map<String, Object>) osm.get("tags");
            if (tagsValue != null && tagsValue.containsKey("name")) {
                return true;
            }
        }
        return false;
    }
}
