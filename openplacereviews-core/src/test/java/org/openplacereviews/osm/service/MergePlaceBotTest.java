package org.openplacereviews.osm.service;

import org.junit.Before;
import org.junit.Test;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.util.JsonFormatter;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_OSM;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_SOURCE;

public class MergePlaceBotTest {

    private static final String PLACES_PATH = "./src/test/java/org/openplacereviews/osm/service/places.json";
    private static final String PLACES_NOT_MERGE_PATH = "./src/test/java/org/openplacereviews/osm/service/places_not_merge.json";

    private MergePlaceBot bot;
    private OpObject[] places;
    private OpObject[] placesNotMerge;

    @Before
    public void beforeEachTestMethod() throws Exception{
        JsonFormatter formatter = new JsonFormatter();
        OpObject botObject = new OpObject();
        botObject.setId("1");
        bot = new MergePlaceBot(botObject);
        bot.TRACE = false;
        places = formatter.fromJson(Files.newBufferedReader(Paths.get(PLACES_PATH)), OpObject[].class);
        placesNotMerge = formatter.fromJson(Files.newBufferedReader(Paths.get(PLACES_NOT_MERGE_PATH)), OpObject[].class);
    }

    @Test
    public void testMergeByName() {
        for (int i = 0; i < places.length; i += 2) {
            List<Map<String, Object>> newOsmList = places[i].getField(null, F_SOURCE, F_OSM);
            List<Map<String, Object>> oldOsmList = places[i + 1].getField(null, F_SOURCE, F_OSM);

            assertTrue(bot.isMergeByName(newOsmList, oldOsmList));
        }
    }

    @Test
    public void testMergePlaces() {
        for (int i = 0; i < places.length; i += 2) {
            List<List<String>> deleted = new ArrayList<>();
            List<OpObject> edited = new ArrayList<>();
            bot.mergePlaces(places[i], places[i + 1], deleted, edited);

            assertFalse(deleted.isEmpty());
        }
    }

    @Test
    public void testNotMergeByName() {
        for (int i = 0; i < placesNotMerge.length; i += 2) {
            List<Map<String, Object>> newOsmList = placesNotMerge[i].getField(null, F_SOURCE, F_OSM);
            List<Map<String, Object>> oldOsmList = placesNotMerge[i + 1].getField(null, F_SOURCE, F_OSM);

            assertFalse(bot.isMergeByName(newOsmList, oldOsmList));
        }
    }

}
