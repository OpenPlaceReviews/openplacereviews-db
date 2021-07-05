package org.openplacereviews.osm.service;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.osm.service.MergePlaceBot.MatchType;

public class MergePlaceBotTest {

	private static final String PLACES_PATH = "src/test/resources/merge/places.json";
	private static final String PLACES_NOT_MERGE_PATH = "src/test/resources/merge/places_not_merge.json";

	private MergePlaceBot bot;
	private OpObject[] places;
	private OpObject[] placesNotMerge;

	@Before
	public void beforeEachTestMethod() throws Exception {
		JsonFormatter formatter = new JsonFormatter();
		OpObject botObject = new OpObject();
		botObject.setId("1");
		bot = new MergePlaceBot(botObject);
		bot.TRACE = false;
		places = formatter.fromJson(Files.newBufferedReader(Paths.get(PLACES_PATH)), OpObject[].class);
		placesNotMerge = formatter.fromJson(Files.newBufferedReader(Paths.get(PLACES_NOT_MERGE_PATH)),
				OpObject[].class);
	}

	@Test
	public void testMergeByName() {
		for (int i = 0; i < places.length; i += 2) {
			List<OpObject> lst = new ArrayList<>();
			lst.add(places[i]);
			System.out.println(places[i]);
			System.out.println(places[i+1]);
			assertTrue(bot.mergePlaces(EnumSet.allOf(MatchType.class), places[i + 1], lst, new ArrayList<>(), new ArrayList<>()) != null);
		}
	}

	@Test
	public void testMergePlaces() {
		for (int i = 0; i < places.length; i += 2) {
			List<List<String>> deleted = new ArrayList<>();
			List<OpObject> edited = new ArrayList<>();
			List<OpObject> lst = new ArrayList<>();
			lst.add(places[i]);
			bot.mergePlaces(EnumSet.allOf(MatchType.class), places[i + 1], lst,
					deleted, edited);

			assertFalse(deleted.isEmpty());
		}
	}

	@Test
	public void testNotMergeByName() {
		for (int i = 0; i < placesNotMerge.length; i += 2) {
			List<OpObject> lst = new ArrayList<>();
			lst.add(places[i]);
			assertFalse(bot.mergePlaces(EnumSet.allOf(MatchType.class), places[i + 1],
					lst, new ArrayList<>(), new ArrayList<>()) == null);
		}
	}

}
