package org.openplacereviews.osm.util;

import com.github.filosganga.geogson.model.Feature;
import com.github.filosganga.geogson.model.Point;
import com.google.gson.Gson;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.openplacereviews.api.OprMapCollectionApiResult;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.PublicDataManager;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.osm.model.OsmMapUtils;

import java.text.Collator;
import java.util.*;

import static org.openplacereviews.api.BaseOprPlaceDataProvider.OPR_ID;
import static org.openplacereviews.api.OprHistoryChangesProvider.OPR_PLACE;
import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.*;

public class MergeUtil {

	public static final String PLACE_NAME = "name";
	public static final String WIKIDATA = "wikidata";
	public static final String WEBSITE = "website";
	private static final String ALL_PUNCTUATION = "^\\p{Punct}+|\\p{Punct}+$";
	private static final String SPACE = " ";
	private static final String COMMA = ",";
	private static final String OLD_NAME = "old_name";
	private static final String GEO = "geo";
	private static final String TILE_ID = "tileid";
	private static final int SIMILAR_PLACE_DISTANCE = 150;

	public enum MatchType {
		NAME_MATCH(true),
		OTHER_TAGS_MATCH(true),
		OTHER_NAME_MATCH(true),
		EMPTY_NAME_MATCH(false);

		public final boolean allow2PlacesMerge;

		MatchType(boolean allow2PlacesMerge) {
			this.allow2PlacesMerge = allow2PlacesMerge;
		}
	}

	public static boolean match(MatchType mt, Map<String, String> oldOsmTags, Map<String, String> newOsmTags) {
		String oldName = oldOsmTags.get(PLACE_NAME);
		String newName = newOsmTags.get(PLACE_NAME);
		if (mt == MatchType.OTHER_TAGS_MATCH) {
			if (equalsNotEmptyStringValue(oldOsmTags.get(WIKIDATA), newOsmTags.get(WIKIDATA))) {
				return true;
			}
			if (equalsNotEmptyStringValue(oldOsmTags.get(WEBSITE), newOsmTags.get(WEBSITE))) {
				return true;
			}
		} else if (mt == MatchType.NAME_MATCH) {
			return checkNames(oldName, newName);
		} else if (mt == MatchType.OTHER_NAME_MATCH) {
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

	public static OprMapCollectionApiResult getDataReport(String tileId, PublicDataManager dataManager) {
		PublicDataManager.PublicAPIEndpoint<?, ?> apiEndpoint = dataManager.getEndpoint(GEO);
		if (apiEndpoint != null) {
			Map<String, String[]> dataParams = new LinkedHashMap<>();
			dataParams.put(TILE_ID, new String[]{tileId});
			return getReport(apiEndpoint, dataParams);
		}
		return null;
	}

	private static OprMapCollectionApiResult getReport(PublicDataManager.PublicAPIEndpoint<?, ?> apiEndpoint, Map<String, String[]> params) {
		return (OprMapCollectionApiResult) apiEndpoint.getContentObject(params);
	}

	public static List<List<Feature>> getMergeGroups(List<Feature> list) {
		List<List<Feature>> mergeGroups = new ArrayList<>();
		if (list == null) {
			return mergeGroups;
		}
		int currentGroupBeginIndex = 0;
		for (int i = 1; i < list.size() - 1; i++) {
			Point currentPoint = (Point) list.get(i).geometry();
			Point prevPoint = (Point) list.get(i).geometry();
			if (isDeleted(list, i)
					&& (!isDeleted(list, i - 1)
					|| (isDeleted(list, i - 1) && getDistance(currentPoint.lat(), currentPoint.lon(), prevPoint.lat(), prevPoint.lon()) > SIMILAR_PLACE_DISTANCE))) {
				mergeGroups.add(list.subList(currentGroupBeginIndex, i));
				currentGroupBeginIndex = i;
			}
		}
		mergeGroups.add(list.subList(currentGroupBeginIndex, list.size()));
		return mergeGroups;
	}

	public static double getDistance(double lat1, double lon1, double lat2, double lon2) {
		return OsmMapUtils.getDistance(lat1, lon1, lat2, lon2);
	}

	public static boolean equalsNotEmptyStringValue(String s1, String s2) {
		if (OUtils.isEmpty(s1) || OUtils.isEmpty(s2)) {
			return false;
		}
		return OUtils.equalsStringValue(s1, s2);
	}

	public static List<String> getOtherPlaceName(Map<String, String> tags) {
		List<String> otherNames = new ArrayList<>();
		for (Map.Entry<String, String> tag : tags.entrySet()) {
			if (tag.getKey().startsWith(PLACE_NAME) || tag.getKey().equals(OLD_NAME)) {
				otherNames.add(tag.getValue());
			}
		}
		return otherNames;
	}

	public static boolean hasSimilarNameByFeatures(Feature feature, Feature currentFeature) {
		Map<String, String> objByTileTags = new HashMap<>();
		Map<String, String> currentObjTags = new HashMap<>();
		Map<String, Object> osmTagsF = new Gson().fromJson(feature.properties().get(F_TAGS).toString(), HashMap.class);
		for (Map.Entry<String, Object> entry : osmTagsF.entrySet()) {
			objByTileTags.put(entry.getKey(), entry.getValue().toString());
		}
		Map<String, Object> osmTagsCurr = new Gson().fromJson(currentFeature.properties().get(F_TAGS).toString(), HashMap.class);
		for (Map.Entry<String, Object> entry : osmTagsCurr.entrySet()) {
			currentObjTags.put(entry.getKey(), entry.getValue().toString());
		}

		for (MatchType mt : EnumSet.allOf(MatchType.class)) {
			if (match(mt, currentObjTags, objByTileTags)) {
				return true;
			}
		}
		return false;
	}

	public static boolean checkNames(String oldName, String newName) {
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

	public static OpObject getCurrentObject(Feature newPlace, BlocksManager blocksManager) {
		OpObject obj = blocksManager.getBlockchain().getObjectByName(OPR_PLACE, getPlaceId(newPlace));
		if (obj != null && obj.getField(null, F_DELETED_PLACE) != null) {
			return null;
		}
		return obj;
	}

	public static String getTileIdByFeature(Feature feature) {
		return feature.properties().get(OPR_ID).toString().replace("\"", "").split(",")[0];
	}

	public static Map<String, Object> getMainOsmFromList(OpObject o) {
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

	private static boolean isDeleted(List<Feature> list, int i) {
		return list.get(i).properties().containsKey(F_DELETED_OSM);
	}

	private static List<String> getPlaceId(Feature feature) {
		return new ArrayList<>(Arrays.asList(getOprGenId(feature).split(COMMA)));
	}

	public static String getOprGenId(Feature feature) {
		return feature.properties().get(OPR_ID).getAsString();
	}

	private static List<String> getWords(String name) {
		List<String> wordList = new ArrayList<>();
		for (String word : name.split(SPACE)) {
			String res = word.trim().replaceAll(ALL_PUNCTUATION, "");
			if (!res.equals("")) {
				wordList.add(res);
			}
		}
		return wordList;
	}

	private static boolean isSubCollection(List<String> mainList, List<String> subList, Collator collator) {
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

	private static int getMaxLevenshteinDistance(String wordMain, String wordSub) {
		int wordMainLength = wordMain.length();
		int wordSubLength = wordSub.length();
		int resLength = Math.min(wordMainLength, wordSubLength);
		if (resLength <= 4) {
			return 1;
		}
		if (resLength <= 6) {
			return 2;
		}
		if (resLength <= 8) {
			return 3;
		}
		return 4;
	}
}
