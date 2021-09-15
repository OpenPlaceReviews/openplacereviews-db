package org.openplacereviews.osm.service;

import static org.mockito.ArgumentMatchers.any;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_DELETED_OSM;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.zip.GZIPInputStream;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.openplacereviews.api.OprMapCollectionApiResult;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.osm.service.MergePlaceBot.MergeInfo;

import com.github.filosganga.geogson.gson.GeometryAdapterFactory;
import com.github.filosganga.geogson.model.Feature;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public class MergePlaceBotOtherTest {

    private static final String PLACES_PATH = "src/test/resources/merge/opr_june_2021.json.gz";

    @Spy
	private MergePlaceBot bot;

	@Ignore
    @Test
	public void testCoordinates() throws IOException {
    	Gson geoJson = new GsonBuilder().registerTypeAdapterFactory(new GeometryAdapterFactory()).create();
    	OprMapCollectionApiResult rs = geoJson.fromJson(new InputStreamReader(new GZIPInputStream(new FileInputStream(PLACES_PATH))), OprMapCollectionApiResult.class);

		List<Feature> listFeatures = rs.geo.features();
		OprMapCollectionApiResult newRs = new OprMapCollectionApiResult();
		newRs.geo.features().addAll(replacesFeature(listFeatures));
		newRs.parameters.putAll(rs.parameters);

        // bot.TRACE = false;
        MockitoAnnotations.initMocks(this);

        Mockito.doAnswer(new Answer<OpObject>() {

			@Override
			public OpObject answer(InvocationOnMock invocation) throws Throwable {
				Feature f = invocation.getArgument(0);
				OpObject obj = new OpObject();
				List<String> id = bot.getPlaceId(f);
				obj.setId(id.get(0), id.get(1));
				Map<String, Object> osm = new TreeMap<>();
				osm.put("changeset", f.properties().get("changeset").getAsString());
				osm.put("osm_tag", f.properties().get("osm_tag").getAsString());
				osm.put("osm_value", f.properties().get("osm_value").getAsString());
				osm.put("timestamp", f.properties().get("timestamp").getAsString());
				osm.put("type", f.properties().get("osm_type").getAsString());
				osm.put("version", f.properties().get("version").getAsString());
				if (f.properties().containsKey("deleted")) {
					osm.put("deleted", f.properties().get("deleted").getAsString());
				}
				osm.put("id", f.properties().get("osm_id").getAsLong());
				osm.put("lat", f.properties().get("lat").getAsDouble());
				osm.put("lon", f.properties().get("lon").getAsDouble());
				
				Map<String, String> tags= new TreeMap<>();
				JsonObject objTags = f.properties().get("tags").getAsJsonObject();
				for(String k : objTags.keySet()) {
					tags.put(k, objTags.get(k).getAsString());
				}
				osm.put("tags", tags);
				obj.setFieldByExpr("source.osm", Collections.singletonList(osm));
				return obj;
			}
		}).when(bot).getCurrentObjectInBot(any());
        bot.TRACE = false;
        
        MergeInfo info = new MergeInfo();
    	bot.mergeAndClosePlaces(newRs, info);
    	System.out.println(String.format("Merge places has finished: place groups %d, closed places %d, found similar places %d, merged %d", 
				info.mergedGroupSize, info.closedPlaces, info.similarPlacesCnt, info.mergedPlacesCnt));
    	Assert.assertTrue(info.mergedPlacesCnt > 2100);
    }

	private List<Feature> replacesFeature(List<Feature> listFeatures) {
		List<List<Feature>> mergeGroups = getMergeGroups(listFeatures);
		List<Feature> newList = new ArrayList<>();
		for (List<Feature> group : mergeGroups) {
			ArrayList<Feature> temp = new ArrayList<>(group);
			Collections.reverse(temp);
			newList.addAll(temp);
		}
		return newList;
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
}
