package org.openplacereviews.osm.service;

import static org.mockito.ArgumentMatchers.any;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import org.junit.Assert;
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
    
    @Test
	public void testCoordinates() throws IOException {
    	Gson geoJson = new GsonBuilder().registerTypeAdapterFactory(new GeometryAdapterFactory()).create();
    	OprMapCollectionApiResult rs = geoJson.fromJson(new InputStreamReader(new GZIPInputStream(new FileInputStream(PLACES_PATH))), OprMapCollectionApiResult.class);

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
		}).when(bot).getCurrentObject(any());
        bot.TRACE = false;
        
        MergeInfo info = new MergeInfo();
    	bot.mergePlaces(rs.geo.features(), info);
    	System.out.println(String.format("Merge places has finished: place groups %d, closed places %d, found similar places %d, merged %d", 
				info.mergedGroupSize, info.closedPlaces, info.similarPlacesCnt, info.mergedPlacesCnt));
    	Assert.assertTrue(info.mergedPlacesCnt > 2100);
    }

}
