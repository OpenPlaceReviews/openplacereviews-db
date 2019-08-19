package org.openplacereviews.osm.service;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class BotPlacePublisherTest {

	@Test
	public void testCoordinates() {
		TreeMap<String,String> map = new TreeMap<>();
		map.put("node[\"leisure\"]","");
		map.put("node[\"name\"]","(50.3456,30.3671,50.5580,30.8671)");
		map.put("node[\"tourism\"]","");
		map.put("node[\"shop\"]","(50.3456,30.3671,50.5580,30.8671)");

		// generate separate request only wit coordinateTags!!
		TreeMap<String, String> tagsWithCoordinates = new TreeMap<>();

		// generate separate request for each point
		TreeMap<String, String> tagsWithoutCoordinates = new TreeMap<>();

		for (Map.Entry<String,String> e : map.entrySet()) {
			if (e.getValue().equals("")) {
				tagsWithoutCoordinates.put(e.getKey(), e.getValue());
			} else {
				tagsWithCoordinates.put(e.getKey(), e.getValue());
			}
		}

		double quadr_size = 360/32d; //tile size = 128
		double quadr_size1 = 180/16d; // tile size = 64
		System.out.println(quadr_size + " " + quadr_size1);
		double p1 = -90, p2 = -180, p3 = 90, p4 = 180;
		List<String> coordinates = new ArrayList<>();
		while (p2 != 180) {
			double t = p1 + quadr_size1;
			double t1 = p2 + quadr_size;
			coordinates.add(p1 + ", " + p2 + ", " + t + ", " + t1);
			p1 += quadr_size1;
			if (p1 == 90) {
				p1 = -90;
				p2 += quadr_size;
			}
		}

		for (String s : coordinates) {
			System.out.println(s);
		}

		System.out.println("SIZE: "+ coordinates.size());

	}


}
