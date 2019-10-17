package org.openplacereviews.osm.util;

import org.junit.Test;
import org.openplacereviews.opendb.ops.OpObject;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_TAGS;

public class PlaceOpObjectHelperTest {

	@Test
	public void testGenerateDiff() {
		OpObject opObject = null;
		String field = F_TAGS + ".";
		Map<String, Object> change = new TreeMap<>();
		Map<String, Object> current = new TreeMap<>();
		Map<String, Object> oldM = new TreeMap<>();
		oldM.put("addr:city", "台北市");
		oldM.put("amenity", "cafe");
		oldM.put("http://touat.com.tw", "running");
		Map<String, Object> newM = new TreeMap<>();
		newM.put("addr:city", "rets");
		newM.put("amenity", "bar");
		newM.put("http://touat.com.tw1", "test");
		PlaceOpObjectHelper.generateDiff(opObject, field, change, current, oldM, newM);

		assertEquals("{tags.addr:city=台北市, tags.amenity=cafe, tags.{http://touat.com.tw}=running}", current.toString());
		assertEquals("{tags.addr:city={set=rets}, tags.amenity={set=bar}, tags.{http://touat.com.tw1}={set=test}, tags.{http://touat.com.tw}=delete}", change.toString());
	}
}
