package org.openplacereviews.osm.service;


import java.util.List;
import java.util.Map;

import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.bots.GenericBlockchainReviewBot;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.osm.util.PlaceOpObjectHelper;

public class PlaceTypeBot extends GenericBlockchainReviewBot<PlaceTypeBot> {

	public PlaceTypeBot(OpObject botObject) {
		super(botObject);
	}
	
	public String objectType() {
		return "opr.place";
	}
	
	
	private String evalPlaceType(OpObject o) {
		List<Map<String, Object>> osmList = o.getField(null, "source", "osm");
		if(osmList == null || osmList.size() == 0) {
			return "";
		}
		Map<String, Object> osm = osmList.get(0);
		return (String) osm.get("osm_value");
	}

	@Override
	public String getTaskDescription() {
		return "Evaluating main tags";
	}

	@Override
	public String getTaskName() {
		return "place-type";
	}
	
	public boolean processSingleObject(OpObject o, OpOperation op, OpBlock lastBlockHeader) {
		String tp = o.getField(null, "placetype");
		String ntp = evalPlaceType(o);
		if (!OUtils.equals(wrapNull(ntp), wrapNull(tp))) {
			PlaceOpObjectHelper.generateSetOperation(op, o.getId(), "placetype", tp, ntp);
			return true;
		}
		return false;
	}

	private String wrapNull(String tp) {
		if(tp == null) {
			return "";
		}
		return tp;
	}


	
}
