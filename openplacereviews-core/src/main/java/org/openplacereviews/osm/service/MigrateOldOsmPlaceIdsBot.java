package org.openplacereviews.osm.service;


import static org.openplacereviews.opendb.ops.OpObject.F_CHANGE;
import static org.openplacereviews.opendb.ops.OpObject.F_CURRENT;
import static org.openplacereviews.opendb.ops.OpObject.F_ID;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.bots.GenericBlockchainReviewBot;
import org.openplacereviews.osm.util.PlaceOpObjectHelper;

public class MigrateOldOsmPlaceIdsBot extends GenericBlockchainReviewBot<MigrateOldOsmPlaceIdsBot> {

	public MigrateOldOsmPlaceIdsBot(OpObject botObject) {
		super(botObject);
	}
	
	public String objectType() {
		return "opr.place";
	}
	
	
	@Override
	public String getTaskDescription() {
		return "Delete and rename old osm place ids";
	}

	@Override
	public String getTaskName() {
		return "migrate-old-osm-place-ids";
	}
	
	public boolean processSingleObject(OpObject o, OpOperation op, OpBlock lastBlockHeader) {
		List<Map<String, Object>> vls = o.getField(null, PlaceOpObjectHelper.F_SOURCE, PlaceOpObjectHelper.F_OLD_OSM_IDS);
		if (vls != null) {
			OpObject newVersion = blocksManager.getBlockchain().getObjectByName(objectType(), o.getId());
			if (newVersion != null) {
				List<Map<String, Object>> nvls = newVersion.getField(null, PlaceOpObjectHelper.F_SOURCE, PlaceOpObjectHelper.F_OLD_OSM_IDS);
				// skip cause it was changed in a newer version 
				if (!vls.equals(nvls)) {
					return false;
				}
			}
			OpObject editObject = new OpObject();
			editObject.putObjectValue(F_ID, o.getId());
			Map<String, Object> changeTagMap = new TreeMap<>();
			Map<String, Object> currentTagMap = new TreeMap<>();
			for (Map<String, Object> od : vls) {
				Map<String, Object> lod = new TreeMap<String, Object>(od);
				lod.put(PlaceOpObjectHelper.F_DELETED_OSM, lastBlockHeader.getDateString());
				changeTagMap.put(PlaceOpObjectHelper.F_SOURCE + "." + PlaceOpObjectHelper.F_OSM,
						PlaceOpObjectHelper.append(lod));
			}
			changeTagMap.put(PlaceOpObjectHelper.F_SOURCE + "." + PlaceOpObjectHelper.F_OLD_OSM_IDS,
					OpBlockChain.OP_CHANGE_DELETE);
			currentTagMap.put(PlaceOpObjectHelper.F_SOURCE + "." + PlaceOpObjectHelper.F_OLD_OSM_IDS, vls);
			editObject.putObjectValue(F_CHANGE, changeTagMap);
			editObject.putObjectValue(F_CURRENT, currentTagMap);
			op.addEdited(editObject);
			return true;
		}
		return false;
	}

}
