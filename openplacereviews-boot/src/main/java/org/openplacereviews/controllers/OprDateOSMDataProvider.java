package org.openplacereviews.controllers;

import com.github.filosganga.geogson.model.Feature;
import com.github.filosganga.geogson.model.FeatureCollection;
import com.github.filosganga.geogson.model.Point;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.commons.lang3.time.DateUtils;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.openplacereviews.opendb.ops.OpOperation.F_TYPE;
import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.*;

public class OprDateOSMDataProvider extends OprPlaceDataProvider {

	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd/MM/yyyy");
	public static final String OPR_PLACE = "opr.place";
	public static final String PARAM_OSM_DATE = "date";
	public static final String OSM_ID = "osm_id";
	public static final String OSM_TYPE = "osm_type";
	public static final String OPR_ID = "opr_id";
	public static final String OSM_INDEX = "osm_index";
	public static final String BLOCK_TIMESTAMP = "block_timestamp";
	public static final String BLOCK_HASH = "block_hash";
	public static final String BLOCK_ID = "block_id";
	public static final String OP_HASH = "op_hash";

	public void getOsmObjectByDate(String stringDate, FeatureCollection featureCollection) throws ParseException {
		Date date = DATE_FORMAT.parse(stringDate);
		List<OpBlock> listBlocks = blocksManager.getBlockchain().getBlockHeaders(-1);
		List<OpBlock> blocksByDate = new LinkedList<>();
		for (OpBlock opBlock : listBlocks) {
			Date blockDate = OpBlock.dateFormat.parse(opBlock.getDateString());
			if (DateUtils.isSameDay(date, blockDate)) {
				blocksByDate.add(opBlock);
			}
		}

		OpBlockChain blc = blocksManager.getBlockchain();
		for (OpBlock block : blocksByDate) {
			OpBlock fullBlock = blc.getFullBlockByRawHash(block.getRawHash());
			List<OpOperation> opOperations = fullBlock.getOperations();
			for (OpOperation opOperation : opOperations) {
				if (opOperation.getType().equals(OPR_PLACE)) {
					for (OpObject opObject : opOperation.getCreated()) {
						generateCreateEntityFromOpObject(opObject, featureCollection, block, opOperation.getRawHash());
					}
					// TODO add edited
					// TODO add removed
				}
			}
		}

	}

	@SuppressWarnings("unchecked")
	private void generateCreateEntityFromOpObject(OpObject opObject, FeatureCollection featureCollection, OpBlock opBlock, String opHash) {
		List<Map<String, Object>> osmList = opObject.getField(null, F_SOURCE, F_OSM);
		if (osmList.size() == 0) {
			return;
		}
		Map<String, Object> osm = osmList.get(0);
		double lat = (double) osm.get(ATTR_LATITUDE);
		double lon = (double) osm.get(ATTR_LONGITUDE);
		Point p = Point.from(lon, lat);
		ImmutableMap.Builder<String, JsonElement> bld = ImmutableMap.builder();

		bld.put(OSM_ID, new JsonPrimitive((Number) osm.get(OpObject.F_ID)));
		bld.put(OSM_TYPE, new JsonPrimitive((String) osm.get(F_TYPE)));

		Map<String, Object> tagsValue = (Map<String, Object>) osm.get(F_TAGS);
		if (tagsValue != null) {
			JsonObject obj = new JsonObject();
			Iterator<Map.Entry<String, Object>> it = tagsValue.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, Object> e = it.next();
				obj.add(e.getKey(), new JsonPrimitive(e.getValue().toString()));
			}
			bld.put(F_TAGS, obj);
		}

		JsonArray opr_id = new JsonArray();
		for (String key: opObject.getId()) {
			opr_id.add(key);
		}
		bld.put(OPR_ID, opr_id);
		bld.put(OSM_INDEX, new JsonPrimitive(0));
		bld.put(BLOCK_TIMESTAMP, new JsonPrimitive(opBlock.getDateString()));
		bld.put(BLOCK_HASH, new JsonPrimitive(opBlock.getRawHash()));
		bld.put(BLOCK_ID, new JsonPrimitive(opBlock.getBlockId()));
		bld.put(OP_HASH, new JsonPrimitive(opHash));
		bld.put(ATTR_LATITUDE, new JsonPrimitive(lat));
		bld.put(ATTR_LONGITUDE, new JsonPrimitive(lon));
		bld.put(F_VERSION, new JsonPrimitive((String) osm.get(F_VERSION)));
		bld.put(F_CHANGESET, new JsonPrimitive((String) osm.get(F_CHANGESET)));

		Feature f = new Feature(p, bld.build(), Optional.absent());
		featureCollection.features().add(f);
	}

	@Override
	public FeatureCollection getContent(String date) {
		FeatureCollection fc = new FeatureCollection(new ArrayList<>());
		if(!date.equals("")) {
			try {
				getOsmObjectByDate(date, fc);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}

		return fc;
	}

	@Override
	public String formatParams(Map<String, String[]> params) {
		String[] tls = params.get(PARAM_OSM_DATE);
		if(tls != null && tls.length == 1 && tls[0] != null) {
			return tls[0];
		}
		return "";
	}

}
