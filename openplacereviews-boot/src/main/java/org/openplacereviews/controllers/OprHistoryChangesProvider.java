package org.openplacereviews.controllers;

import static org.openplacereviews.controllers.MapCollection.TYPE_DATE;
import static org.openplacereviews.opendb.ops.OpObject.F_CHANGE;
import static org.openplacereviews.opendb.ops.OpObject.F_CURRENT;
import static org.openplacereviews.opendb.service.HistoryManager.DESC_SORT;
import static org.openplacereviews.opendb.service.HistoryManager.HISTORY_BY_OBJECT;
import static org.openplacereviews.osm.model.Entity.ATTR_ID;
import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_OSM;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_SOURCE;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.F_TAGS;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.api.ApiController;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.HistoryManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.InputStreamResource;

import com.github.filosganga.geogson.model.Feature;
import com.github.filosganga.geogson.model.FeatureCollection;
import com.github.filosganga.geogson.model.Point;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class OprHistoryChangesProvider extends OprPlaceDataProvider {

	private static final Log LOGGER = LogFactory.getLog(ApiController.class);

	public static final String OSM_INDEX = "osm_index";
	public static final String BLOCK_TIMESTAMP = "block_timestamp";
	public static final String BLOCK_HASH = "block_hash";
	public static final String BLOCK_ID = "block_id";
	public static final String OP_HASH = "op_hash";
	public static final String OPR_PLACE = "opr.place";
	public static final String OBJ_PREV = "prev";
	public static final String OBJ_NEXT = "next";

	public static final String ATTR_TYPE = "type";
	public static final String ATTR_SET = "set";

	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

	// constants ? or human strings?
	public static final String OBJ_CREATED = "Created";
	public static final String OBJ_EDITED = "Edited";
	public static final String OBJ_REMOVED = "Removed";
	
	protected static final String COLOR_GREEN = "green";
	protected static final String COLOR_BLUE = "blue";
	protected static final String COLOR_RED = "red";

	@Autowired
	private HistoryManager historyManager;

	public void retrievePlacesByDate(String stringDate, FeatureCollection fc) throws ParseException {
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
						generateEntity(fc, block, opOperation.getRawHash(), opObject, OBJ_CREATED, COLOR_GREEN);
					}
					for (OpObject opObject : opOperation.getEdited()) {
						generateEditedEntityFromOpObject(opObject, fc, block, opOperation.getRawHash());
					}
					for (List<String> objId : opOperation.getDeleted()) {
						generateRemovedEntityFromOpObject(objId, fc, block, opOperation.getRawHash());
					}
				}
			}
		}

	}


	private void generateRemovedEntityFromOpObject(List<String> objId, FeatureCollection fc, OpBlock opBlock, String opHash) {
		if (historyManager.isRunning()) {
			HistoryManager.HistoryObjectRequest historyObjectRequest = new HistoryManager.HistoryObjectRequest(
					HISTORY_BY_OBJECT,
					generateSearchStringKey(objId),
					1,
					DESC_SORT
			);
			historyManager.retrieveHistory(historyObjectRequest);
			List<HistoryManager.HistoryEdit> historyEdits = historyObjectRequest.historySearchResult;
			HistoryManager.HistoryEdit lastVersion = historyEdits.get(0);
			OpObject opObject = lastVersion.getObjEdit();
			generateEntity(fc, opBlock, opHash, opObject, OBJ_REMOVED, COLOR_RED);
		} else {
			// not supported cause we don't have lat/lon of deleted object
		}
	}

	private void generateEntity(FeatureCollection fc, OpBlock opBlock, String opHash, OpObject opObject, 
			String status, String color) {
		List<Map<String, Object>> osmList = opObject.getField(null, F_SOURCE, F_OSM);
		for (int i = 0; i < osmList.size(); i++) {
			Map<String, Object> osm = osmList.get(i);
			ImmutableMap.Builder<String, JsonElement> bld = ImmutableMap.builder();
			bld.put(OSM_INDEX, new JsonPrimitive(i));
			bld.put(TITLE, new JsonPrimitive(status + " " + getTitle(osm)));
			bld.put(COLOR, new JsonPrimitive(color));
			bld.put(PLACE_TYPE, new JsonPrimitive((String) osm.get(OSM_VALUE)));
			generateAllFields(osm, bld);
			generateObjectBlockInfo(opObject, opBlock, opHash, bld);

			Feature f = new Feature(generatePoint(osm), bld.build(), Optional.absent());
			fc.features().add(f);
		}
	}

	@SuppressWarnings("unchecked")
	private void generateAllFields(Map<String, Object> osm, ImmutableMap.Builder<String, JsonElement> bld) {
		for (String k : osm.keySet()) {
			if (k.equals(F_TAGS)) {
				continue;
			}
			if (k.equals(ATTR_ID)) {
				bld.put(OSM_ID, new JsonPrimitive((long) osm.get(ATTR_ID)));
				continue;
			}
			if (k.equals(ATTR_TYPE)) {
				bld.put(OSM_TYPE, new JsonPrimitive((String) osm.get(ATTR_TYPE)));
				continue;
			}
			bld.put(k, new JsonPrimitive(osm.get(k).toString()));
		}
		Map<String, Object> tagsValue = (Map<String, Object>) osm.get(F_TAGS);
		generateTagsForEntity(bld, tagsValue);
	}
	
	private int getOsmSourceIndex(String key) {
		String prefix = F_SOURCE + "." + F_OSM + "[";
		if (key.startsWith(prefix) && key.endsWith("]")) {
			String intInd = key.substring(prefix.length(), key.length() - 1);
			try {
				return Integer.parseInt(intInd);
			} catch (NumberFormatException ne) {
			}
		}
		return -1;
	}

	@SuppressWarnings("unchecked")
	private void generateEditedEntityFromOpObject(OpObject opObject, FeatureCollection featureCollection, OpBlock opBlock, String opHash) {
		Map<String, Object> change = opObject.getStringObjMap(F_CHANGE);
		Map<String, Object> current = opObject.getStringObjMap(F_CURRENT);

		for (String key : change.keySet()) {
			int ind = getOsmSourceIndex(key);
			if (ind != -1 && change.get(key).equals("delete")) {
				Map<String, Object> osm = (Map<String, Object>) current.get(key);
				ImmutableMap.Builder<String, JsonElement> bld = ImmutableMap.builder();

				bld.put(OSM_INDEX, new JsonPrimitive(ind));
				bld.put(TITLE, new JsonPrimitive(OBJ_REMOVED + " " + getTitle(osm)));
				bld.put(COLOR, new JsonPrimitive(COLOR_RED));
				bld.put(PLACE_TYPE, new JsonPrimitive((String) osm.get(OSM_VALUE)));
				generateAllFields(osm, bld);
				generateObjectBlockInfo(opObject, opBlock, opHash, bld);

				Feature f = new Feature(generatePoint(osm), bld.build(), Optional.absent());
				featureCollection.features().add(f);
			}
		}
	}


	private Point generatePoint(Map<String, Object> osm) {
		double lat = ((Number)osm.get(ATTR_LATITUDE)).doubleValue();
		double lon = ((Number)osm.get(ATTR_LONGITUDE)).doubleValue();
		return Point.from(lon, lat);
	}

	
	private void generateObjectBlockInfo(OpObject opObject, OpBlock opBlock, String opHash, ImmutableMap.Builder<String, JsonElement> bld) {
		bld.put(OPR_ID, new JsonPrimitive(generateStringId(opObject)));
		bld.put(BLOCK_TIMESTAMP, new JsonPrimitive(opBlock.getDateString()));
		bld.put(BLOCK_HASH, new JsonPrimitive(opBlock.getRawHash()));
		bld.put(BLOCK_ID, new JsonPrimitive(opBlock.getBlockId()));
		bld.put(OP_HASH, new JsonPrimitive(opHash));
	}

	private String generateStringId(OpObject opObject) {
		return opObject.getId().get(0) + "," + opObject.getId().get(1);
	}


	private void generateTagsForEntity(ImmutableMap.Builder<String, JsonElement> bld, Map<String, Object> tagsValue) {
		if (tagsValue != null) {
			JsonObject obj = new JsonObject();
			for (Map.Entry<String, Object> e : tagsValue.entrySet()) {
				obj.add(e.getKey(), new JsonPrimitive(e.getValue().toString()));
			}
			bld.put(F_TAGS, obj);
		}
	}

	private List<String> generateSearchStringKey(List<String> objId) {
		List<String> searchKey = new ArrayList<>();
		searchKey.add(OPR_PLACE);
		searchKey.addAll(objId);
		return searchKey;
	}

	@Override
	public MapCollection getContent(String date) {
		MapCollection fc = new MapCollection();
		fc.parameters.put(TYPE_DATE, TYPE_DATE);
		if(!date.equals("")) {
			try {
				retrievePlacesByDate(date, fc.geo);
			} catch (ParseException e) {
				LOGGER.error("Incorrect 'date' format", e);
			}
		}

		return fc;
	}

	@Override
	public String formatParams(Map<String, String[]> params) {
		String[] tls = params.get(TYPE_DATE);
		if(tls != null && tls.length == 1 && tls[0] != null) {
			return tls[0];
		}
		return "";
	}

	
	@Override
	public AbstractResource getMetaPage(Map<String, String[]> params) {
		return new InputStreamResource(OprSummaryPlaceDataProvider.class.getResourceAsStream("/mapall.html"));
	}
}
