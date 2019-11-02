package org.openplacereviews.controllers;

import com.github.filosganga.geogson.model.Feature;
import com.github.filosganga.geogson.model.FeatureCollection;
import com.github.filosganga.geogson.model.Point;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.openplacereviews.controllers.MapCollection.TYPE_DATE;
import static org.openplacereviews.opendb.ops.OpObject.F_CHANGE;
import static org.openplacereviews.opendb.ops.OpObject.F_CURRENT;
import static org.openplacereviews.opendb.service.HistoryManager.DESC_SORT;
import static org.openplacereviews.opendb.service.HistoryManager.HISTORY_BY_OBJECT;
import static org.openplacereviews.osm.model.Entity.*;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.*;

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

	private static final String SOURCE_OSM_REGEX = "source.osm\\[\\d+]";
	private static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+");
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

	// constants ? or human strings?
	public static final String OBJ_CREATED = "Created";
	public static final String OBJ_EDITED = "Edited";
	public static final String OBJ_REMOVED = "Removed";

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
						generateEntity(fc, block, opOperation.getRawHash(), opObject, OBJ_CREATED, "green");
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
			generateEntity(fc, opBlock, opHash, opObject, OBJ_REMOVED, "red");
		} else {
			// TODO we does not have a lat/lon for removed object -> only objId
//			double lat = 0;
//			double lon = 0;
//			Point p = Point.from(lon, lat);
//			Feature f = Feature.of(p).withProperty(TITLE, new JsonPrimitive(OBJ_REMOVED));
//			fc.features().add(f);
		}
	}

	private void generateEntity(FeatureCollection fc, OpBlock opBlock, String opHash, OpObject opObject, 
			String status, String color) {
		List<Map<String, Object>> osmList = opObject.getField(null, F_SOURCE, F_OSM);
		for (int i = 0; i < osmList.size(); i++) {
			Map<String, Object> osm = osmList.get(i);
			double lat = (double) osm.get(ATTR_LATITUDE);
			double lon = (double) osm.get(ATTR_LONGITUDE);
			Point p = Point.from(lon, lat);
			ImmutableMap.Builder<String, JsonElement> bld = ImmutableMap.builder();
			bld.put(OSM_INDEX, new JsonPrimitive(i));
			bld.put(TITLE, new JsonPrimitive(status + " " + getTitle(osm)));
			bld.put(COLOR, new JsonPrimitive(color));
			generateAllFields(osm, bld);
			generateObjectBlockInfo(opObject, opBlock, opHash, bld);

			Feature f = new Feature(p, bld.build(), Optional.absent());
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

	@SuppressWarnings("unchecked")
	private void generateEditedEntityFromOpObject(OpObject opObject, FeatureCollection featureCollection, OpBlock opBlock, String opHash) {
		Map<String, Object> change = opObject.getStringObjMap(F_CHANGE);
		Map<String, Object> current = opObject.getStringObjMap(F_CURRENT);

		// TODO remove regex
		if (change.size() == 2 && (((String) change.keySet().toArray()[1]).matches(SOURCE_OSM_REGEX))) {
			for (String key : current.keySet()) {
				if (key.matches(SOURCE_OSM_REGEX)) {
					Map<String, Object> osm = (Map<String, Object>) current.get(key);
					double lat = (double) osm.get(ATTR_LATITUDE);
					double lon = (double) osm.get(ATTR_LONGITUDE);
					Point p = Point.from(lon, lat);
					ImmutableMap.Builder<String, JsonElement> bld = ImmutableMap.builder();

					bld.put(OSM_INDEX, new JsonPrimitive(getOsmIndexFromStringKey(key)));
					bld.put(TITLE, new JsonPrimitive(OBJ_REMOVED));
					generateAllFields(osm, bld);
					generateObjectBlockInfo(opObject, opBlock, opHash, bld);

					Feature f = new Feature(p, bld.build(), Optional.absent());
					featureCollection.features().add(f);
				}
			}
		} else {
			// TODO updated generating edited objects
			Set<Integer> osmIds = new HashSet<>();
			for (String key : current.keySet()) {
				osmIds.add(getOsmIndexFromStringKey(key));
			}
			OpObject originObject = blocksManager.getBlockchain().getObjectByName(OPR_PLACE, opObject.getId());
			List<Map<String, Object>> objects = originObject.getField(null, "source", "osm");
			for (Integer id : osmIds) {
				if (objects.size() > id) {
					Map<String, Object> osm = objects.get(id);
					double lat = (double) osm.get(ATTR_LATITUDE);
					double lon = (double) osm.get(ATTR_LONGITUDE);
					Point p = Point.from(lon, lat);
					ImmutableMap.Builder<String, JsonElement> bld = ImmutableMap.builder();

					bld.put(OSM_INDEX, new JsonPrimitive(id));
					bld.put(TITLE, new JsonPrimitive(OBJ_EDITED + " " + getTitle(osm)));
					bld.put(OSM_ID, new JsonPrimitive((long) osm.get(ATTR_ID)));
					bld.put(OSM_TYPE, new JsonPrimitive((String) osm.get(ATTR_TYPE)));
					generateObjectBlockInfo(opObject, opBlock, opHash, bld);

					JsonObject prev = new JsonObject();
					for (String k : osm.keySet()) {
						if (k.equals(F_TAGS)) {
							continue;
						}
						prev.addProperty(k, osm.get(k).toString());
					}
					Map<String, Object> tagsValue = (Map<String, Object>) osm.get(F_TAGS);
					if (tagsValue != null) {
						JsonObject obj = new JsonObject();
						for (Map.Entry<String, Object> e : tagsValue.entrySet()) {
							obj.add(e.getKey(), new JsonPrimitive(e.getValue().toString()));
						}
						prev.add(F_TAGS, obj);
					}
					JsonObject next = new JsonObject();

					bld.put(OBJ_PREV, prev);
					bld.put(OBJ_NEXT, next);

					Feature f = new Feature(p, bld.build(), Optional.absent());
					featureCollection.features().add(f);
				}
			}
		}

	}

	private Integer getOsmIndexFromStringKey(String key) {
		Matcher m = NUMBER_PATTERN.matcher(key);
		m.find();
		return Integer.valueOf(m.group(0));
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
