package org.openplacereviews.controllers;

import com.github.filosganga.geogson.model.Feature;
import com.github.filosganga.geogson.model.FeatureCollection;
import com.github.filosganga.geogson.model.Point;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
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
import org.springframework.security.core.parameters.P;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.openplacereviews.opendb.ops.OpObject.F_CHANGE;
import static org.openplacereviews.opendb.ops.OpObject.F_CURRENT;
import static org.openplacereviews.opendb.ops.OpObject.F_ID;
import static org.openplacereviews.opendb.ops.OpOperation.F_TYPE;
import static org.openplacereviews.opendb.service.HistoryManager.DESC_SORT;
import static org.openplacereviews.opendb.service.HistoryManager.HISTORY_BY_OBJECT;
import static org.openplacereviews.osm.model.Entity.ATTR_LATITUDE;
import static org.openplacereviews.osm.model.Entity.ATTR_LONGITUDE;
import static org.openplacereviews.osm.util.PlaceOpObjectHelper.*;

public class OprHistoryChangesProvider extends OprPlaceDataProvider {

	private static final Log LOGGER = LogFactory.getLog(ApiController.class);

	public static final String PARAM_OSM_DATE = "date";

	public static final String OSM_ID = "osm_id";
	public static final String OSM_TYPE = "osm_type";
	public static final String OSM_INDEX = "osm_index";

	public static final String BLOCK_TIMESTAMP = "block_timestamp";
	public static final String BLOCK_HASH = "block_hash";
	public static final String BLOCK_ID = "block_id";
	public static final String OP_HASH = "op_hash";
	public static final String OPR_ID = "opr_id";
	public static final String OPR_PLACE = "opr.place";

	public static final String OBJ_CREATED = "Created";
	public static final String OBJ_EDITED = "Edited";
	public static final String OBJ_REMOVED = "Removed";

	private static final String SOURCE_OSM_REGEX = "source.osm\\[\\d+]";

	@Autowired
	private HistoryManager historyManager;

	public void getOsmObjectByDate(String stringDate, FeatureCollection featureCollection) throws ParseException {
		Date date = new SimpleDateFormat("yyyy-MM-dd").parse(stringDate);
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
					for (OpObject opObject : opOperation.getEdited()) {
						generateEditedEntityFromOpObject(opObject, featureCollection);
					}
					for (List<String> objId : opOperation.getDeleted()) {
						generateRemovedEntityFromOpObject(objId, featureCollection);
					}
				}
			}
		}

	}

	@SuppressWarnings("unchecked")
	private void generateCreateEntityFromOpObject(OpObject opObject, FeatureCollection featureCollection, OpBlock opBlock, String opHash) {
		List<Map<String, Object>> osmList = opObject.getField(null, F_SOURCE, F_OSM);
		for (Map<String, Object> osm : osmList) {
			double lat = (double) osm.get(ATTR_LATITUDE);
			double lon = (double) osm.get(ATTR_LONGITUDE);
			Point p = Point.from(lon, lat);
			ImmutableMap.Builder<String, JsonElement> bld = ImmutableMap.builder();

			bld.put(OSM_ID, new JsonPrimitive((Number) osm.get(OpObject.F_ID)));
			bld.put(OSM_TYPE, new JsonPrimitive((String) osm.get(F_TYPE)));

			Map<String, Object> tagsValue = (Map<String, Object>) osm.get(F_TAGS);
			generateTagsForEntity(bld, tagsValue);

			bld.put(OPR_ID, new JsonPrimitive(opObject.getId().get(0) + "," + opObject.getId().get(1)));
			bld.put(OSM_INDEX, new JsonPrimitive(0));
			bld.put(BLOCK_TIMESTAMP, new JsonPrimitive(opBlock.getDateString()));
			bld.put(BLOCK_HASH, new JsonPrimitive(opBlock.getRawHash()));
			bld.put(BLOCK_ID, new JsonPrimitive(opBlock.getBlockId()));
			bld.put(OP_HASH, new JsonPrimitive(opHash));
			bld.put(ATTR_LATITUDE, new JsonPrimitive(lat));
			bld.put(ATTR_LONGITUDE, new JsonPrimitive(lon));
			bld.put(F_VERSION, new JsonPrimitive((String) osm.get(F_VERSION)));
			bld.put(F_CHANGESET, new JsonPrimitive((String) osm.get(F_CHANGESET)));

			Feature f = new Feature(p, bld.build(), Optional.absent()).withId(OBJ_CREATED);
			featureCollection.features().add(f);
		}
	}

	@SuppressWarnings("unchecked")
	private void generateRemovedEntityFromOpObject(List<String> objId, FeatureCollection featureCollection) {
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
		List<Map<String, Object>> osmList = opObject.getField(null, F_SOURCE, F_OSM);
		for (Map<String, Object> osm : osmList) {
			double lat = (double) osm.get(ATTR_LATITUDE);
			double lon = (double) osm.get(ATTR_LONGITUDE);
			Point p = Point.from(lon, lat);
			ImmutableMap.Builder<String, JsonElement> bld = ImmutableMap.builder();

			bld.put(OPR_ID, new JsonPrimitive(opObject.getId().get(0) + "," + opObject.getId().get(1)));
			for (String k : osm.keySet()) {
				if (k.equals(F_TAGS)) {
					continue;
				}
				bld.put(k, new JsonPrimitive(osm.get(k).toString()));
			}
			Map<String, Object> tagsValue = (Map<String, Object>) osm.get(F_TAGS);
			generateTagsForEntity(bld, tagsValue);

			Feature f = new Feature(p, bld.build(), Optional.absent()).withId(OBJ_REMOVED);
			featureCollection.features().add(f);
		}

	}

	@SuppressWarnings("unchecked")
	private void generateEditedEntityFromOpObject(OpObject opObject, FeatureCollection featureCollection) {
		Map<String, Object> change = opObject.getStringObjMap(F_CHANGE);
		Map<String, Object> current = opObject.getStringObjMap(F_CURRENT);

		if (change.size() == 2 && (((String) change.keySet().toArray()[1]).matches(SOURCE_OSM_REGEX))) {
			for (String key : current.keySet()) {
				if (key.matches(SOURCE_OSM_REGEX)) {
					Map<String, Object> osm = (Map<String, Object>) current.get(key);
					double lat = (double) osm.get(ATTR_LATITUDE);
					double lon = (double) osm.get(ATTR_LONGITUDE);
					Point p = Point.from(lon, lat);
					ImmutableMap.Builder<String, JsonElement> bld = ImmutableMap.builder();

					bld.put(OPR_ID, new JsonPrimitive(opObject.getId().get(0) + "," + opObject.getId().get(1)));
					for (String k : osm.keySet()) {
						if (k.equals(F_TAGS)) {
							continue;
						}
						if (k.equals(F_ID)) {
							bld.put(OSM_ID, new JsonPrimitive((Number) osm.get(k)));
							continue;
						}
						if (k.equals(F_TYPE)) {
							bld.put(OSM_TYPE, new JsonPrimitive(osm.get(k).toString()));
							continue;
						}
						bld.put(k, new JsonPrimitive(osm.get(k).toString()));
					}
					Map<String, Object> tagsValue = (Map<String, Object>) osm.get(F_TAGS);
					generateTagsForEntity(bld, tagsValue);

					Feature f = new Feature(p, bld.build(), Optional.absent()).withId(OBJ_REMOVED);
					featureCollection.features().add(f);
				}
			}
		} else {
			// TODO create edit entity
		}

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
	public FeatureCollection getContent(String date) {
		FeatureCollection fc = new FeatureCollection(new ArrayList<>());
		if(!date.equals("")) {
			try {
				getOsmObjectByDate(date, fc);
			} catch (ParseException e) {
				LOGGER.error("Incorrect 'date' format", e);
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

	
	@Override
	public AbstractResource getMetaPage(Map<String, String[]> params) {
		return new InputStreamResource(OprSummaryPlaceDataProvider.class.getResourceAsStream("/mapall.html"));
	}
}
