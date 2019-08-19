package org.openplacereviews.osm.service;

import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OpExprEvaluator;
import org.openplacereviews.osm.util.OprExprEvaluatorExt;

import java.util.HashMap;
import java.util.Map;

import static org.openplacereviews.osm.service.OsmSyncBot.F_CONFIG;

public class PlaceManager {

	public static final String F_MATCH_ID = "match-id";
	public static final String F_MAPPING = "mapping";

	private final String index = "osmid";
	private String opType;
	private BlocksManager blocksManager;
	private OpObject botObject;

	public PlaceManager(String opType, BlocksManager blocksManager, OpObject botObject) {
		this.blocksManager = blocksManager;
		this.botObject = botObject;
		this.opType = opType;
	}

	public OpObject getObjectByExtId(Long extId) {
		OpBlockChain.ObjectsSearchRequest objectsSearchRequest = new OpBlockChain.ObjectsSearchRequest();
		objectsSearchRequest.limit = 1;
		blocksManager.getObjectsByIndex(opType, index, objectsSearchRequest,  extId);

		if (objectsSearchRequest.result != null && !objectsSearchRequest.result.isEmpty()) {
			return objectsSearchRequest.result.get(0);
		} else {
			return null;
		}
	}

	public String generateMatchIdFromOpObject(OpObject opObject) {
		Map<String, Object> ctx = new HashMap<>();
		for (Map.Entry<String, String> entry : ((Map<String, String>)botObject.getStringObjMap(F_CONFIG).get(F_MAPPING)).entrySet()) {
			ctx.put(entry.getKey(), opObject.getFieldByExpr(entry.getValue()));
		}
		JsonFormatter formatter = blocksManager.getBlockchain().getRules().getFormatter();
		OpExprEvaluator.EvaluationContext evaluationContext = new OpExprEvaluator.EvaluationContext(
				null,
				formatter.toJsonElement(ctx).getAsJsonObject(),
				formatter.toJsonElement(opObject).getAsJsonObject(),
				null,
				null
		);

		String matchId = botObject.getStringMap(F_CONFIG).get(F_MATCH_ID);
		return OprExprEvaluatorExt.parseExpression(matchId).evaluateObject(evaluationContext).toString();
	}

}
