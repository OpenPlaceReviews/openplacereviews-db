package org.openplacereviews.osm.service;

import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.util.OpExprEvaluator;
import org.openplacereviews.osm.util.OprExprEvaluatorExt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.openplacereviews.osm.service.PublishBot.*;

public class PlaceManager {

	private String table = "obj_opr_places";
	private String column = "osmid";
	private String field = "id";
	private BlocksManager blocksManager;
	private OpObject botObject;

	public PlaceManager(BlocksManager blocksManager, OpObject botObject) {
		this.blocksManager = blocksManager;
		this.botObject = botObject;
	}

	public OpObject getObjectByExtId(String extId) {
		List<OpObject> opObjectList = blocksManager.getObjectByIndex(table, column, field, extId);

		if (opObjectList != null && !opObjectList.isEmpty()) {
			return opObjectList.get(0);
		} else {
			return null;
		}
	}

	public String generateMatchIdFromOpObject(OpObject opObject) {
		Map<String, Object> ctx = new HashMap<>();
		for (Map.Entry<String, String> entry : botObject.getStringMap(F_MAPPING).entrySet()) {
			ctx.put(entry.getKey(), opObject.getFieldByExpr(entry.getValue()));
		}
		OpExprEvaluator.EvaluationContext evaluationContext = new OpExprEvaluator.EvaluationContext(
				null,
				blocksManager.getBlockchain().getRules().getFormatter().toJsonElement(ctx).getAsJsonObject(),
				blocksManager.getBlockchain().getRules().getFormatter().toJsonElement(opObject).getAsJsonObject(),
				null,
				null
		);

		for (Map.Entry<String, String> entry : botObject.getStringMap(F_RULES).entrySet()) {
			if (entry.getKey().equals(F_MATCH_ID)) {
				return OprExprEvaluatorExt.parseExpression(entry.getValue()).evaluateObject(evaluationContext).toString();
			}
		}

		return null;
	}

}
