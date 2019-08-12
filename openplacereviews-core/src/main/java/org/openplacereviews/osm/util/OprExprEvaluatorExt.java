package org.openplacereviews.osm.util;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.openplacereviews.opendb.expr.OpenDBExprLexer;
import org.openplacereviews.opendb.expr.OpenDBExprParser;
import org.openplacereviews.opendb.util.OpExprEvaluator;
import org.openplacereviews.osm.parser.OsmLocationTool;

import javax.activation.UnsupportedDataTypeException;
import java.util.List;

public class OprExprEvaluatorExt extends OpExprEvaluator {

	public static final String FUNCTION_FIRST_NOT_EMPTY = "str:first_non_empty";

	public static final String FUNCTION_PLACE_LOCATION = "opr:place_loc";
	public static final String FUNCTION_SIMPLE_NAME = "opr:simple_name";

	public OprExprEvaluatorExt(OpenDBExprParser.ExpressionContext ectx) {
		super(ectx);
	}

	public static OpExprEvaluator parseExpression(String value) throws RecognitionException {
		OpenDBExprLexer lexer = new OpenDBExprLexer(new ANTLRInputStream(value));
		ThrowingErrorListener twt = new ThrowingErrorListener(value);
		lexer.removeErrorListeners();
		lexer.addErrorListener(twt);
		OpenDBExprParser parser = new OpenDBExprParser(new CommonTokenStream(lexer));
		parser.removeErrorListeners();
		parser.addErrorListener(twt);
		OpenDBExprParser.ExpressionContext ectx = parser.expression();
		return new OprExprEvaluatorExt(ectx);
	}

	@Override
	protected Object callFunction(String functionName, List<Object> args, EvaluationContext ctx) {
		Object obj1, obj2, obj3;
		switch (functionName) {
		case FUNCTION_FIRST_NOT_EMPTY: {
			StringBuilder str = new StringBuilder();
			for (int i = 0; i < args.size(); i++) {
				String obj = getStringArgument(functionName, args, i);
				if (obj != null) {
					int indexOf = obj.indexOf(";");
					if (indexOf != -1) {
						str.append(obj, 0, indexOf);
					} else {
						str.append(obj);
					}
				}
			}

			if (str.length() == 0) {
				return null;
			}
			return str.toString();
		}
		case FUNCTION_PLACE_LOCATION: {
			obj1 = getObjArgument(functionName, args, 0, false);
			obj2 = getObjArgument(functionName, args, 1, false);
			obj3 = getObjArgument(functionName, args, 1, false);

			if (obj1 instanceof Number && obj2 instanceof Number && obj2 instanceof Number) {
				return OsmLocationTool.encode((double) obj1, (double) obj2, ((Long) obj3).intValue());
			}
			throw new UnsupportedOperationException(FUNCTION_PLACE_LOCATION + "support only Numbers");
		}
		case FUNCTION_SIMPLE_NAME: {
			obj1 = getStringArgument(functionName, args, 0);

			if (obj1 == null) {
				return null;
			}

			return ((String) obj1).replaceAll("[ -]", "").toLowerCase();
		}
		default: {
			return super.callFunction(functionName, args, ctx);
		}
		}
	}

}
