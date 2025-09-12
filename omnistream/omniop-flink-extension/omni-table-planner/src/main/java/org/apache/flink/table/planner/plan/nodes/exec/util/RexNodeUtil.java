package org.apache.flink.table.planner.plan.nodes.exec.util;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RexNodeUtil {
    public static final Map<String, Integer> RexTypeToIdMap = new HashMap<>();
    public static final Map<String, SpecialExprType> specialOperatorMap = new HashMap<>();
    public static final Map<String, UnaryExprType> unaryOperatorMap = new HashMap<>();
    public static final Map<String, BinaryExprType> binaryOperatorMap = new HashMap<>();
    private static final Logger LOG = LoggerFactory.getLogger(RexNodeUtil.class);
    public static HashMap<Integer, Integer> accessIndexMap = new HashMap<>();

    static {
        // This map converts Calcite RexNode SqlTypeName to OmniType Id
        RexTypeToIdMap.put("INT", 1);
        RexTypeToIdMap.put("INTEGER", 1);
        RexTypeToIdMap.put("BIGINT", 2);
        RexTypeToIdMap.put("DOUBLE", 3);
        RexTypeToIdMap.put("BOOLEAN", 4);
        RexTypeToIdMap.put("TINYINT", 5);
        RexTypeToIdMap.put("SMALLINT", 5);
        RexTypeToIdMap.put("DECIMAL", 6);
        RexTypeToIdMap.put("DECIMAL128", 7);
        RexTypeToIdMap.put("DATE", 8);
        RexTypeToIdMap.put("TIME", 10);
        RexTypeToIdMap.put("TIMESTAMP", 12);
        RexTypeToIdMap.put("INTERVAL_MONTH", 13);
        RexTypeToIdMap.put("INTERVAL_DAY", 14);
        RexTypeToIdMap.put("INTERVAL_SECOND", 14);
        RexTypeToIdMap.put("VARCHAR", 15);
        RexTypeToIdMap.put("CHAR", 16);
        RexTypeToIdMap.put("ROW", 17);
        RexTypeToIdMap.put("INVALID", 18);
        RexTypeToIdMap.put("TIME_WITHOUT_TIME_ZONE", 19); // TODO: Is this the same as TIME?
        RexTypeToIdMap.put("TIMESTAMP_WITHOUT_TIME_ZONE", 20);//TODO: Omni's TIMESTAMP uses int64_t, Flink has the possibility of accuracy>3
        RexTypeToIdMap.put("TIMESTAMP_TZ", 21);//TIMESTAMP_WITH_TIMEZONE
        RexTypeToIdMap.put("TIMESTAMP_WITH_LOCAL_TIME_ZONE", 22);
        RexTypeToIdMap.put("ARRAY", 23);
        RexTypeToIdMap.put("MULTISET", 24);
        RexTypeToIdMap.put("MAP", 25);
    }

    static {
        specialOperatorMap.put("CASE", SpecialExprType.SWITCH);
        specialOperatorMap.put("REGEXP_EXTRACT", SpecialExprType.REGEXP_EXTRACT);
        specialOperatorMap.put("SPLIT_INDEX", SpecialExprType.SPLIT_INDEX);
        specialOperatorMap.put("count_char", SpecialExprType.COUNT_CHAR);
        specialOperatorMap.put("SEARCH", SpecialExprType.SEARCH);
        specialOperatorMap.put("LOWER", SpecialExprType.LOWER);
        specialOperatorMap.put("HASH_CODE", SpecialExprType.HASH_CODE);
        specialOperatorMap.put("IS NOT NULL", SpecialExprType.IS_NOT_NULL);
        specialOperatorMap.put("PROCTIME_MATERIALIZE", SpecialExprType.PROCTIME);
        specialOperatorMap.put("PROCTIME", SpecialExprType.PROCTIME);
        specialOperatorMap.put("EXTRACT", SpecialExprType.EXTRACT);
        specialOperatorMap.put("DATE_FORMAT", SpecialExprType.DATE_FORMAT);
        specialOperatorMap.put("CAST", SpecialExprType.CAST);
        specialOperatorMap.put("AND", SpecialExprType.AND);
        specialOperatorMap.put("OR", SpecialExprType.OR);
        specialOperatorMap.put("IF", SpecialExprType.IF);
    }

    static {
        unaryOperatorMap.put("-", UnaryExprType.NEGATION);
        unaryOperatorMap.put("IS TRUE", UnaryExprType.IS_TRUE);
    }

    static {
        binaryOperatorMap.put("OR", BinaryExprType.OR);
        binaryOperatorMap.put("AND", BinaryExprType.AND);
        binaryOperatorMap.put("+", BinaryExprType.ADD);
        binaryOperatorMap.put("-", BinaryExprType.SUBTRACT);
        binaryOperatorMap.put("*", BinaryExprType.MULTIPLY);
        binaryOperatorMap.put("/", BinaryExprType.DIVIDE);
        binaryOperatorMap.put("MOD", BinaryExprType.MODULUS);
        binaryOperatorMap.put(">", BinaryExprType.GREATER_THAN);
        binaryOperatorMap.put(">=", BinaryExprType.GREATER_THAN_OR_EQUAL);
        binaryOperatorMap.put("<", BinaryExprType.LESS_THAN);
        binaryOperatorMap.put("<=", BinaryExprType.LESS_THAN_OR_EQUAL);
        binaryOperatorMap.put("=", BinaryExprType.EQUAL);
        binaryOperatorMap.put("<>", BinaryExprType.NOT_EQUAL);
    }

    public enum OperatorExprType {
        FIELD_REFERENCE,
        FIELD_ACCESS, // we can parse it, but currently we dont support it in operator-level operations.
        LITERAL,
        UNARY,
        BINARY,
        SPECIAL,
        INVALID
    }

    public enum UnaryExprType {
        NEGATION,
        IS_TRUE
    }


    public enum BinaryExprType {
        //binary logical operators
        OR,
        AND,
        //Arithmetic
        ADD,
        SUBTRACT,
        MULTIPLY,
        DIVIDE,
        MODULUS,
        //Comparison
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        EQUAL,
        NOT_EQUAL
    }

    public enum SpecialExprType {
        SWITCH,
        REGEXP_EXTRACT,
        SEARCH,
        LOWER,
        HASH_CODE,
        SPLIT_INDEX,
        IS_NOT_NULL,
        PROCTIME,
        EXTRACT,
        DATE_FORMAT,
        COUNT_CHAR,
        CAST,
        OTHERS,
        AND,
        OR,
        IF
    }


    public static Object[] getExpression(RexNode inputExpr) {
        RexNode input = null;
        RexNode value = null;
        if (inputExpr instanceof RexCall) {
            RexCall call = (RexCall) inputExpr;
            LOG.info("Current RexCall name {}", call.getOperator().getName());
            if (binaryOperatorMap.containsKey(call.getOperator().getName())
                    && (binaryOperatorMap.get(call.getOperator().getName()) == BinaryExprType.EQUAL
                    || binaryOperatorMap.get(call.getOperator().getName()) == BinaryExprType.AND)) {
                input = call.operands.get(0);
                value = call.operands.get(1);
                return new Object[]{input, value};
            } else {
                LOG.info("Cannot parse the expression of a CASE operation");
            }
        } else {
            LOG.info("Cannot parse the expression of a CASE operation");
        }
        return new Object[]{input, value};
    }

    public static void setDataType(RexNode rexNode, Map<String, Object> jsonMap, String keyStr) {
        if (rexNode.getType().getSqlTypeName() == SqlTypeName.DECIMAL) {
            // check for decimal precision
            int precision = rexNode.getType().getPrecision();
            if (precision < 19) {
                jsonMap.put(keyStr, 6);
            } else {
                jsonMap.put(keyStr, 7);
            }
            jsonMap.put("precision", precision);
            jsonMap.put("scale", rexNode.getType().getScale());
        } else if (rexNode.getType().getSqlTypeName() == SqlTypeName.CHAR
                || rexNode.getType().getSqlTypeName() == SqlTypeName.VARCHAR) {
            int precision = rexNode.getType().getPrecision();
            jsonMap.put(keyStr, RexTypeToIdMap.get(rexNode.getType().getSqlTypeName().toString()));
            jsonMap.put("width", precision);
        } else if (SqlTypeName.DATETIME_TYPES.contains(rexNode.getType().getSqlTypeName())) {
            jsonMap.put(keyStr, 2);
        } else if (SqlTypeName.INTERVAL_TYPES.contains(rexNode.getType().getSqlTypeName())) {
            jsonMap.put(keyStr, 1);
        } else {
            jsonMap.put(keyStr, RexTypeToIdMap.get(rexNode.getType().getSqlTypeName().toString()));
        }
    }

    public static Map<String, Object> buildJsonMap(RexNode rexNode) {
        Map<String, Object> jsonMap = new LinkedHashMap<>();
        if (rexNode instanceof RexCall) {
            RexCall rexCall = (RexCall) rexNode;
            List<RexNode> operands = rexCall.getOperands();
            int numOperands = operands.size();
            SqlOperator operator = rexCall.getOperator();
            LOG.info("Current rexNode is {}", rexCall.toString());
            if (rexCall.operands.size() == 2 && binaryOperatorMap.containsKey(operator.getName())) {
                BinaryExprType binaryType = binaryOperatorMap.get(operator.getName());
                jsonMap.put("exprType",  OperatorExprType.BINARY.name());
                setDataType(rexCall,jsonMap, "returnType");
                jsonMap.put("operator", binaryType.name());
                Map<String, Object> leftMap = buildJsonMap(operands.get(0));
                jsonMap.put("left", leftMap);
                Map<String, Object> rightMap =  buildJsonMap(operands.get(1));
                jsonMap.put("right", rightMap);
                return jsonMap;
            } else if (rexCall.operands.size() == 1 && unaryOperatorMap.containsKey(operator.getName())) {
                Map<String, Object> childMap = buildJsonMap(operands.get(0));
                Integer returnType = RexTypeToIdMap.get(rexCall.getType().getSqlTypeName().toString());
                Integer childDataType = (Integer) childMap.getOrDefault("dataType", -1);
                if (childMap.containsKey("returnType")) {
                    childDataType = (Integer) childMap.get("returnType");
                }
                UnaryExprType unaryType = unaryOperatorMap.get(operator.getName());
                if (unaryType.equals(UnaryExprType.IS_TRUE) ||
                        childMap.containsKey("exprType") && childMap.get("exprType").equals(SpecialExprType.SWITCH)) {
                    return childMap;
                }
                jsonMap.put("exprType",  OperatorExprType.UNARY.name());
                setDataType(rexCall,jsonMap, "returnType");
                jsonMap.put("operator", unaryType.name());
                jsonMap.put("expr", childMap);
                return jsonMap;
            } else if (specialOperatorMap.containsKey(operator.getName())) {
                SpecialExprType specialType = specialOperatorMap.get(operator.getName());
                switch (specialType){
			/*
                    case SWITCH:
                        jsonMap.put("exprType",  specialType.SWITCH);
                        jsonMap.put("returnType", RexTypeToIdMap.get(rexCall.getType().getSqlTypeName().toString()));
                        int numOfCases = (numOperands-1)/2;
                        jsonMap.put("numOfCases", numOfCases);
                        RexNode input = null;
                        for(int i = 1;i<= numOfCases;i++){
                            int index = (i-1)*2;
                            Object[] expr = getExpression(operands.get(index));
                            if (input != null && (!input.equals(expr[0]))){
                                //if the input expressions do not match each others
                                LOG.info("Cannot parse the CASE expression.");
                                break;
                            }else{
                                input = (RexNode) expr[0];
                                String key= "Case"+Integer.toString(i);
                                Map<String, Object> valueMap = buildJsonMap((RexNode) expr[1]);
                                Map<String, Object> resultMap = buildJsonMap((RexNode) operands.get(index+1));
                                Map<String, Object> whenJsonMap = new LinkedHashMap<>();
                                whenJsonMap.put("exprType","WHEN" );
                                whenJsonMap.put("returnType", RexTypeToIdMap.get(rexCall.getType().getSqlTypeName().toString()));
                                whenJsonMap.put("when", valueMap);
                                whenJsonMap.put("result", resultMap);
                                jsonMap.put(key, whenJsonMap);
                            }

                        }
                        Map<String, Object> inputMap = buildJsonMap((RexNode) input);
                        jsonMap.put("input", inputMap);
                        Map<String, Object> defaultMap = buildJsonMap((RexNode)operands.get(numOperands-1));
                        jsonMap.put("else", defaultMap);
                        LOG.info("The expression is {} ", rexCall.toString());
                        break;
                    */
                    case SWITCH: // This is the more general switch
                    case IF:
                        jsonMap.put("exprType",  "SWITCH_GENERAL");
                        setDataType(rexCall,jsonMap, "returnType");
                        List<RexNode> switchOperands = rexCall.getOperands();
                        // SWITCH([input_expr, condition1], 'result1', [input_expr, condition2], 'result2', 'optional other')
                        int numOfCases = (numOperands - 1) / 2;
                        jsonMap.put("numOfCases", numOfCases);

                        //jsonMap.put("input", buildJsonMap(operands.get(0)));

                        for (int i = 0; i < numOfCases; i++) {
                            Map<String, Object> caseMap = new LinkedHashMap<>();
                            caseMap.put("when", buildJsonMap(operands.get(i * 2)));
                            caseMap.put("result", buildJsonMap(operands.get(i * 2 + 1)));
                            caseMap.put("exprType", "WHEN");
                            setDataType(operands.get(i * 2 + 1), caseMap, "returnType");
                            jsonMap.put("Case"+Integer.toString(i + 1), caseMap);
                        }
                        // The else
                        jsonMap.put("else", buildJsonMap(operands.get(operands.size() - 1)));
                        break;
                    case REGEXP_EXTRACT:
                        jsonMap.put("exprType", "FUNCTION");
                        setDataType(rexCall,jsonMap, "returnType");
                        jsonMap.put("function_name", "regex_extract_null");

                        List<Map<String, Object>> regArgs = new ArrayList<>();
                        regArgs.add(buildJsonMap(operands.get(0)));
                        regArgs.add(buildJsonMap(operands.get(1)));
                        regArgs.add(buildJsonMap(operands.get(2)));
                        jsonMap.put("arguments", regArgs);
                        LOG.info("The expression is {} ", rexCall.toString());
                        break;
                    case SPLIT_INDEX:
                        jsonMap.put("exprType", "FUNCTION");
                        // todo:check VARCHAR(length) mapping to returnTypeID
                        setDataType(rexCall,jsonMap, "returnType");
                        jsonMap.put("function_name", "SplitIndex");

                        List<Map<String, Object>> stringList = new ArrayList<>();
                        stringList.add(buildJsonMap(operands.get(0)));
                        stringList.add(buildJsonMap(operands.get(1)));
                        stringList.add(buildJsonMap(operands.get(2)));

                        LOG.info("List is {}", stringList.toString());
                        jsonMap.put("arguments", stringList);
                        LOG.info("The expresssion is {} ", rexCall.toString());
                        break;
                    case COUNT_CHAR:
                        jsonMap.put("exprType", "FUNCTION");
                        setDataType(rexCall, jsonMap, "returnType");
                        jsonMap.put("function_name", "CountChar");

                        stringList = new ArrayList<>();
                        stringList.add(buildJsonMap(operands.get(0)));
                        stringList.add(buildJsonMap(operands.get(1)));

                        LOG.info("List is {}", stringList.toString());
                        jsonMap.put("arguments", stringList);
                        LOG.info("The expresssion is {} ", rexCall.toString());
                        break;
                    case SEARCH:
                        RexLiteral searchArg = (RexLiteral) operands.get(1);  // Sarg object
                        // Get the Sarg value
                        Sarg<?> sarg = ((RexLiteral) searchArg).getValueAs(Sarg.class);
                        // Check if its a list or a range
                        if (sarg.isPoints()) { // A point list
                            jsonMap.put("exprType", "IN");
                            setDataType(rexCall,jsonMap, "returnType");

                            stringList = new ArrayList<>();
                            // first argument is the input
                            stringList.add(buildJsonMap(operands.get(0)));
                            // Extract all elements from the list
                            List<?> values = new ArrayList<>(sarg.rangeSet.asRanges().stream()
                                    .map(range -> range.lowerEndpoint()).collect(Collectors.toList()));

                            for(int i = 0; i < values.size(); i++) {
                                Map<String, Object> literalMap = new LinkedHashMap<>();
                                literalMap.put("exprType", "LITERAL");
                                literalMap.put("isNull", false);
                                if (values.get(i) instanceof NlsString) {
                                    String endpoint = ((NlsString) values.get(i)).getValue();
                                    literalMap.put("value", endpoint);
                                    setDataType(operands.get(0), literalMap, "dataType");
                                } else {
                                    literalMap.put("value", values.get(i));
                                    setDataType(operands.get(0), literalMap, "dataType");
                                }
                                stringList.add(literalMap);
                            }
                            jsonMap.put("arguments", stringList);
                        } else { // a range
                            jsonMap.put("exprType", "BETWEEN");
                            setDataType(rexCall,jsonMap, "returnType");
                            jsonMap.put("value", buildJsonMap(operands.get(0)));
                            // TODO: inclusive, exclusive problem not solved!

                            // Extract lower and upper bounds
                            Object lowerBound = sarg.rangeSet.asRanges().iterator().next().lowerEndpoint();
                            Object upperBound = sarg.rangeSet.asRanges().iterator().next().upperEndpoint();

                            Map<String, Object> lowMap = new LinkedHashMap<>();
                            lowMap.put("exprType", "LITERAL");
                            lowMap.put("isNull", false);
                            lowMap.put("value", lowerBound);
                            setDataType(operands.get(0), lowMap, "dataType");
                            jsonMap.put("lower_bound", lowMap);

                            Map<String, Object> upMap = new LinkedHashMap<>();
                            upMap.put("exprType", "LITERAL");
                            upMap.put("isNull", false);
                            upMap.put("value", upperBound);
                            setDataType(operands.get(0), upMap, "dataType");
                            jsonMap.put("upper_bound", upMap);
                        }
                        break;
                    case HASH_CODE:
                        jsonMap.put("exprType", "FUNCTION");
                        Integer returnType = RexTypeToIdMap.get(rexCall.getType().getSqlTypeName().toString());
                        if (returnType == 1){
                            jsonMap.put("function_name", "mm3hash");
                        }
                        else {
                            jsonMap.put("function_name", "xxhash64");
                        }
                        jsonMap.put("function_name", "mm3hash");
                        setDataType(rexCall,jsonMap, "returnType");
                        List<Object> arguments = new LinkedList<>();
                        Map<String, Object> seed = new LinkedHashMap<>();
                        // Set the seed to 0
                        seed.put("value", 0);
                        seed.put("isNull", false);
                        seed.put("exprType", "LITERAL");
                        seed.put("dataType", returnType);
                        arguments.add(buildJsonMap(operands.get(0)));
                        arguments.add(seed);
                        jsonMap.put("arguments", arguments);
                        break;
                    case EXTRACT:
                        // Current hardcoded solution for extracting hour
                        if (operands.get(0).toString().equals("FLAG(HOUR)")) {// && operands.get(1).toString().equals("CAST($2):TIMESTAMP(3)")){
                            jsonMap.put("exprType", "FUNCTION");
                            // Returns int for 0-24
                            jsonMap.put("returnType", 1);
                            jsonMap.put("function_name", "get_hour");
                            List<Object> args = new LinkedList<>();
                            args.add(buildJsonMap(operands.get(1)));
                            jsonMap.put("arguments", args);
                        } else {
                            jsonMap.put("exprType", "INVALID");
                        }
                        break;
                    case LOWER:
                        jsonMap.put("exprType", "FUNCTION");
                        setDataType(rexCall,jsonMap, "returnType");
                        jsonMap.put("function_name", "lower");
                        List<Map<String, Object>> lowerArgList = new ArrayList<>();
                        lowerArgList.add(buildJsonMap(operands.get(0)));
                        jsonMap.put("arguments", lowerArgList);
                        break;
                    case IS_NOT_NULL:
                        jsonMap.put("exprType", "IS_NOT_NULL");
                        setDataType(rexCall,jsonMap, "returnType");

                        List<Map<String, Object>> notnullArgList = new ArrayList<>();
                        notnullArgList.add(buildJsonMap(operands.get(0)));
                        jsonMap.put("arguments", notnullArgList);
                        break;
                    case PROCTIME:
                        jsonMap.put("exprType", SpecialExprType.PROCTIME);
//                        jsonMap.put("returnType",  rexCall.getType().getSqlTypeName().toString());
//                        jsonMap.put("dataType", rexCall.getType().getSqlTypeName().toString());
                        jsonMap.put("returnType", 22);
                        LOG.info("The operator is* {} ", operator.getName());
                        LOG.info("The type is* {} ", rexCall.getType().getSqlTypeName().toString());
                        break;
                    case DATE_FORMAT:
                        jsonMap.put("exprType", "FUNCTION");
                        jsonMap.put("function_name", "from_unixtime_without_tz");
                        Integer returnDataType = RexTypeToIdMap.get(rexCall.getType().getSqlTypeName().toString());
                        jsonMap.put("returnType", returnDataType);
                        jsonMap.put("width", operands.get(1).getType().getPrecision());
                        List<Map<String, Object>> argumentsList = new ArrayList<>();
                        Map<String, Object> argMap1 = buildJsonMap(operands.get(0));
                        setDataType(operands.get(0), argMap1, "dataType");
                        if (!argMap1.getOrDefault("dataType", 2).equals(2)) {
                            argMap1.put("value", "INVALID");
                        }
                        argMap1.put("dataType", 2);
                        argumentsList.add(argMap1);
                        Map<String, Object> argMap2 = buildJsonMap(operands.get(1));
                        argMap2.put("dataType", returnDataType);
                        argumentsList.add(argMap2);
                        if ("yyyy-MM-dd".equals(argMap2.get("value"))) {
                            argMap2.put("value", "%Y-%m-%d");
                        } else if ("HH:mm".equals(argMap2.get("value"))) {
                            argMap2.put("value", "%H:%M");
                        } else {
                            argMap2.put("value", "INVALID");
                        }
                        jsonMap.put("arguments", argumentsList);
                        break;
                    case CAST:
                        jsonMap.put("exprType", "FUNCTION");
                        Map<String, Object> childMap = buildJsonMap(operands.get(0));
                        SqlTypeName currentTypeName = rexCall.getType().getSqlTypeName();
                        SqlTypeName childTypeName = operands.get(0).getType().getSqlTypeName();
                        boolean decimalSameScale = currentTypeName == SqlTypeName.DECIMAL
                                && childTypeName == SqlTypeName.DECIMAL
                                && rexNode.getType().getScale() == operands.get(0).getType().getScale();
                        boolean nonDecimalSameType = currentTypeName != SqlTypeName.DECIMAL
                                && childTypeName != SqlTypeName.DECIMAL
                                && currentTypeName == childTypeName;
                        if (nonDecimalSameType || decimalSameScale) {
                            return childMap;
                        }
                        setDataType(rexCall,jsonMap, "returnType");
                        jsonMap.put("function_name", specialType.name());
                        jsonMap.put("expr", childMap);
                        List<Map<String, Object>> castArgList = new ArrayList<>();
                        castArgList.add(buildJsonMap(operands.get(0)));
                        jsonMap.put("arguments", castArgList);
                        break;
                    case AND:
                        jsonMap.put("exprType", "MULTIPLE_AND_OR");
                        jsonMap.put("operator", "AND");
                        setDataType(rexCall, jsonMap, "returnType");
                        List<Map<String, Object>> cond = new ArrayList<>();
                        for (int i=0; i < rexCall.operands.size(); i++) {
                            cond.add(buildJsonMap(operands.get(i)));
                        }
                        jsonMap.put("conditions", cond);
                        LOG.info("List is {}", cond.toString());
                        LOG.info("The expression is {} ", rexCall.toString());
                        break;
                    case OR:
                        jsonMap.put("exprType", "MULTIPLE_AND_OR");
                        jsonMap.put("operator", "OR");
                        setDataType(rexCall, jsonMap, "returnType");
                        cond = new ArrayList<>();
                        for (int i=0; i < rexCall.operands.size(); i++) {
                            cond.add(buildJsonMap(operands.get(i)));
                        }
                        jsonMap.put("conditions", cond);
                        LOG.info("List is {}", cond.toString());
                        LOG.info("The expression is {} ", rexCall.toString());
                        break;
                }
                return jsonMap;
            }
            else{
                LOG.info("The operator {} is not supported", operator.toString());
                LOG.info("The expression is {} ", rexCall.toString());
                jsonMap.put("operator","INVALID");
                return jsonMap;
            }
        }
        else if (rexNode instanceof RexLiteral) {
            RexLiteral rexLiteral = (RexLiteral) rexNode;
            jsonMap.put("exprType",  OperatorExprType.LITERAL.name());
            setDataType(rexLiteral, jsonMap, "dataType");
            jsonMap.put("isNull", rexLiteral.isNull());
            if (!rexLiteral.isNull()){
                Object value = rexLiteral.getValue2();
                jsonMap.put("value",value);
            }
            //todo: for DECIMAL64D and DECIMAL128, add fields: precision and scale
            return jsonMap;

        } else if (rexNode instanceof RexInputRef){
            RexInputRef inputRef = (RexInputRef) rexNode;
            if (inputRef instanceof RexPatternFieldRef || inputRef instanceof RexTableInputRef){
                //we may parse RexTableInputRef later.
                LOG.info("RexPatternFieldRef/RexTableInputRef is not supported.");
                jsonMap.put("exprType",  OperatorExprType.INVALID.name());
                return jsonMap;
            } else{ // deal with $index
                RexInputRef  rexInputRef =(RexInputRef) rexNode;
                jsonMap.put("exprType",  OperatorExprType.FIELD_REFERENCE.name());
                SqlTypeName dataType= rexInputRef.getType().getSqlTypeName();
                setDataType(rexInputRef, jsonMap, "dataType");
                jsonMap.put("colVal", accessIndexMap.getOrDefault(rexInputRef.hashCode(), rexInputRef.hashCode()));
                //todo: add fields precision and scale for DECIMAL64D and DECIMAL128
                return jsonMap;
            }
        } else if (rexNode instanceof RexFieldAccess){
            RexFieldAccess fieldAccess = (RexFieldAccess) rexNode;
            // Create a field access based on accessIndexMap
            jsonMap.put("exprType",  OperatorExprType.FIELD_REFERENCE.name());
            SqlTypeName dataType= fieldAccess.getType().getSqlTypeName();
            setDataType(fieldAccess, jsonMap, "dataType");
            int colVal = fieldAccess.getReferenceExpr().hashCode();
            int fieldVal = fieldAccess.getField().getIndex();
            //offset + fieldVal
            jsonMap.put("colVal", accessIndexMap.get(colVal) + fieldVal);
            return jsonMap;
            /*
            jsonMap.put("exprType",  OperatorExprType.FIELD_ACCESS.name());
            SqlTypeName dataType= fieldAccess.getType().getSqlTypeName();
	        setDataType(fieldAccess, jsonMap);
            jsonMap.put("colVal", fieldAccess.getReferenceExpr().hashCode());
            jsonMap.put("field_colVal", fieldAccess.getField().getIndex());
            return jsonMap;
            */
        }
        else {// todo: we may consider to parse other types of RexNode later.
            SqlKind kind = rexNode.getKind();
            LOG.info("The RexNode is {}", kind);
            jsonMap.put("exprType",  OperatorExprType.INVALID.name());
            LOG.info("The RexNode is not a RexCall/RexInputRef/RexLiteral. It is not recognized.");
            return jsonMap;
        }
    }
}
