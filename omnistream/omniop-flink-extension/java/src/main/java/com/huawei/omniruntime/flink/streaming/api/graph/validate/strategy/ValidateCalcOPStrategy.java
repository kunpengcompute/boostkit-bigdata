package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import org.apache.flink.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ValidateCalcOPStrategy extends AbstractValidateOperatorStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ValidateCalcOPStrategy.class);

    private static final Set<String> SUPPORT_BINARYOP_NAME = new HashSet<>(Arrays.asList(
            "OR",
            "AND",
            "ADD",
            "SUBTRACT",
            "MULTIPLY",
            "DIVIDE",
            "MODULUS",
            "GREATER_THAN",
            "GREATER_THAN_OR_EQUAL",
            "LESS_THAN",
            "LESS_THAN_OR_EQUAL",
            "EQUAL",
            "NOT_EQUAL",
            "DATE_FORMAT",
            "count_char"));
    private static final Set<String> SUPPORT_UNARYOP_NAME = new HashSet<>(Arrays.asList("CAST", "NEGATION"));

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        //check input/output fields.
        int inputSize = 0;
        if (operatorInfoMap.containsKey("inputTypes")) {
            List<String> inputTypes = (List<String>) operatorInfoMap.get("inputTypes");
            inputSize = inputTypes.size();
            if (CollectionUtil.isNullOrEmpty(inputTypes)) {
                return false;
            }
        } else {
            LOG.info("Missing inputTypes field.");
            return false;
        }
        if (operatorInfoMap.containsKey("outputTypes")) {
            List<String> outputTypes = (List<String>) operatorInfoMap.get("outputTypes");
            if (CollectionUtil.isNullOrEmpty(outputTypes)) {
                return false;
            }
        } else {
            LOG.info("Missing outputTypes field.");
            return false;
        }

        //check indices
        List<Map<String, Object>> indicesList = (List<Map<String, Object>>) operatorInfoMap.get("indices");
        if (CollectionUtil.isNullOrEmpty(indicesList)) {
            return false;
        }
        // if any map in the indicesList is null or empty,this calc is not supported
        boolean support = indicesList.stream().noneMatch(CollectionUtil::isNullOrEmpty);
        if (!support) {
            return false;
        }

        for (Map<String, Object> indexmap : indicesList) {
            if (!validateCalcExpr(indexmap, inputSize)) {
                return false;
            }
        }

        //check condition
        if (operatorInfoMap.containsKey("condition")) {
            Map<String, Object> conditionMap = (Map<String, Object>) operatorInfoMap.get("condition");
            if ((conditionMap != null) && !validateCalcExpr(conditionMap, inputSize)) {
                return false;
            }
        }
        //check dataTypes
        return validateDataTypes(getDataTypes(operatorInfoMap, "inputTypes", "outputTypes"));
    }


    private static boolean validateCalcExprMaps(int inputSize, String operatorType, Object... exprMaps) {
        for (Object exprMap : exprMaps) {
            if (exprMap instanceof Map) {
                //recursive call validateCalcExpr
                if (!validateCalcExpr((Map<String, Object>) exprMap, inputSize)) {
                    return false;
                }
            } else {
                LOG.info("ERROR: Cannot parse the right expr in a expression: {}", operatorType);
                return false;
            }
        }
        return true;
    }

    /**
     * validateCalcExpr
     *
     * @param exprMap exprMap
     * @param inputSize inputSize
     * @return boolean result
     */
    public static boolean validateCalcExpr(Map<String, Object> exprMap, int inputSize) {
        if (!exprMap.containsKey("exprType")) {
            return false; // If any map doesn't contain "expr", return false
        }
        String exprType = (String) exprMap.get("exprType");
        switch (exprType) {
            case "BINARY":
                String binaryOperatorType = (String) exprMap.get("operator");
                if (!SUPPORT_BINARYOP_NAME.contains(binaryOperatorType)) {
                    return false;
                }
                if (!exprMap.containsKey("returnType") || !exprMap.containsKey("left") || !exprMap.containsKey("right")) {
                    return false;
                }
                if (!RexTypeToIdMap.containsValue(exprMap.get("returnType"))) {
                    return false;
                }

                Object leftExpr = exprMap.get("left");
                if (leftExpr instanceof Map) {
                    //recursive call validateCalcExpr
                    if (!validateCalcExpr((Map<String, Object>) leftExpr, inputSize)) {
                        return false;
                    }
                } else {
                    LOG.info("ERROR: Cannot parse the left expr in a binary expression: {}", binaryOperatorType);
                    return false;
                }
                Object rightExpr = exprMap.get("right");
                if (rightExpr instanceof Map) {
                    //recursive call validateCalcExpr
                    if (!validateCalcExpr((Map<String, Object>) rightExpr, inputSize)) {
                        return false;
                    }
                } else {
                    LOG.info("ERROR: Cannot parse the right expr in a binary expression: {}", binaryOperatorType);
                    return false;
                }
                return true;
            case "UNARY":
                String unaryOperatorType = (String) exprMap.get("operator");
                if (!SUPPORT_UNARYOP_NAME.contains(unaryOperatorType)) {
                    return false;
                }
                if (!exprMap.containsKey("returnType") || !exprMap.containsKey("expr")) {
                    return false;
                }
                Object expr = exprMap.get("expr");
                if (expr instanceof Map) {
                    //recursive call validateCalcExpr
                    if (!validateCalcExpr((Map<String, Object>) expr, inputSize)) {
                        return false;
                    }
                } else {
                    LOG.info("ERROR: Cannot parse expr in an unary expression: {}", unaryOperatorType);
                    return false;
                }
                //we currently only deal with a fake CAST

                LOG.info("WARNING: CAST/NEGATION might not be supported.");
                return true;
            case "LITERAL":
                if (!exprMap.containsKey("dataType") || !exprMap.containsKey("isNull")) {
                    return false;
                }
                if (!RexTypeToIdMap.containsValue(exprMap.get("dataType"))) {
                    return false;
                }
                if (!(boolean) exprMap.get("isNull") && !exprMap.containsKey("value")) {
                    return false;
                }
                return true;
            case "FIELD_REFERENCE":
                if (!exprMap.containsKey("dataType") || !exprMap.containsKey("colVal")) {
                    return false;
                }
                if (!RexTypeToIdMap.containsValue(exprMap.get("dataType"))) {
                    return false;
                }
                Object colVal = exprMap.get("colVal");
                if (colVal instanceof Integer) {
                    if ((int) colVal < 0 || (int) colVal >= inputSize) {
                        LOG.info("Column value in a FIELD_REFERENCE is out of bound");
                        return false;
                    }
                } else {
                    LOG.info("Cannot parse column value in a FIELD_REFERENCE");
                    return false;
                }
                return true;
            case "SWITCH":
                if (!exprMap.containsKey("returnType") || !exprMap.containsKey("numOfCases") || !exprMap.containsKey("input") || !exprMap.containsKey("else")) {
                    return false;
                }
                Object inputExpr = exprMap.get("input");
                if (inputExpr instanceof Map) {
                    //recursive call validateCalcExpr
                    if (!validateCalcExpr((Map<String, Object>) inputExpr, inputSize)) {
                        return false;
                    }
                } else {
                    LOG.info("ERROR: Cannot parse the input expr in a CASE expression.");
                    return false;
                }
                Object elseExpr = exprMap.get("else");
                if (elseExpr instanceof Map) {
                    //recursive call validateCalcExpr
                    if (!validateCalcExpr((Map<String, Object>) elseExpr, inputSize)) {
                        return false;
                    }
                } else {
                    LOG.info("ERROR: Cannot parse the else expr in a CASE expression.");
                    return false;
                }

                Object numOfCases = exprMap.get("numOfCases");
                if (numOfCases instanceof Integer) {
                    int num = (int) numOfCases;
                    for (int i = 1; i <= num; i++) {
                        String caseKey = "Case" + i;
                        if (!exprMap.containsKey(caseKey)) {
                            return false;
                        }
                        Object caseExpr = exprMap.get(caseKey);
                        if (caseExpr instanceof Map) {
                            if (!validateCaseExpr((Map<String, Object>) caseExpr, inputSize)) {
                                return false;
                            }
                        } else {
                            LOG.info("ERROR: Cannot parse the case expr in a CASE expression.");
                            return false;
                        }
                    }
                } else {
                    LOG.info("Cannot parse column numOfCases in a CASE expression");
                    return false;
                }
                return true;
            case "REGEX_EXTRACT":
                if (!exprMap.containsKey("returnType") || !exprMap.containsKey("input") || !exprMap.containsKey("regex") || !exprMap.containsKey("extract_index")) {
                    return false;
                }
                if (!validateCalcExprMaps(inputSize, "REGEX_EXTRACT", exprMap.get("input"), exprMap.get("regex"), exprMap.get("extract_index"))) {
                    return false;
                }
                return true;
            case "SPLIT_INDEX":
                if (!exprMap.containsKey("returnType") || !exprMap.containsKey("input") || !exprMap.containsKey("delimiter") || !exprMap.containsKey("extract_index")) {
                    return false;
                }
                if (!validateCalcExprMaps(inputSize, "SPLIT_INDEX", exprMap.get("input"), exprMap.get("delimiter"), exprMap.get("extract_index"))) {
                    return false;
                }
                return true;

            case "FUNCTION":
                if (!exprMap.containsKey("returnType") || !exprMap.containsKey("exprType") || !exprMap.containsKey("function_name") || !exprMap.containsKey("arguments")) {
                    return false;
                }

                return validateReturnTypeAndArguments(exprMap, inputSize);

            case "SWITCH_GENERAL":
                if (!exprMap.containsKey("returnType") || !exprMap.containsKey("numOfCases") || !exprMap.containsKey("else")) {
                    return false;
                }

                Object elseExpre = exprMap.get("else");
                if (elseExpre instanceof Map) {
                    //recursive call validateCalcExpr
                    if (!validateCalcExpr((Map<String, Object>) elseExpre, inputSize)) {
                        return false;
                    }
                } else {
                    LOG.info("ERROR: Cannot parse the else expr in a CASE expression.");
                    return false;
                }

                Object numOfCasesG = exprMap.get("numOfCases");
                if (numOfCasesG instanceof Integer) {
                    int num = (int) numOfCasesG;
                    for (int i = 1; i <= num; i++) {
                        String caseKey = "Case" + i;
                        if (!exprMap.containsKey(caseKey)) {
                            return false;
                        }
                        Object caseExpr = exprMap.get(caseKey);
                        if (caseExpr instanceof Map) {
                            if (!validateCaseExpr((Map<String, Object>) caseExpr, inputSize)) {
                                return false;
                            }
                        } else {
                            LOG.info("ERROR: Cannot parse the case expr in a CASE expression.");
                            return false;
                        }
                    }
                } else {
                    LOG.info("Cannot parse column numOfCases in a CASE expression");
                    return false;
                }
                return true;
            case "IS_NOT_NULL":
                return validateReturnTypeAndArguments(exprMap, inputSize);
            case "MULTIPLE_AND_OR":
                if (!exprMap.containsKey("returnType") || !exprMap.containsKey("conditions")) {
                    return false;
                }
                Object conditions = exprMap.get("conditions");
                if (!(conditions instanceof List)) {
                    LOG.info("ERROR: Cannot parse the case expr in a CASE expression conditions.");
                    return false;
                }
                List<Map<String, Object>> conditionList = (List<Map<String, Object>>) conditions;
                for (Map<String, Object> condition : conditionList) {
                    if (!validateCalcExpr(condition, inputSize)) {
                        return false;
                    }
                }
                return true;
            case "IN":
                return validateReturnTypeAndArguments(exprMap, inputSize);
            case "BETWEEN":
                if (!exprMap.containsKey("returnType") || !exprMap.containsKey("value") || !exprMap.containsKey("lower_bound") || !exprMap.containsKey("upper_bound")) {
                    return false;
                }

                Object valueExpr = exprMap.get("value");
                if (valueExpr instanceof Map) {
                    //recursive call validateCalcExpr
                    if (!validateCalcExpr((Map<String, Object>) valueExpr, inputSize)) {
                        return false;
                    }
                } else {
                    LOG.info("BETWEEN's value is not a map");
                    return false;
                }

                Object lowerBoundExpr = exprMap.get("lower_bound");
                if (lowerBoundExpr instanceof Map) {
                    //recursive call validateCalcExpr
                    if (!validateCalcExpr((Map<String, Object>) lowerBoundExpr, inputSize)) {
                        return false;
                    }
                } else {
                    LOG.info("BETWEEN's lower_bound is not a map");
                    return false;
                }

                Object upperBoundExpr = exprMap.get("upper_bound");
                if (upperBoundExpr instanceof Map) {
                    //recursive call validateCalcExpr
                    if (!validateCalcExpr((Map<String, Object>) upperBoundExpr, inputSize)) {
                        return false;
                    }
                } else {
                    LOG.info("BETWEEN's upper_bound is not a map");
                    return false;
                }
                return true;
            case "PROCTIME" :
                return true;
            default:
                LOG.info("Invalid expr type: {}", exprType);
                return false; // Invalid expr type
        }
    }

    private static boolean validateReturnTypeAndArguments(Map<String, Object> exprMap, int inputSize) {
        if (!exprMap.containsKey("returnType") || !exprMap.containsKey("arguments")) {
            return false;
        }
        Object arguments = exprMap.get("arguments");
        if (!(arguments instanceof List)) {
            LOG.info("ERROR: Cannot parse the case expr in a CASE expression arguments.");
            return false;
        }
        List<Map<String, Object>> argumentList = (List<Map<String, Object>>) arguments;
        for (Map<String, Object> argument : argumentList) {
            if (!validateCalcExpr(argument, inputSize)) {
                return false;
            }
        }
        return true;
    }

    private static boolean validateCaseExpr(Map<String, Object> exprMap, int inputSize) {
        if (!exprMap.containsKey("exprType") || !exprMap.containsKey("returnType") || !exprMap.containsKey("when") || !exprMap.containsKey("result")) {
            return false;
        }
        Object whenExpr = exprMap.get("when");
        if (whenExpr instanceof Map) {
            //recursive call validateCalcExpr
            if (!validateCalcExpr((Map<String, Object>) whenExpr, inputSize)) {
                return false;
            }
        } else {
            LOG.info("ERROR: Cannot parse the when expr in a CASE expr");
            return false;
        }
        Object resultExpr = exprMap.get("result");
        if (resultExpr instanceof Map) {
            //recursive call validateCalcExpr
            if (!validateCalcExpr((Map<String, Object>) resultExpr, inputSize)) {
                return false;
            }
        } else {
            LOG.info("ERROR: Cannot parse the result expr in a CASE expr");
            return false;
        }
        return true;
    }
}
