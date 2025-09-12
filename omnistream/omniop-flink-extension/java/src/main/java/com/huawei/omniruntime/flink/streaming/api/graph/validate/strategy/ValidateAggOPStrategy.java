package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import org.apache.flink.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ValidateAggOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateAggOPStrategy.class);

    private static final Set<String> SUPPORT_AGG_FUNCTION_NAME = new HashSet<>(Arrays.asList(
            "MAX",
            "AVG",
            "COUNT",
            "MIN",
            "SUM",
            "SUM0",
            "last_string_value_without_retract"));

    private static final Map<String, List<String>> SUPPORT_AGG_FUNCTION_DATATYPE = new HashMap<>();

    static {
        SUPPORT_AGG_FUNCTION_DATATYPE.put("MAX", Arrays.asList("BIGINT", "VARCHAR(2147483647)"));
        SUPPORT_AGG_FUNCTION_DATATYPE.put("AVG", Collections.singletonList("BIGINT"));
        SUPPORT_AGG_FUNCTION_DATATYPE.put("COUNT", Collections.singletonList("BIGINT"));
        SUPPORT_AGG_FUNCTION_DATATYPE.put("MIN", Arrays.asList("BIGINT", "VARCHAR(2147483647)"));
        SUPPORT_AGG_FUNCTION_DATATYPE.put("SUM", Collections.singletonList("BIGINT"));
        SUPPORT_AGG_FUNCTION_DATATYPE.put("SUM0", Collections.singletonList("BIGINT"));
        SUPPORT_AGG_FUNCTION_DATATYPE.put("last_string_value_without_retract", Collections.singletonList("VARCHAR(2147483647)"));
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        Map<String, Object> aggInfoListMap = (Map<String, Object>) operatorInfoMap.get("aggInfoList");
        List<Map<String, Object>> aggregateCalls = (ArrayList<Map<String, Object>>) aggInfoListMap.get("aggregateCalls");
        if (CollectionUtil.isNullOrEmpty(aggregateCalls)) {
            return false;
        }

        List<String> inputTypeList = (ArrayList<String>) operatorInfoMap.get("inputTypes");
        boolean inputTypesEmpty = CollectionUtil.isNullOrEmpty(inputTypeList);
        for (Map<String, Object> aggregateCallMap : aggregateCalls) {
            String name = aggregateCallMap.get("name").toString();
            // aggCallName like "MAX($1)","AVG($2)","COUNT($1)"
            String functionName = name.substring(0, name.indexOf("("));
            // some name like "$SUM()" need to delete "$"
            if (functionName.contains("$")) {
                functionName = functionName.replace("$", "");
            }
            if (!SUPPORT_AGG_FUNCTION_NAME.contains(functionName)) {
                LOG.info("validateVertexChainInfoForOmniTask not support aggCall is {}", name);
                return false;
            }
            List<Integer> argIndexes = (ArrayList<Integer>) aggregateCallMap.get("argIndexes");
            if (inputTypesEmpty || CollectionUtil.isNullOrEmpty(argIndexes)) {
                continue;
            }
            int argIndex = argIndexes.get(0);
            String argType = inputTypeList.get(argIndex);
            List<String> supportDataTypes = SUPPORT_AGG_FUNCTION_DATATYPE.get(functionName);
            if (!supportDataTypes.contains(argType)) {
                return false;
            }
        }

        // if function support,then validate dataTypes
        List<List<String>> dataTypesList = new ArrayList<>(getDataTypes(operatorInfoMap, "inputTypes", "outputTypes"));
        return validateDataTypes(dataTypesList);
    }
}
