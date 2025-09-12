/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import org.apache.flink.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * the strategy for GroupWindowAggregate operator.
 *
 * @since 2025-05-30
 */
public class ValidateGroupWindowAggOPStrategy extends AbstractValidateOperatorStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ValidateGroupWindowAggOPStrategy.class);

    private static final Set<String> SUPPORT_AGG_FUNCTION_NAME = new HashSet<>(Collections.singletonList("COUNT"));

    private static final Set<String> SUPPORT_WINDOW_TYPE = new HashSet<>(
        Collections.singletonList("SessionGroupWindow"));

    private static final Map<String, List<String>> SUPPORT_AGG_FUNCTION_DATATYPE = new HashMap<>();

    static {
        SUPPORT_AGG_FUNCTION_DATATYPE.put("COUNT", Collections.singletonList("BIGINT"));
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        List<String> inputTypeList = (ArrayList<String>) operatorInfoMap.get("inputTypes");
        // Validate SUPPORT_WINDOW_TYPE
        String windowInfo;
        Object windowInfoObj = operatorInfoMap.get("windowType");
        if (windowInfoObj instanceof String) {
            windowInfo = (String) operatorInfoMap.get("windowType");
        } else {
            return false;
        }
        String windowType = windowInfo.substring(0, windowInfo.indexOf("("));
        if (!SUPPORT_WINDOW_TYPE.contains(windowType)) {
            return false;
        }

        // Validate agg function
        Map<String, Object> aggInfoListMap = (Map<String, Object>) operatorInfoMap.get("aggInfoList");
        List<Map<String, Object>> aggregateCalls =
            (ArrayList<Map<String, Object>>) aggInfoListMap.get("aggregateCalls");
        boolean inputTypesEmpty = CollectionUtil.isNullOrEmpty(inputTypeList);
        for (Map<String, Object> aggregateCallMap : aggregateCalls) {
            String name = aggregateCallMap.get("name").toString();
            String functionName = name.substring(0, name.indexOf("("));
            if (!SUPPORT_AGG_FUNCTION_NAME.contains(functionName)) {
                LOG.info("ValidateWindowAggOPStrategy not support aggCall is {}", name);
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
