/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import org.apache.flink.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ValidateRankOPStrategy extends AbstractValidateOperatorStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ValidateRankOPStrategy.class);

    private static final List<String> SUPPORT_GROUP_KEY_TYPES = Collections.singletonList("BIGINT");
    private static final List<String> SUPPORT_ORDER_BY_TYPES = Arrays.asList("BIGINT",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)");
    private static final List<String> SUPPORT_FUNCTIONS = Arrays.asList("AppendOnlyTopNFunction", "FastTop1Function");

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        List<String> inputTypeList = (ArrayList<String>) operatorInfoMap.get("inputTypes");
        List<Integer> partitionKeys = (ArrayList<Integer>) operatorInfoMap.get("partitionKey");

        Object rankFunctionObj = operatorInfoMap.get("processFunction");
        String rankFunction = null;
        if (rankFunctionObj instanceof String) {
            rankFunction = (String) rankFunctionObj;
        } else {
            return false;
        }
        List<Boolean> sortAscendingOrders = (ArrayList<Boolean>) operatorInfoMap.get("sortAscendingOrders");
        List<Integer> sortFieldIds = (ArrayList<Integer>) operatorInfoMap.get("sortFieldIndices");

        // only support TOP1 and TOPN
        if (!SUPPORT_FUNCTIONS.contains(rankFunction)) {
            return false;
        }

        if (CollectionUtil.isNullOrEmpty(partitionKeys) || CollectionUtil.isNullOrEmpty(inputTypeList)) {
            LOG.info("failed inside isNull or empty check");
            return false;
        }

        if (partitionKeys.size() > 1) {
            return false;
        }

        for (int partitionKey : partitionKeys) {
            String keyType = inputTypeList.get(partitionKey);
            if (!SUPPORT_GROUP_KEY_TYPES.contains(keyType)) {
                LOG.info("failed inside group key type check {} ", keyType);
                return false;
            }
        }

        if (!validateTop1Function(operatorInfoMap, inputTypeList, sortFieldIds)) {
            return false;
        }

        if (!validateTopNFunction(operatorInfoMap, inputTypeList, sortFieldIds, sortAscendingOrders)) {
            return false;
        }

        // if function support,then validate dataTypes
        List<List<String>> dataTypesList = new ArrayList<>(getDataTypes(operatorInfoMap, "inputTypes", "outputTypes"));
        LOG.info("Otherwise failed for dataTypes");
        return validateDataTypes(dataTypesList);
    }

    private boolean validateTop1Function(Map<String, Object> operatorInfoMap, List<String> inputTypeList,
                                         List<Integer> sortFieldIds) {
        // TOP1
        if (!operatorInfoMap.get("processFunction").equals("FastTop1Function")) {
            return true;
        }

        if (sortFieldIds.size() > 2) {
            return false;
        }
        for (int sortId : sortFieldIds) {
            if (!SUPPORT_ORDER_BY_TYPES.contains(inputTypeList.get(sortId))) {
                return false;
            }
        }

        return true;
    }

    private boolean validateTopNFunction(Map<String, Object> operatorInfoMap, List<String> inputTypeList,
                                         List<Integer> sortFieldIds, List<Boolean> sortAscendingOrders) {
        // TOP N  more strict condition
        if (!operatorInfoMap.get("processFunction").equals("AppendOnlyTopNFunction")) {
            return true;
        }

        return sortFieldIds.size() <= 1 && inputTypeList.get(sortFieldIds.get(0)).equals("BIGINT")
                && !sortAscendingOrders.get(0);
    }
}
