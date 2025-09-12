package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ValidateLookupJoinOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateLookupJoinOPStrategy.class);
    private static final Set<String> SUPPORT_JOIN_TYPE = new HashSet<>(Arrays.asList("InnerJoin"));

    private static final Set<String> SUPPORT_TABLESOURCE_TYPE = new HashSet<>(Arrays.asList("CsvTableSource"));

    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        // native lookupJoin only support csv table now
        String temporalTableSourceTypeName = operatorInfoMap.get("temporalTableSourceTypeName").toString();
        if (!SUPPORT_TABLESOURCE_TYPE.contains(temporalTableSourceTypeName)) {
            return false;
        }

        // Checks if join type is valid
        String joinType = operatorInfoMap.get("joinType").toString();
        if (!SUPPORT_JOIN_TYPE.contains(joinType)) {
            return false;
        }

        // Checks if data types are valid
        return validateDataTypes(getDataTypes(operatorInfoMap, "inputTypes", "lookupInputTypes", "outputTypes"));
    }

}
