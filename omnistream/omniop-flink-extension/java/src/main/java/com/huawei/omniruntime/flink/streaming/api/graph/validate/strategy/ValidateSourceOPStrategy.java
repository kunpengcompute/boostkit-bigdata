package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @since 2025/5/10
 */
public class ValidateSourceOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Set<String> SUPPORT_KAFKA_SCHEMA_TPYE = new HashSet<>();

    static {
        SUPPORT_KAFKA_SCHEMA_TPYE.addAll(Arrays.asList("JsonRowDataDeserializationSchema"));
    }

    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        return false;
    }
}
