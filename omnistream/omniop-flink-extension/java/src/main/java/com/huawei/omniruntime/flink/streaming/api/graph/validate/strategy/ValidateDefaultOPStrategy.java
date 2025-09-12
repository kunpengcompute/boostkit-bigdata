package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import java.util.Map;

public class ValidateDefaultOPStrategy extends AbstractValidateOperatorStrategy {
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        return true;
    }
}
