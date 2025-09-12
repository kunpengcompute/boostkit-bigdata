package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import java.util.HashMap;
import java.util.Map;

public class ValidateOperatorStrategyFactory {

    private static final Map<String, AbstractValidateOperatorStrategy> strategyMap = new HashMap<>();

    static {
        strategyMap.put("GroupAggregate", new ValidateAggOPStrategy());
        strategyMap.put("LocalWindowAggregate", new ValidateWindowAggOPStrategy());
        strategyMap.put("GlobalWindowAggregate", new ValidateWindowAggOPStrategy());
        strategyMap.put("GroupWindowAggregate", new ValidateGroupWindowAggOPStrategy());
        strategyMap.put("IncrementalGroupAggregate", new ValidateAggOPStrategy());
        strategyMap.put("Join", new ValidateJoinOPStrategy());
        strategyMap.put("LookupJoin", new ValidateLookupJoinOPStrategy());
        strategyMap.put("Calc", new ValidateCalcOPStrategy());
        strategyMap.put("Expand", new ValidateExpandOPStrategy());
        strategyMap.put("Deduplicate", new ValidateDeduplicateOPStrategy());
        strategyMap.put("WatermarkAssigner", new ValidateWatermarkOPStrategy());
        strategyMap.put("Rank", new ValidateRankOPStrategy());
    }

    public static AbstractValidateOperatorStrategy getStrategy(String operatorName) {
        return strategyMap.getOrDefault(operatorName, new ValidateDefaultOPStrategy());
    }
}
