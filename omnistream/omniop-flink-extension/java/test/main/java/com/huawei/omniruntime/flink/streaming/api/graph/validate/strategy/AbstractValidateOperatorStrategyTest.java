package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AbstractValidateOperatorStrategyTest {

    private final AbstractValidateOperatorStrategy validate = new ValidateAggOPStrategy();

    @Test
    public void testValidateDataTypesForNotSupport() {
        List<String> inputTypes = Arrays.asList("BIGINT", "DOUBLE", "BIGINT");
        List<String> outputTypes = Arrays.asList("BIGINT", "TIMESTAMP_WITHOUT_TIME_ZONE(3)", "BIGINT");
        boolean result = validate.validateDataTypes(Arrays.asList(inputTypes, outputTypes));
        assertFalse(result);
    }

    @Test
    public void testValidateDataTypesForAllSupport() {
        List<String> inputTypes = Arrays.asList("BIGINT", "TIMESTAMP_WITHOUT_TIME_ZONE(0)", "BIGINT");
        List<String> outputTypes = Arrays.asList("BIGINT", "TIMESTAMP_WITHOUT_TIME_ZONE(3)", "BIGINT");
        boolean result = validate.validateDataTypes(Arrays.asList(inputTypes, outputTypes));
        assertTrue(result);
    }

    @Test
    public void testValidateDataTypesForTimestampNotSupport() {
        List<String> inputTypes = Arrays.asList("BIGINT", "BIGINT", "BIGINT");
        List<String> outputTypes = Arrays.asList("BIGINT", "TIMESTAMP_WITHOUT_TIME_ZONE(5)", "BIGINT");
        boolean result = validate.validateDataTypes(Arrays.asList(inputTypes, outputTypes));
        assertFalse(result);
    }
}