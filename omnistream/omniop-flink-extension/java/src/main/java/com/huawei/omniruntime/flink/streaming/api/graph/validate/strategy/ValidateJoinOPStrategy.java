package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import static org.apache.flink.util.Preconditions.checkState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ValidateJoinOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateJoinOPStrategy.class);
    private static final Set<String> SUPPORT_JOIN_TYPE = new HashSet<>(Arrays.asList("InnerJoin", "LeftOuterJoin"));

    private static boolean nonEquiConditionCheck(Map<String, Object> condition, int inputSize) {
        checkState(condition.get("exprType") instanceof String);
        String exprType = (String) condition.get("exprType");
        if (exprType.equals("BINARY")) {
            if (!condition.get("returnType").equals(RexTypeToIdMap.get("BOOLEAN"))) {
                LOG.warn("Expression type is binary, but return type is not a boolean");
                return false;
            }
            return ValidateCalcOPStrategy.validateCalcExpr(condition, inputSize);
        }

        if (exprType.equals("INVALID") || exprType == null) {
            return false;
        }
        if (exprType.equals("FIELD_ACCESS")) {
            return false;
        }

        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        // Checks if join type is valid
        String joinType = operatorInfoMap.get("joinType").toString();
        if (!SUPPORT_JOIN_TYPE.contains(joinType)) {
            return false;
        }
        List<Integer> leftJoinKey = (List<Integer>) operatorInfoMap.get("leftJoinKey");
        List<Integer> rightJoinKey = (List<Integer>) operatorInfoMap.get("rightJoinKey");
        List<String> leftInputTypes = (List<String>) operatorInfoMap.get("leftInputTypes");
        List<String> rightInputTypes = (List<String>) operatorInfoMap.get("rightInputTypes");
        List<String> outputTypes = (List<String>) operatorInfoMap.get("outputTypes");

        // Checks if leftJoinKey and rightJoinKey sizes are the same
        if (leftJoinKey.size() != rightJoinKey.size() || leftJoinKey.size() == 0) {
            LOG.warn("Join Key indicies do not match or key size is 0");
            return false;
        }

        // Checks if key types are equal
        for (int i = 0; i < leftJoinKey.size(); i++) {
            String leftType = leftInputTypes.get(leftJoinKey.get(i));
            String rightType = rightInputTypes.get(rightJoinKey.get(i));
            if (!leftType.equals(rightType)) {
                LOG.warn("Join Key types are not equal. leftType = {}, rightType = {}", leftType, rightType);
                return false;
            }
        }

        // Checks if inputSpec is allowed
        if (operatorInfoMap.get("leftInputSpec").equals("JoinKeyContainsUniqueKey")) {
            if (((ArrayList<Object>) operatorInfoMap.get("leftUniqueKeys")).size() == 0) {
                LOG.warn("Left input should contain unique key, but no unique key index has been found");
                return false;
            }
        }
        if (operatorInfoMap.get("rightInputSpec").equals("JoinKeyContainsUniqueKey")) {
            if (((ArrayList<Object>) operatorInfoMap.get("rightUniqueKeys")).size() == 0) {
                LOG.warn("Right input should contain unique key, but no unique key index has been found");
                return false;
            }
        }

        // Checks if condition is valid
        Object condition = operatorInfoMap.get("nonEquiCondition");
        if (condition != null) {
            if (!nonEquiConditionCheck((Map<String, Object>) condition, outputTypes.size())) {
                return false;
            }
        }

        // Checks if data types are valid
        return validateDataTypes(getDataTypes(operatorInfoMap, "leftInputTypes", "rightInputTypes", "outputTypes"));
    }
}
