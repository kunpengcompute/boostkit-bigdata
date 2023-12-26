package com.huawei.boostkit.hive;

import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;

import nova.hetu.omniruntime.type.DataType;
import org.apache.hadoop.hive.ql.exec.ExprNodeColumnEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeGenericFuncEvaluator;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class JoinUtils {
    public static List<ExprNodeEvaluator> getExprNodeColumnEvaluator(List<ExprNodeEvaluator> joinKeys) {
        List<ExprNodeEvaluator> exprNodeColumnEvaluators = new ArrayList<>();
        for (ExprNodeEvaluator joinKey : joinKeys) {
            dealChildren(joinKey, exprNodeColumnEvaluators);
        }
        return exprNodeColumnEvaluators;
    }

    private static void dealChildren(ExprNodeEvaluator joinKey, List<ExprNodeEvaluator> exprNodeColumnEvaluators) {
        if (joinKey instanceof ExprNodeColumnEvaluator) {
            exprNodeColumnEvaluators.add(joinKey);
            return;
        }
        if (joinKey.getChildren() == null) {
            return;
        }
        Arrays.stream(joinKey.getChildren()).forEach(child -> dealChildren(child, exprNodeColumnEvaluators));
    }

    public static String[] getExprFromExprNode(List<ExprNodeEvaluator> nodes, Map<String, Integer> keyColNameToId, ObjectInspector inspector) {
        List<String> expressions = new ArrayList<>();
        for(ExprNodeEvaluator node:nodes) {
            if (node instanceof ExprNodeGenericFuncEvaluator) {
                expressions.add(ExpressionUtils.build((ExprNodeGenericFuncDesc) node.getExpr(), inspector).toString());
            } else {
                expressions.add(TypeUtils.buildExpression(((AbstractPrimitiveObjectInspector) node.getOutputOI()).getTypeInfo(),
                        keyColNameToId.get(((ExprNodeColumnEvaluator) node).getExpr().getColumn())));
            }
        }
        return expressions.toArray(new String[0]);
    }

    public static DataType[] getTypeFromInspectors(List<ObjectInspector> inspectors) {
        return inspectors.stream().map(
                inspector -> TypeUtils.buildInputDataType(((AbstractPrimitiveObjectInspector) inspector).getTypeInfo()))
                .toArray(DataType[]::new);
    }
}
