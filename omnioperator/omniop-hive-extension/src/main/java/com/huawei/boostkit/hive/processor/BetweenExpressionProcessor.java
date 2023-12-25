package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.CompareExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class BetweenExpressionProcessor implements ExpressionProcessor {

    private static final int LOWER_BOUND_INDEX = 2;
    private static final int UPPER_BOUND_INDEX = 3;

    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {

        CompareExpression leaf = new CompareExpression("BINARY",
                TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                operator.equals("NOT") ? "OR" : "AND");
        leaf.add(new CompareExpression("BINARY",
                TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                operator.equals("NOT") ? "LESS_THAN" : "GREATER_THAN_OR_EQUAL"));
        leaf.add(new CompareExpression("BINARY",
                TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                operator.equals("NOT") ? "GREATER_THAN" : "LESS_THAN_OR_EQUAL"));
        BaseExpression subLeaf;
        ExprNodeDesc comparedNode = node.getChildren().get(1);
        if (node.getChildren().get(1) instanceof ExprNodeGenericFuncDesc) {
            subLeaf = ExpressionUtils.build((ExprNodeGenericFuncDesc) comparedNode, inspector);
        } else {
            subLeaf = ExpressionUtils.createNode(comparedNode, inspector);
        }
        leaf.add(subLeaf);
        leaf.add(createBoundNode(node, comparedNode, LOWER_BOUND_INDEX, inspector));
        leaf.add(subLeaf);
        leaf.add(createBoundNode(node, comparedNode, UPPER_BOUND_INDEX, inspector));
        return leaf;
    }

    private BaseExpression createBoundNode(ExprNodeGenericFuncDesc rootNode,
                                           ExprNodeDesc comparedNode, int boundIndex, ObjectInspector inspector) {
        ExprNodeDesc node = rootNode.getChildren().get(boundIndex);
        BaseExpression resultNode;
        if (node instanceof ExprNodeGenericFuncDesc) {
            resultNode = ExpressionUtils.build((ExprNodeGenericFuncDesc) node, inspector);
        } else {
            resultNode = ExpressionUtils.createNode(node, inspector);
        }
        if (!comparedNode.getTypeInfo().equals(
                rootNode.getChildren().get(boundIndex).getTypeInfo())) {
            resultNode = ExpressionUtils.preCast(resultNode, node, comparedNode);
        }
        return resultNode;
    }
}
