package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.FunctionExpression;
import com.huawei.boostkit.hive.expression.NotExpression;
import com.huawei.boostkit.hive.expression.ReferenceFactor;
import com.huawei.boostkit.hive.expression.TypeUtils;
import com.huawei.boostkit.hive.expression.UnaryExpression;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.List;

public class SubstrExpressionProcessor implements ExpressionProcessor {
    private static final int LOWER_BOUND_INDEX = 1;
    private static final int UPPER_BOUND_INDEX = 2;

    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        List<ExprNodeDesc> children = node.getChildren();
        FunctionExpression functionExpression = new FunctionExpression(
                TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                "substr", children.size(),
                calculateLength((ExprNodeConstantDesc) children.get(UPPER_BOUND_INDEX),
                        (ExprNodeConstantDesc) children.get(LOWER_BOUND_INDEX)));
        for (ExprNodeDesc child : children) {
            if (child instanceof ExprNodeGenericFuncDesc) {
                functionExpression.add(ExpressionUtils.build((ExprNodeGenericFuncDesc) child, inspector));
            } else {
                BaseExpression node1 = ExpressionUtils.createNode(child, inspector);
                if (node1 instanceof ReferenceFactor) {
                    functionExpression.add(wrapNotNullExpression((ReferenceFactor) node1));
                } else {
                    functionExpression.add(node1);
                }
            }
        }
        return functionExpression;
    }

    private BaseExpression wrapNotNullExpression(ReferenceFactor referenceFactor) {
        UnaryExpression unaryExpression = new UnaryExpression("IS_NULL",
                referenceFactor.getReturnType(),
                1);
        unaryExpression.add(referenceFactor);
        NotExpression notExpression = new NotExpression();
        notExpression.setExpr(unaryExpression);
        return notExpression;
    }

    private int calculateLength(ExprNodeConstantDesc right, ExprNodeConstantDesc left) {
        int rightLength = (int) right.getValue();
        int leftLength = (int) left.getValue();
        return rightLength - leftLength + 1;
    }
}
