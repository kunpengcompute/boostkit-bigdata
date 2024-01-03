package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.FunctionExpression;
import com.huawei.boostkit.hive.expression.NotExpression;
import com.huawei.boostkit.hive.expression.ReferenceFactor;
import com.huawei.boostkit.hive.expression.TypeUtils;
import com.huawei.boostkit.hive.expression.UnaryExpression;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.List;

public class UpperExpressionProcessor implements ExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        List<ExprNodeDesc> children = node.getChildren();
        FunctionExpression functionExpression = new FunctionExpression(TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                "upper", children.size(), null);
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
}
