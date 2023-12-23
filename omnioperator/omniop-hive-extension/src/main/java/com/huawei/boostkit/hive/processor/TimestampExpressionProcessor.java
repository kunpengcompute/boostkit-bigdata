package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinsepector.ObjectInspector;

public class TimestampExpressionProcessor implements ExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        ExprNodeDesc exprNodeDesc = node.getChildren().get(0);
        if (exprNodeDesc instanceof ExprNodeGenericFuncDesc) {
            return ExpressionUtils.build((ExprNodeGenericFuncDesc) child, inspector);
        } else {
            return ExpressionUtils.createNode(child, inspector);
        }
    }
}
