package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.NotExpression;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class NotExpressionProcessor implements ExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        ExprNodeDesc exprNodeDesc = node.getChildren().get(0);
        NotExpression notExpression = new NotExpression();
        if (exprNodeDesc instanceof ExprNodeGenericFuncDesc) {
            notExpression.setExpr(ExpressionUtils.build((ExprNodeGenericFuncDesc) exprNodeDesc, inspector));
        } else {
            notExpression.setExpr(ExpressionUtils.createNode(exprNodeDesc, inspector));
        }
        return notExpression;
    }
}
