package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinsepector.ObjectInspector;

public class NotExpressionProcessor implements ExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        ExprNodeDesc exprNodeDesc = node.getChildren().get(0);
        ExpressionProcessor expressionProcessor = ExpressionUtils.UDF_TO_PROCESSOR.get(
                ((ExprNodeGenericFuncDesc) exprNodeDesc).getGenericUDF().getClass());
        BaseExpression process = null;
        if (expressionProcessor != null) {
            process = expressionProcessor.process((ExprNodeGenericFuncDesc) exprNodeDesc, operator, inspector);
        }
        return process;
    }
}
