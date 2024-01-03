package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.NotExpression;
import com.huawei.boostkit.hive.expression.ReferenceFactor;
import com.huawei.boostkit.hive.expression.UnaryExpression;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public interface ExpressionProcessor {
    BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector);

    default BaseExpression wrapNotNullExpression(ReferenceFactor referenceFactor) {
        UnaryExpression unaryExpression = new UnaryExpression("IS_NULL",
                referenceFactor.getReturnType(),
                1);
        unaryExpression.add(referenceFactor);
        NotExpression notExpression = new NotExpression();
        notExpression.setExpr(unaryExpression);
        return notExpression;
    }
}
