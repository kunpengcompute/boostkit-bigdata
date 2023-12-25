package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.NotExpression;
import com.huawei.boostkit.hive.expression.TypeUtils;
import com.huawei.boostkit.hive.expression.UnaryExpression;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.List;

public class NotNullExpressionProcessor implements ExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        List<ExprNodeDesc> children = node.getChildren();
        UnaryExpression unaryExpression = new UnaryExpression("IS_NULL",
                TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                children.size());
        for (ExprNodeDesc child : children) {
            if (child instanceof ExprNodeGenericFuncDesc) {
                unaryExpression.add(ExpressionUtils.build((ExprNodeGenericFuncDesc) child, inspector));
            } else {
                unaryExpression.add(ExpressionUtils.createNode(child, inspector));
            }
        }
        if (node.getGenericUDF() instanceof GenericUDFOPNotNull) {
            NotExpression notExpression = new NotExpression();
            notExpression.setExpr(unaryExpression);
            return notExpression;
        }
        return unaryExpression;
    }
}
