package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.CastFunctionExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;

public class CastExpressionProcessor implements ExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        List<ExprNodeDesc> children = node.getChildren();
        TypeInfo typeInfo = node.getTypeInfo();
        Integer precision = null;
        Integer scale = null;
        if (node.getTypeInfo() instanceof DecimalTypeInfo) {
            precision = ((DecimalTypeInfo) node.getTypeInfo()).getPrecision();
            scale = ((DecimalTypeInfo) node.getTypeInfo()).getScale();
        }

        int width = TypeUtils.getCharWidth(node);

        CastFunctionExpression castFunctionExpression = new CastFunctionExpression(
                TypeUtils.convertHiveTypeToOmniType(typeInfo),
                width,
                precision,
                scale);
        for (ExprNodeDesc child : children) {
            if (child instanceof ExprNodeGenericFuncDesc) {
                castFunctionExpression.add(ExpressionUtils.build((ExprNodeGenericFuncDesc) child, inspector));
            } else {
                castFunctionExpression.add(ExpressionUtils.createNode(child, inspector));
            }
        }
        return castFunctionExpression;
    }
}
