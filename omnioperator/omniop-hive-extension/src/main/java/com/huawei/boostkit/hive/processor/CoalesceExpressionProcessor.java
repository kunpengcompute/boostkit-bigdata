package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.CoalesceExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;

public class CoalesceExpressionProcessor implements ExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        List<ExprNodeDesc> children = node.getChildren();
        TypeInfo typeInfo = node.getTypeInfo();
        Integer precision = null;
        Integer scale = null;
        if (typeInfo instanceof DecimalTypeInfo) {
            precision = ((DecimalTypeInfo) typeInfo).getPrecision();
            scale = ((DecimalTypeInfo) typeInfo).getScale();
        }

        CoalesceExpression coalesceExpression = new CoalesceExpression(
                TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                TypeUtils.calculateVarcharLength(node), precision, scale);
        for (ExprNodeDesc child : children) {
            BaseExpression childNode;
            if (child instanceof ExprNodeGenericFuncDesc) {
                childNode = ExpressionUtils.build((ExprNodeGenericFuncDesc) child, inspector);
            } else {
                childNode = ExpressionUtils.createNode(child, inspector);
            }

            if (node.getTypeInfo() != child.getTypeInfo()) {
                childNode = ExpressionUtils.preCast(childNode, child, node);
            }
            coalesceExpression.add(childNode);
        }
        return coalesceExpression;
    }
}
