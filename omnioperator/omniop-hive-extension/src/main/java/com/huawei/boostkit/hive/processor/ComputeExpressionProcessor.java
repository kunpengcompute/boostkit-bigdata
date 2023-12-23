package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.CompareExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinsepector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;

public class ComputeExpressionProcessor implements ExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        List<ExprNodeDesc> children = node.getChildren();
        conjureNodeType(children);
        CompareExpression compareExpression = new CompareExpression("BINARY",
                TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                operator);
        for (ExprNodeDesc child : children) {
            if (child instanceof ExprNodeGenericFuncDesc) {
                compareExpression.add(ExpressionUtils.build((ExprNodeGenericFuncDesc) child, inspector));
            } else {
                compareExpression.add(ExpressionUtils.createNode(child, inspector));
            }
        }
        return compareExpression;
    }

    private void conjureNodeType(List<ExprNodeDesc> nodes) {
        ExprNodeDesc markNode = null;
        TypeInfo replaceTypeInfo = null;
        for (ExprNodeDesc node : nodes) {
            if (node instanceof ExprNodeConstantDesc) {
                markNode = node;
            } else {
                replaceTypeInfo = node.getTypeInfo();
            }
        }
        if (markNode != null && replaceTypeInfo != null) {
            markNode.setTypeInfo(replaceTypeInfo);
        }
    }
}
