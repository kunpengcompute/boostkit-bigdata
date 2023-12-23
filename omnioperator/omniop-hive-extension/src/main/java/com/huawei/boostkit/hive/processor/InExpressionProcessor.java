package com.huawei.boostkit.hive.processor;

import static com.huawei.boostkit.hive.expression.ExpressionUtils.getExprNodeColumnDesc;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;
import com.huawei.boostkit.hive.expression.UnaryExpression;

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinsepector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;

public class InExpressionProcessor implements ExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        List<ExprNodeDesc> children = node.getChildren();
        UnaryExpression in = new UnaryExpression("IN", TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                children.size());
        TypeInfo nodeColumnTypeInfo = getExprNodeColumnDesc(node).get(0).getTypeInfo();
        for (ExprNodeDesc child : children) {
            if (child instanceof ExprNodeGenericFuncDesc) {
                in.add(ExpressionUtils.build((ExprNodeGenericFuncDesc) child, inspector));
            } else if (child instanceof ExprNodeColumnDesc) {
                in.add(ExpressionUtils.createNode(child, inspector));
            } else if (child instanceof ExprNodeConstantDesc) {
                if (!child.getTypeInfo().equals(nodeColumnTypeInfo)) {
                    child = new ExprNodeConstantDesc(nodeColumnTypeInfo, ((ExprNodeConstantDesc) child).getValue());
                }
                in.add(ExpressionUtils.createLiteralNode(child));
            }
        }
        return in;
    }
}
