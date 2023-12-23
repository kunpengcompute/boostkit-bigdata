package com.huawei.boostkit.hive.processor;


import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.ConditionExpression;
import com.huawei.boostkit.hive.expression.DecimalLiteral;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.LiteralFactor;
import com.huawei.boostkit.hive.expression.TypeUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinsepector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class CaseWhenExpressionProcessor implements ExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        List<ExprNodeDesc> children = node.getChildren();
        Queue<ExprNodeDesc> queue = new LinkedBlockingQueue<>(children);
        TypeInfo typeInfo = node.getTypeInfo();
        Integer precision = null;
        Integer scale = null;
        if (typeInfo instanceof DecimalTypeInfo) {
            precision = ((DecimalTypeInfo) typeInfo).getPrecision();
            scale = ((DecimalTypeInfo) typeInfo).getScale();
        }

        ConditionExpression conditionExpression = new ConditionExpression(
                TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                TypeUtils.calculateVarcharLength(node),
                precision,
                scale);

        while (!queue.isEmpty()) {
            ExprNodeDesc poll = queue.poll();
            conditionExpression.add(createConditionExpression(node, poll, inspector));
            poll = queue.poll();
            conditionExpression.add(createConditionExpression(node, poll, inspector));
            if (queue.size() > 1) {
                ConditionExpression temp = conditionExpression.copyBase();
                conditionExpression.add(temp);
            } else if (queue.size() == 1) {
                conditionExpression.add(createConditionExpression(node, queue.poll(), inspector));
            } else {
                LiteralFactor<Void> literalFactor = new LiteralFactor<>("LITERAL",
                        null, null, null,
                        TypeUtils.getCharWidth(node), TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()));
                conditionExpression.add(literalFactor);
            }
        }
        return conditionExpression;
    }

    private BaseExpression createConditionExpression(ExprNodeGenericFuncDesc root, ExprNodeDesc nodeDesc, ObjectInspector inspector) {
        if (nodeDesc instanceof ExprNodeGenericFuncDesc) {
            return ExpressionUtils.build((ExprNodeGenericFuncDesc) nodeDesc, inspector);
        } else if (nodeDesc instanceof ExprNodeConstantDesc) {
            if (nodeDesc.getTypeInfo().getTypeName().equals("void")) {
                if (root.getTypeInfo() instanceof DecimalTypeInfo) {
                    return new DecimalLiteral(null, TypeUtils.convertHiveTypeToOmniType(root.getTypeInfo()),
                            ((DecimalTypeInfo) root.getTypeInfo()).getPrecision(),
                            ((DecimalTypeInfo) root.getTypeInfo()).getScale());
                }
                return new LiteralFactor<Void>("LITERAL",
                        null, null, null, TypeUtils.calculateVarcharLength(root),
                        TypeUtils.convertHiveTypeToOmniType(root.getTypeInfo()));
            }
            if (root.getTypeInfo() instanceof DecimalTypeInfo && ((ExprNodeConstantDesc) nodeDesc).getValue().equals(0)) {
                return new DecimalLiteral("0", TypeUtils.convertHiveTypeToOmniType(root.getTypeInfo()),
                        ((DecimalTypeInfo) root.getTypeInfo()).getPrecision(),
                        ((DecimalTypeInfo) root.getTypeInfo()).getScale());
            }
            return ExpressionUtils.createNode(nodeDesc, inspector);
        } else {
            return ExpressionUtils.createNode(nodeDesc, inspector);
        }
    }
}
