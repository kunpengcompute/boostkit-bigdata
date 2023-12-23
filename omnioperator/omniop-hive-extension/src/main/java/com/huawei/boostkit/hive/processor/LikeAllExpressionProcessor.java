package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.CompareExpression;
import com.huawei.boostkit.hive.expression.TypeUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinsepector.ObjectInspector;

import java.util.List;

public class LikeAllExpressionProcessor extends LikeExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        boolean isNot = operator.equals("NOT");
        List<ExprNodeDesc> children = node.getChildren();
        int group = (children.size() - 1) / 2;
        BaseExpression compareGroup = null;
        if (group > 0) {
            compareGroup = new CompareExpression("BINARY",
                    TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo()),
                    isNot ? "AND" : operator);
        }
        for (int i = 1; i < children.size(); i++) {
            BaseExpression like = buildLikeNode(node, i, inspector);
            if (isNot) {
                like = wrapNotExpression(like);
            }
            if (compareGroup != null) {
                compareGroup.add(like);
            } else {
                compareGroup = like;
            }
        }
        return compareGroup;
    }
}
