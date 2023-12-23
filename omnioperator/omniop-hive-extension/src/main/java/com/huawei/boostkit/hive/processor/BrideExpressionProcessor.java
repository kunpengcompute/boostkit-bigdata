package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.CastFunctionExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinsepector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;

import java.util.ArrayList;
import java.util.List;

public class BrideExpressionProcessor implements ExpressionProcessor {
    public static List<String> SUPPORT_BRIDGE_UDF = new ArrayList<>() {
        {
            add("UDFToString");
            add("UDFToDouble");
            add("UDFToInteger");
            add("UDFToLong");
            add("UDFToShort");
            add("UDFToByte");
            add("UDFToBoolean");
            add("substr");
        }
    };

    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        BaseExpression leaf = null;
        if (node.getGenericUDF().getUdfName().equals("UDFToString")
                && node.getChildren().get(0).getTypeInfo() instanceof BaseCharTypeInfo) {
            leaf = createNode(node.getChildren().get(0), inspector);
        } else if (node.getGenericUDF().getUdfName().equals("like")) {
            LikeExpressionProcessor likeExpressionProcessor = new LikeExpressionProcessor();
            leaf = likeExpressionProcessor.process(node, operator, inspector);
        } else if (node.getGenericUDF().getUdfName().equals("substr")) {
            SubstrExpressionProcessor substrExpressionProcessor = new SubstrExpressionProcessor();
            leaf = substrExpressionProcessor.process(node, operator, inspector);
        } else if (node.getGenericUDF().getUdfName().equals("UDFToDouble")
                || node.getGenericUDF().getUdfName().equals("UDFToInteger")
                || node.getGenericUDF().getUdfName().equals("UDFToLong")
                || node.getGenericUDF().getUdfName().equals("UDFToShort")
                || node.getGenericUDF().getUdfName().equals("UDFToByte")
                || node.getGenericUDF().getUdfName().equals("UDFToBoolean")
                || node.getGenericUDF().getUdfName().equals("UDFToString")) {
            leaf = createNode(node.getChildren().get(0), inspector);
            CastFunctionExpression functionExpression = new CastFunctionExpression(
                    TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo(), null, null, null));
            return ExpressionUtils.optimizeCast(leaf, functionExpression);
        }
        return leaf;
    }

    private BaseExpression createNode(ExprNodeDesc nodeDesc, ObjectInspector inspector) {
        if (nodeDesc instanceof ExprNodeGenericFuncDesc) {
            return ExpressionUtils.build((ExprNodeGenericFuncDesc) nodeDesc, inspector);
        } else {
            return ExpressionUtils.createNode(nodeDesc, inspector);
        }
    }
}
