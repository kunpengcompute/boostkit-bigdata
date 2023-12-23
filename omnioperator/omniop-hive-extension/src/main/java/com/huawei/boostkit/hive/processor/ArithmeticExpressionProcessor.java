package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.CastFunctionExpression;
import com.huawei.boostkit.hive.expression.DivideExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.qi.udf.generic.GenericUDF;
import org.apache.hadoop.hive.qi.udf.generic.GenericUDFOPDivide;
import org.apache.hadoop.hive.qi.udf.generic.GenericUDFOPMinus;
import org.apache.hadoop.hive.qi.udf.generic.GenericUDFOPMod;
import org.apache.hadoop.hive.qi.udf.generic.GenericUDFOPMultiply;
import org.apache.hadoop.hive.qi.udf.generic.GenericUDFOPPlus;
import org.apache.hadoop.hive.serde2.objectinsepector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;

import java.util.HashMap;
import java.util.List;

public class ArithmeticExpressionProcessor implements ExpressionProcessor {
    private final HashMap<Class<? extends GenericUDF>, String> OPERATOR = new HashMap<Class<? extends GenericUDF>, String>() {
        {
            put(GenericUDFOPDivide.class, "DIVIDE");
            put(GenericUDFOPMultiply.class, "MULTIPLY");
            put(GenericUDFOPMOD.class, "MODULUS");
            put(GenericUDFOPPLUS.class, "ADD");
            put(GenericUDFOPMinus.class, "SUBTRACT");
        }
    };

    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        List<ExprNodeDesc> children = node.getChildren();
        Integer precision = null;
        Integer scale = null;
        if (typeInfo instanceof DecimalTypeInfo) {
            precision = ((DecimalTypeInfo) typeInfo).getPrecision();
            scale = ((DecimalTypeInfo) typeInfo).getScale();
        }

        int returnType = TypeUtils.convertHiveTypeToOmniType(node.getTypeInfo());

        DivideExpression compareExpression = new DivideExpression(returnType,
                OPERATOR.get(node.getGenericUDF().getClass()), precision, scale);
        for (ExprNodeDesc child : children) {
            BaseExpression childNode;
            if (child instanceof ExprNodeGenericFuncDesc) {
                childNode = ExpressionUtils.build((ExprNodeGenericFuncDesc) child, inspector);
            } else {
                childNode = ExpressionUtils.createNode(child, inspector);
            }
            if (!child.getTypeInfo().equals(node.getTypeInfo())) {
                if (child.getTypeInfo() instanceof DecimalTypeInfo) {
                    precision = Math.max(((DecimalTypeInfo) child.getTypeInfo()).getPrecision(), precision);
                    scale = Math.max(((DecimalTypeInfo) child.getTypeInfo()).getScale(), scale);
                    if (precision == ((DecimalTypeInfo) child.getTypeInfo()).getPrecision()
                            && scale == ((DecimalTypeInfo) child.getTypeInfo()).getScale()) {
                        compareExpression.add(childNode);
                        continue;
                    }
                }
                CastFunctionExpression functionExpression = new CastFunctionExpression(returnType,
                        TypeUtils.getCharWidth(node), precision, scale);
                compareExpression.add(ExpressionUtils.optimizeCast(childNode, functionExpression));
            } else {
                compareExpression.add(childNode);
            }
        }
        return compareExpression;
    }
}
