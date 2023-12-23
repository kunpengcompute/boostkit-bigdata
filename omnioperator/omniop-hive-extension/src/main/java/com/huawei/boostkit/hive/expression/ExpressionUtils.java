package com.huawei.boostkit.hive.expression;

import com.huawei.boostkit.hive.processor.AbsExpressionProcessor;
import com.huawei.boostkit.hive.processor.ArithmeticExpressionProcessor;
import com.huawei.boostkit.hive.processor.BetweenExpressionProcessor;
import com.huawei.boostkit.hive.processor.BridgeExpressionProcessor;
import com.huawei.boostkit.hive.processor.CaseWhenExpressionProcessor;
import com.huawei.boostkit.hive.processor.CastExpressionProcessor;
import com.huawei.boostkit.hive.processor.CoalesceExpressionProcessor;
import com.huawei.boostkit.hive.processor.ComputeExpressionProcessor;
import com.huawei.boostkit.hive.processor.ConcatExpressionProcessor;
import com.huawei.boostkit.hive.processor.ExpressionProcessor;
import com.huawei.boostkit.hive.processor.InExpressionProcessor;
import com.huawei.boostkit.hive.processor.LikeAllExpressionProcessor;
import com.huawei.boostkit.hive.processor.LikeAnyExpressionProcessor;
import com.huawei.boostkit.hive.processor.LogicExpressionProcessor;
import com.huawei.boostkit.hive.processor.NotExpressionProcessor;
import com.huawei.boostkit.hive.processor.NotNullExpressionProcessor;
import com.huawei.boostkit.hive.processor.RoundExpressionProcessor;
import com.huawei.boostkit.hive.processor.TimestampExpressionProcessor;
import com.huawei.boostkit.hive.processor.UpperExpressionProcessor;

import nove.hetu.omniruntime.type.Decimal128DataType;
import nove.hetu.omniruntime.type.Decimal64DataType;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.ql.plan.ExprNodeDesc;
import org.apache.hadoop.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.ql.udf.generic.GenericUDF;
import org.apache.hadoop.ql.udf.generic.GenericUDFAbs;
import org.apache.hadoop.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.ql.udf.generic.GenericUDFCoalesce;
import org.apache.hadoop.ql.udf.generic.GenericUDFConcat;
import org.apache.hadoop.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.ql.udf.generic.GenericUDFLikeAll;
import org.apache.hadoop.ql.udf.generic.GenericUDFLikeAny;
import org.apache.hadoop.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.ql.udf.generic.GenericUDFOPDivide;
import org.apache.hadoop.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.ql.udf.generic.GenericUDFOPMinus;
import org.apache.hadoop.ql.udf.generic.GenericUDFOPMod;
import org.apache.hadoop.ql.udf.generic.GenericUDFOPMultiply;
import org.apache.hadoop.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.ql.udf.generic.GenericUDFOPPLus;
import org.apache.hadoop.ql.udf.generic.GenericUDFRound;
import org.apache.hadoop.ql.udf.generic.GenericUDFTimestamp;
import org.apache.hadoop.ql.udf.generic.GenericUDFToDecimal;
import org.apache.hadoop.ql.udf.generic.GenericUDFUpper;
import org.apache.hadoop.ql.udf.generic.GenericUDFWhen;
import org.apache.hadoop.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.serde2.typeinfo.TypeInfo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

public class ExpressionUtils {
    public static final Map<Class<? extends GenericUDF>, ExpressionProcessor> UDF_TO_PROCESSOR = new HashMap<Class<? extends GenericUDF>, ExpressionProcessor>() {
        {

        }
    };

    public static boolean isSupportUDF(GenericUDF udf) {
        if (udf instanceof GenericUDFBridge) {
            return BridgeExpressionProcessor.SUPPORT_BRIDGE_UDF.contains(udf.getUdfName());
        }
        return UDF_TO_PROCESSOR.containsKey(udf.getClass());
    }

    public static BaseExpression build(ExprNodeGenericFuncDesc exprNode, ObjectInspector inspector) {
        BaseExpression root;
        ExpressionProcessor expressionProcessor = UDF_TO_PROCESSOR.get(exprNode.getGenericUDF().getClass());
        if (expressionProcessor != null) {
            root = expressionProcessor.process(exprNode, TypeUtils.getOperatorDesc(exprNode.getGenericUDF()),
                    inspector);
        } else {
            throw new RuntimeException("no udf processor!");
        }
        return root;
    }

    public static BaseExpression createLiteralNode(ExprNodeDesc next) {
        BaseExpression leaf;
        Object value = ((ExprNodeConstantDesc) next).getValue();
        int omniType = TypeUtils.convertHiveTypeToOmniType(next.getTypeInfo());
        if (omniType == Decimal128DataType.DECIMAL128.getId().toValue()) {
            int scale = ((DecimalTypeInfo) next.getTypeInfo()).getScale();
            String realValue;
            if (value == null) {
                realValue = null;
            } else {
                realValue = new BigInteger(((HiveDecimal) value).bigIntegerBytesScaled(scale)).toString();
            }
            leaf = new DecimalLiteral(realValue, Decimal128DataType.getId().toValue(),
                    ((DecimalTypeInfo) next.getTypeInfo()).getPrecision(), scale);
        } else {
            leaf = new LiteralFactor<>("LITERAL", null, null, TypeUtils.getLiteralValue(value, next.getTypeInfo()),
                    TypeUtils.getCharWidth(next), omniType);
        }
        return leaf;
    }

    public static BaseExpression createReferenceNode(ExprNodeDesc next, ObjectInspector inspector) {
        BaseExpression leaf;
        String name;
        if (next instanceof ExprNodeGenericFuncDesc) {
            name = next.getChildren().get(0).getExprString();
        } else {
            name = next.getExprString();
        }
        int fieldID = 0;
        if (inspector != null) {
            fieldID = ((StructObjectInspector) inspector).getStructFieldRef(name).getFieldID();
        }

        int omniType = TypeUtils.convertHiveTypeToOmniType(next.getTypeInfo());
        if (omniType == Decimal128DataType.DECIMAL128.getId().toValue()
                || omniType == Decimal64DataType.DECIMAL64.getId().toValue()) {
            leaf = new DecimalReference(fieldID, omniType, ((DecimalTypeInfo) next.getTypeInfo()).getPrecision(),
                    ((DecimalTypeInfo) next.getTypeInfo()).getScale());
        } else {
            leaf = new ReferenceFactor("FIELD_REFERENCE", null, null, fieldID, TypeUtils.getCharWidth(next), omniType);
        }
        return leaf;
    }

    public static BaseExpression createNode(ExprNodeDesc exprNodeDesc, ObjectInspector inspector) {
        if (exprNodeDesc instanceof ExprNodeColumnDesc) {
            return ExpressionUtils.createReferenceNode(exprNodeDesc, inspector);
        } else if (exprNodeDesc instanceof ExprNodeConstantDesc) {
            return ExpressionUtils.createLiteralNode(exprNodeDesc);
        }
        return null;
    }

    public static BaseExpression preCast(BaseExpression castExpression, ExprNodeDesc castNodeDesc, ExprNodeDesc comparedNode) {
        TypeInfo baseTypeInfo = comparedNode.getTypeInfo();
        if (castNodeDesc instanceof ExprNodeConstantDesc) {
            if (baseTypeInfo instanceof DecimalTypeInfo && ((ExprNodeConstantDesc) castNodeDesc).getValue().equals(0)) {
                return new DecimalLiteral("0", TypeUtils.convertHiveTypeToOmniType(baseTypeInfo),
                        ((DecimalTypeInfo) baseTypeInfo).getPrecision(), ((DecimalTypeInfo) baseTypeInfo).getScale());
            }
        }
        Integer precison = null;
        Integer scale = null;
        if (baseTypeInfo instanceof DecimalTypeInfo) {
            precison = ((DecimalTypeInfo) baseTypeInfo).getPrecision();
            scale = ((DecimalTypeInfo) baseTypeInfo).getScale();
        }

        CastFunctionExpression castFunctionExpression = new CastFunctionExpression(
                TypeUtils.convertHiveTypeToOmniType(baseTypeInfo), TypeUtils.getCharWidth(comparedNode), precison,
                scale);
        return ExpressionUtils.optimizeCast(castExpression, castFunctionExpression);
    }


    public static List<ExprNodeDesc> getExprNodeColumnDesc(ExprNodeDesc exprNodeDesc) {
        List<ExprNodeDesc> exprNodeColumnDesc = new ArrayList<>();
        dealChildren(exprNodeDesc, exprNodeColumnDesc);
        return exprNodeColumnDesc;
    }

    private static void dealChildren(ExprNodeDesc exprNodeDesc, List<ExprNodeDesc> exprNodeColumnDesc) {
        if (exprNodeDesc instanceof ExprNodeColumnDesc) {
            exprNodeColumnDesc.add(exprNodeDesc);
            return;
        }
        if (exprNodeDesc.getChildren() == null) {
            return;
        }
        exprNodeDesc.getChildren().forEach(child -> dealChildren(child, exprNodeColumnDesc));
    }

    public static BaseExpression optimizeCast(BaseExpression expression,
                                              CastFunctionExpression castFunctionExpression) {
        if (expression instanceof DecimalLiteral) {
            Integer toCastPrecision = castFunctionExpression.getPrecision();
            Integer toCastScale = castFunctionExpression.getScale();
            Integer scale = ((DecimalLiteral) expression).getScale();
            if (scale >= toCastScale) {
                return expression;
            }
            String value = (String) ((DecimalLiteral) expression).getValue();
            ((DecimalLiteral) expression).setPrecision(toCastPrecision);
            ((DecimalLiteral) expression).setScale(toCastScale);
            BigDecimal decimalValue = new BigDecimal(value);
            BigDecimal newValue = decimalValue.multiply(BigDecimal.TEN.pow(toCastScale - scale));
            ((DecimalLiteral) expression).setValue(newValue.toString());
            return expression;
        }
        castFunctionExpression.add(expression);
        return castFunctionExpression;
    }
}
