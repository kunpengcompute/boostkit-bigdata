/*
 * Copyright (C) 2023-2024. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import com.huawei.boostkit.hive.processor.LowerExpressionProcessor;
import com.huawei.boostkit.hive.processor.LengthExpressionProcessor;
import com.huawei.boostkit.hive.processor.InstrExpressionProcessor;

import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.type.Decimal64DataType;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCoalesce;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFConcat;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLikeAll;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLikeAny;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPDivide;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMinus;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMod;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMultiply;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPPlus;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFRound;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDecimal;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLower;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLength;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFInstr;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

public class ExpressionUtils {
    public static final Map<Class<? extends GenericUDF>, ExpressionProcessor> UDF_TO_PROCESSOR =
        new HashMap<Class<? extends GenericUDF>, ExpressionProcessor>() {
            {
                put(GenericUDFBetween.class, new BetweenExpressionProcessor());
                put(GenericUDFBridge.class, new BridgeExpressionProcessor());
                put(GenericUDFIn.class, new InExpressionProcessor());
                put(GenericUDFLikeAll.class, new LikeAllExpressionProcessor());
                put(GenericUDFLikeAny.class, new LikeAnyExpressionProcessor());
                put(GenericUDFOPNot.class, new NotExpressionProcessor());
                put(GenericUDFOPNotNull.class, new NotNullExpressionProcessor());
                put(GenericUDFOPNull.class, new NotNullExpressionProcessor());
                put(GenericUDFConcat.class, new ConcatExpressionProcessor());
                put(GenericUDFOPMinus.class, new ArithmeticExpressionProcessor());
                put(GenericUDFOPPlus.class, new ArithmeticExpressionProcessor());
                put(GenericUDFOPMultiply.class, new ArithmeticExpressionProcessor());
                put(GenericUDFOPDivide.class, new ArithmeticExpressionProcessor());
                put(GenericUDFOPMod.class, new ArithmeticExpressionProcessor());
                put(GenericUDFWhen.class, new CaseWhenExpressionProcessor());
                put(GenericUDFAbs.class, new AbsExpressionProcessor());
                put(GenericUDFOPEqual.class, new ComputeExpressionProcessor());
                put(GenericUDFOPEqualOrLessThan.class, new ComputeExpressionProcessor());
                put(GenericUDFOPEqualOrGreaterThan.class, new ComputeExpressionProcessor());
                put(GenericUDFOPGreaterThan.class, new ComputeExpressionProcessor());
                put(GenericUDFOPLessThan.class, new ComputeExpressionProcessor());
                put(GenericUDFOPNotEqual.class, new ComputeExpressionProcessor());
                put(GenericUDFCoalesce.class, new CoalesceExpressionProcessor());
                put(GenericUDFToDecimal.class, new CastExpressionProcessor());
                put(GenericUDFRound.class, new RoundExpressionProcessor());
                put(GenericUDFOPAnd.class, new LogicExpressionProcessor());
                put(GenericUDFOPOr.class, new LogicExpressionProcessor());
                put(GenericUDFTimestamp.class, new TimestampExpressionProcessor());
                put(GenericUDFUpper.class, new UpperExpressionProcessor());
                put(GenericUDFLength.class, new LengthExpressionProcessor());
                put(GenericUDFLower.class, new LowerExpressionProcessor());
                put(GenericUDFInstr.class, new InstrExpressionProcessor());
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

    public static BaseExpression buildSimplify(ExprNodeGenericFuncDesc exprNode, ObjectInspector inspector) {
        BridgeExpressionProcessor expressionProcessor =
                (BridgeExpressionProcessor) UDF_TO_PROCESSOR.get(GenericUDFBridge.class);
        expressionProcessor.setSimplify(true);
        BaseExpression build = build(exprNode, inspector);
        expressionProcessor.setSimplify(false);
        return build;
    }

    public static BaseExpression createLiteralNode(ExprNodeDesc next) {
        return createLiteralNode(false, next);
    }

    public static BaseExpression createLiteralNode(boolean hasDecimal128, ExprNodeDesc next) {
        BaseExpression leaf;
        Object value = ((ExprNodeConstantDesc) next).getValue();
        int omniType = TypeUtils.convertHiveTypeToOmniType(next.getTypeInfo());
        if (omniType == Decimal128DataType.DECIMAL128.getId().toValue()
                || omniType == Decimal64DataType.DECIMAL64.getId().toValue()) {
            if (hasDecimal128) {
                omniType = Decimal128DataType.DECIMAL128.getId().toValue();
            }
            int scale = ((DecimalTypeInfo) next.getTypeInfo()).getScale();
            Object realValue;
            if (value == null) {
                realValue = null;
            } else if (omniType == Decimal128DataType.DECIMAL128.getId().toValue()) {
                realValue = new BigInteger(((HiveDecimal) value).bigIntegerBytesScaled(scale)).toString();
            } else {
                realValue = ((HiveDecimal) value).scaleByPowerOfTen(scale).longValue();
            }
            leaf = new DecimalLiteral(realValue, omniType, ((DecimalTypeInfo) next.getTypeInfo()).getPrecision(),
                    scale);
        } else {
            leaf = new LiteralFactor<>("LITERAL", null, null, TypeUtils.getLiteralValue(value, next.getTypeInfo()),
                    TypeUtils.getCharWidth(next), omniType);
        }
        return leaf;
    }

    @Nonnull
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
        return createNode(false, exprNodeDesc, inspector);
    }

    public static BaseExpression createNode(boolean hasDecimal128, ExprNodeDesc exprNodeDesc,
                                            ObjectInspector inspector) {
        BaseExpression baseExpression = null;
        if (exprNodeDesc instanceof ExprNodeColumnDesc) {
            baseExpression = ExpressionUtils.createReferenceNode(exprNodeDesc, inspector);
        } else if (exprNodeDesc instanceof ExprNodeConstantDesc) {
            baseExpression = ExpressionUtils.createLiteralNode(hasDecimal128, exprNodeDesc);
        } else if (exprNodeDesc instanceof ExprNodeFieldDesc) {
            baseExpression = ExpressionUtils.createReferenceNode(((ExprNodeFieldDesc) exprNodeDesc).getDesc(),
                    inspector);
        }
        return baseExpression;
    }

    public static BaseExpression preCast(BaseExpression castExpression, ExprNodeDesc castNodeDesc,
                                         ExprNodeDesc comparedNode) {
        TypeInfo baseTypeInfo = comparedNode.getTypeInfo();
        if (castNodeDesc instanceof ExprNodeConstantDesc && baseTypeInfo instanceof DecimalTypeInfo
                && ((ExprNodeConstantDesc) castNodeDesc).getValue().equals(0)) {
            if (((DecimalTypeInfo) baseTypeInfo).getPrecision() > 18) {
                return new DecimalLiteral("0", TypeUtils.convertHiveTypeToOmniType(baseTypeInfo),
                        ((DecimalTypeInfo) baseTypeInfo).getPrecision(), ((DecimalTypeInfo) baseTypeInfo).getScale());
            } else {
                return new DecimalLiteral(0L, TypeUtils.convertHiveTypeToOmniType(baseTypeInfo),
                        ((DecimalTypeInfo) baseTypeInfo).getPrecision(), ((DecimalTypeInfo) baseTypeInfo).getScale());
            }
        }
        Integer precision = null;
        Integer scale = null;
        if (baseTypeInfo instanceof DecimalTypeInfo) {
            precision = ((DecimalTypeInfo) baseTypeInfo).getPrecision();
            scale = ((DecimalTypeInfo) baseTypeInfo).getScale();
        }

        CastFunctionExpression castFunctionExpression = new CastFunctionExpression(
                TypeUtils.convertHiveTypeToOmniType(baseTypeInfo), TypeUtils.getCharWidth(comparedNode), precision,
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
            ((DecimalLiteral) expression).setDataType(castFunctionExpression.getReturnType());
            if (scale >= toCastScale) {
                return expression;
            }
            Object value = ((DecimalLiteral) expression).getValue();
            ((DecimalLiteral) expression).setPrecision(toCastPrecision);
            ((DecimalLiteral) expression).setScale(toCastScale);
            if (value instanceof Long) {
                long newValue = ((long) Math.pow(10, toCastScale - scale)) * (long) value;
                if (toCastPrecision <= 18) {
                    ((DecimalLiteral) expression).setValue(newValue);
                } else {
                    ((DecimalLiteral) expression).setValue(String.valueOf(newValue));
                }
            } else {
                BigDecimal decimalValue = new BigDecimal((String) value);
                BigDecimal newValue = decimalValue.multiply(BigDecimal.TEN.pow(toCastScale - scale));
                if (toCastPrecision <= 18) {
                    ((DecimalLiteral) expression).setValue(Long.valueOf(newValue.toString()));
                } else {
                    ((DecimalLiteral) expression).setValue(newValue.toString());
                }
            }
            return expression;
        }
        castFunctionExpression.add(expression);
        return castFunctionExpression;
    }

    public static BaseExpression wrapNotNullExpression(ReferenceFactor referenceFactor) {
        UnaryExpression unaryExpression = new UnaryExpression("IS_NULL",
                referenceFactor.getReturnType(), 1);
        unaryExpression.add(referenceFactor);
        NotExpression notExpression = new NotExpression();
        notExpression.setExpr(unaryExpression);
        return notExpression;
    }
}
