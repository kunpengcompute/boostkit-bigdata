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

import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_AVG;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_COUNT_ALL;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_COUNT_COLUMN;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_MAX;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_MIN;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_SUM;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_CHAR;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_DATE32;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_DATE64;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_DECIMAL128;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_DECIMAL64;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_DOUBLE;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_INT;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_LONG;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_SHORT;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_VARCHAR;

import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.constants.OmniWindowFrameBoundType;
import nova.hetu.omniruntime.constants.OmniWindowFrameType;
import nova.hetu.omniruntime.operator.OmniExprVerify;
import nova.hetu.omniruntime.type.BooleanDataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DataTypeSerializer;
import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.type.DoubleDataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.type.ShortDataType;
import nova.hetu.omniruntime.type.VarcharDataType;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFunctionDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLikeAll;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLikeAny;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.parquet.format.DecimalType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


public class TypeUtils {
    private static final Map<PrimitiveObjectInspector.PrimitiveCategory, DataType> HIVE_TO_OMNI_TYPE = new HashMap<PrimitiveObjectInspector.PrimitiveCategory, DataType>() {
        {
            put(PrimitiveObjectInspector.PrimitiveCategory.SHORT, ShortDataType.SHORT);
            put(PrimitiveObjectInspector.PrimitiveCategory.INT, IntDataType.INTEGER);
            put(PrimitiveObjectInspector.PrimitiveCategory.LONG, LongDataType.LONG);
            put(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN, BooleanDataType.BOOLEAN);
            put(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE, DoubleDataType.DOUBLE);
            put(PrimitiveObjectInspector.PrimitiveCategory.STRING, VarcharDataType.VARCHAR);
            put(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP, LongDataType.LONG);
            put(PrimitiveObjectInspector.PrimitiveCategory.DATE, IntDataType.INTEGER);
            put(PrimitiveObjectInspector.PrimitiveCategory.INTERVAL_DAY_TIME, LongDataType.LONG);
            put(PrimitiveObjectInspector.PrimitiveCategory.BYTE, ShortDataType.SHORT);
            put(PrimitiveObjectInspector.PrimitiveCategory.FLOAT, DoubleDataType.DOUBLE);
        }
    };

    private static final Map<Class<? extends GenericUDF>, String> HIVE_FUNCTION_TO_OP = new HashMap<Class<? extends GenericUDF>, String>() {
        {
            put(GenericUDFOPEqual.class, "EQUAL");
            put(GenericUDFOPAnd.class, "AND");
            put(GenericUDFOPGreaterThan.class, "GREATER_THAN");
            put(GenericUDFOPLessThan.class, "LESS_THAN");
            put(GenericUDFOPEqualOrGreaterThan.class, "GREATER_THAN_OR_EQUAL");
            put(GenericUDFOPEqualOrLessThan.class, "LESS_THAN_OR_EQUAL");
            put(GenericUDFOPNotEqual.class, "NOT_EQUAL");
            put(GenericUDFBetween.class, "AND");
            put(GenericUDFOPNot.class, "NOT");
            put(GenericUDFOPOr.class, "OR");
            put(GenericUDFLikeAny.class, "OR");
            put(GenericUDFLikeAll.class, "AND");
        }
    };

    public static final int DEFAULT_VARCHAR_LENGTH = 2000;
    private static final Map<String, FunctionType> HIVE_TO_OMNI_WINDOW_FUNCTION = new HashMap<String, FunctionType>() {
        {
            put("sum", OMNI_AGGREGATION_TYPE_SUM);
            put("count", OMNI_AGGREGATION_TYPE_COUNT_COLUMN);
            put("min", OMNI_AGGREGATION_TYPE_MIN);
            put("max", OMNI_AGGREGATION_TYPE_MAX);
            put("avg", OMNI_AGGREGATION_TYPE_AVG);
            put("rank", FunctionType.OMNI_WINDOW_TYPE_RANK);
            put("row_number", FunctionType.OMNI_WINDOW_TYPE_ROW_NUMBER);
        }
    };

    private static final Map<WindowingSpec.WindowType, OmniWindowFrameType> HIVE_TO_OMNI_WINDOW_FRAME_TYPE = new HashMap<WindowingSpec.WindowType, OmniWindowFrameType>() {
        {
            put(WindowingSpec.WindowType.RANGE, OmniWindowFrameType.OMNI_FRAME_TYPE_RANGE);
            put(WindowingSpec.WindowType.ROWS, OmniWindowFrameType.OMNI_FRAME_TYPE_ROWS);
        }
    };

    public static final Map<WindowingSpec.Direction, OmniWindowFrameBoundType> HIVE_TO_OMNI_WINDOW_FRAME_BOUND_TYPE = new HashMap<WindowingSpec.Direction, OmniWindowFrameBoundType>() {
        {
            put(WindowingSpec.Direction.PRECEDING, OmniWindowFrameBoundType.OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING);
            put(WindowingSpec.Direction.CURRENT, OmniWindowFrameBoundType.OMNI_FRAME_BOUND_CURRENT_ROW);
            put(WindowingSpec.Direction.FOLLOWING, OmniWindowFrameBoundType.OMNI_FRAME_BOUND_UNBOUNDED_FOLLOWING);
        }
    };

    public static final Map<PTFInvocationSpec.Order, Integer> HIVE_TO_OMNI_WINDOW_SORT_TYPE = new HashMap<PTFInvocationSpec.Order, Integer>() {
        {
            put(PTFInvocationSpec.Order.ASC, 1);
            put(PTFInvocationSpec.Order.DESC, 0);
        }
    };

    public static final Map<PTFInvocationSpec.NullOrder, Integer> HIVE_TO_OMNI_WINDOW_NULL_SORT_TYPE = new HashMap<PTFInvocationSpec.NullOrder, Integer>() {
        {
            put(PTFInvocationSpec.NullOrder.NULLS_FIRST, 1);
            put(PTFInvocationSpec.NullOrder.NULLS_LAST, 0);
        }
    };

    private static final Map<String, FunctionType> HIVE_TO_OMNI_AGG_FUNCTION = new HashMap<String, FunctionType>() {
        {
            put("sum", OMNI_AGGREGATION_TYPE_SUM);
            put("count", OMNI_AGGREGATION_TYPE_COUNT_COLUMN);
            put("min", OMNI_AGGREGATION_TYPE_MIN);
            put("max", OMNI_AGGREGATION_TYPE_MAX);
            put("avg", OMNI_AGGREGATION_TYPE_AVG);
        }
    };

    public static String getOperatorDesc(GenericUDF genericUDF) {
        return HIVE_FUNCTION_TO_OP.get(genericUDF.getClass());
    }

    public static FunctionType getAggFunctionTypeFromName(AggregationDesc agg) {
        return HIVE_TO_OMNI_AGG_FUNCTION.get(agg.getGenericUDAFName());
    }

    public static FunctionType getWindowFunctionType(WindowFunctionDef windowFunctionDef) {
        String name = windowFunctionDef.getName();
        if (name.equals("count") && windowFunctionDef.getArgs() == null) {
            return OMNI_AGGREGATION_TYPE_COUNT_ALL;
        }
        return HIVE_TO_OMNI_WINDOW_FUNCTION.get(name);
    }


    public static OmniWindowFrameType getWindowFrameType(WindowingSpec.WindowType windowType) {
        return HIVE_TO_OMNI_WINDOW_FRAME_TYPE.get(windowType);
    }

    public static OmniWindowFrameBoundType getWindowFrameBoundType(WindowingSpec.Direction direction) {
        return HIVE_TO_OMNI_WINDOW_FRAME_BOUND_TYPE.get(direction);
    }

    public static int getWindowSortType(PTFInvocationSpec.Order order) {
        return HIVE_TO_OMNI_WINDOW_SORT_TYPE.get(order);
    }

    public static int getSortNullFirst(PTFInvocationSpec.NullOrder nullOrder) {
        return HIVE_TO_OMNI_WINDOW_NULL_SORT_TYPE.get(nullOrder);
    }

    public static int convertHiveTypeToOmniType(TypeInfo typeInfo) {
        return buildInputDataType(typeInfo).getId().toValue();
    }

    public static DataType buildInputDataType(TypeInfo typeInfo) {
        if (typeInfo instanceof DecimalTypeInfo) {
            return new Decimal128DataType(((DecimalTypeInfo) typeInfo).getPrecision(),
                    ((DecimalTypeInfo) typeInfo).getScale());
        } else if (typeInfo instanceof BaseCharTypeInfo) {
            return new VarcharDataType(((BaseCharTypeInfo) typeInfo).getLength());
        }
        return HIVE_TO_OMNI_TYPE.get(((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory());
    }

    public static String buildExpression(TypeInfo typeInfo, int colVal) {
        return buildExpression(typeInfo, convertHiveTypeToOmniType(typeInfo), colVal);
    }

    public static String buildExpression(TypeInfo typeInfo, int dataType, int colVal) {
        if (typeInfo instanceof BaseCharTypeInfo) {
            return String.format(
                    "{\"exprType\":\"FIELD_REFERENCE\"," + "\"dataType\":%d," + "\"colVal\":%d," + "\"width\":%d}",
                    dataType, colVal, ((BaseCharTypeInfo) typeInfo).getLength());
        } else if (typeInfo instanceof DecimalTypeInfo) {
            return String.format(
                    "{\"exprType\":\"FIELD_REFERENCE\"," + "\"dataType\":%d," + "\"colVal\":%d," + "\"precision\":%d,"
                            + "\"scale\":%d}",
                    dataType, colVal, ((DecimalTypeInfo) typeInfo).getPrecision(),
                    ((DecimalTypeInfo) typeInfo).getScale());
        } else if (typeInfo instanceof PrimitiveTypeInfo && typeInfo.getTypeName().equals("string")) {
            return String.format(
                    "{\"exprType\":\"FIELD_REFERENCE\"," + "\"dataType\":%d," + "\"colVal\":%d," + "\"width\":%d}",
                    dataType, colVal, DEFAULT_VARCHAR_LENGTH);
        }
        return String.format("{\"exprType\":\"FIELD_REFERENCE\"," + "\"dataType\":%d," + "\"colVal\":%d}", dataType, colVal);
    }

    public static Object getLiteralValue(Object obj, TypeInfo typeInfo) {
        if (obj instanceof HiveChar) {
            return ((HiveChar) obj).getStrippedValue();
        } else if (obj instanceof HiveVarchar) {
            return ((HiveVarchar) obj).getValue();
        } else if (obj instanceof Timestamp) {
            long l = ((Timestamp) obj).toEpochMilli();
            return Date.ofEpochMilli(l).toEpochDay();
        } else if (obj instanceof HiveDecimal) {
            if (((DecimalTypeInfo) typeInfo).getPrecision() <= 18) {
                return ((HiveDecimal) obj).unscaledValue().longValue();
            }
            return ((HiveDecimal) obj).unscaledValue().toString();
        } else if (obj instanceof Date) {
            return ((Date) obj).toEpochDay();
        } else if (obj instanceof HiveIntervalDayTime) {
            // to millisecond
            return ((HiveIntervalDayTime) obj).getDays();
        }
        return obj;
    }

    public static Integer getCharWidth(ExprNodeDesc node) {
        TypeInfo typeInfo = node.getTypeInfo();
        if (typeInfo instanceof PrimitiveTypeInfo) {
            String typeName = typeInfo.getTypeName();
            if (typeName.equals("string") && node instanceof ExprNodeConstantDesc) {
                return ((String) ((ExprNodeConstantDesc) node).getValue()).length();
            }
        }
        if (typeInfo instanceof CharTypeInfo) {
            return ((CharTypeInfo) typeInfo).getLength();
        } else if (typeInfo instanceof VarcharTypeInfo) {
            return ((VarcharTypeInfo) typeInfo).getLength();
        }
        return DEFAULT_VARCHAR_LENGTH;
    }

    public static int calculateVarcharLength(ExprNodeGenericFuncDesc node) {
        int result = 0;
        List<ExprNodeDesc> children = node.getChildren();
        for (ExprNodeDesc child : children) {
            if (child instanceof ExprNodeConstantDesc) {
                Object value = ((ExprNodeConstantDesc) child).getValue();
                result += Optional.ofNullable(value).orElse("").toString().length();
            } else if (child instanceof ExprNodeColumnDesc) {
                TypeInfo typeInfo = child.getTypeInfo();
                if (typeInfo instanceof BaseCharTypeInfo) {
                    result += ((BaseCharTypeInfo) typeInfo).getLength();
                } else {
                    result = DEFAULT_VARCHAR_LENGTH;
                    break;
                }
            } else {
                result = DEFAULT_VARCHAR_LENGTH;
                break;
            }
        }
        return result;
    }

    private static boolean isDecimalOrStringType(Integer dataType) {
        if (dataType == OMNI_CHAR.toValue() || dataType == OMNI_VARCHAR.toValue()
                || dataType == OMNI_DECIMAL64.toValue() || dataType == OMNI_DECIMAL128.toValue()
                || dataType == OMNI_DATE32.toValue() || dataType == OMNI_DATE64.toValue()) {
            return true;
        }
        return false;
    }

    public static boolean checkUnsupportedCast(CastFunctionExpression castFunctionExpression) {
        Integer dataType = castFunctionExpression.getDataType();
        Integer returnType = castFunctionExpression.getReturnType();
        // not support Cast(string as !(decimal/string)) and Cast(!(decimal/string) as
        // string)
        if ((dataType == OMNI_CHAR.toValue() || dataType == OMNI_VARCHAR.toValue())
                && !isDecimalOrStringType(returnType))
            return true;
        if ((returnType == OMNI_CHAR.toValue() || returnType == OMNI_VARCHAR.toValue())
                && !isDecimalOrStringType(dataType)) {
            return true;
        }
        return checkUnsupportedDecimal(dataType, returnType, castFunctionExpression);
    }

    private static boolean checkUnsupportedDecimal(Integer dataType, Integer returnType, CastFunctionExpression castFunctionExpression) {
        // not support Cast(double/decimal as decimal)
        if ((dataType == OMNI_DOUBLE.toValue() || dataType == OMNI_DECIMAL64.toValue())
                && (returnType == OMNI_DECIMAL64.toValue() || returnType == OMNI_DECIMAL128.toValue())) {
            return true;
        }
        if (dataType == OMNI_DECIMAL128.toValue() && returnType == OMNI_DECIMAL128.toValue()) {
            BaseExpression child = castFunctionExpression.getArguments().get(0);
            if (child instanceof DecimalReference && !((DecimalReference) child).getPrecision().equals(castFunctionExpression.getPrecision())) {
                return true;
            }
        }
        // not support Cast(decimal as short/int/long)
        return (dataType == OMNI_DECIMAL64.toValue() || dataType == OMNI_DECIMAL128.toValue())
                && isUnsupportedReturnType(returnType);
    }

    private static boolean isUnsupportedReturnType(Integer returnType) {
        return returnType == OMNI_SHORT.toValue() || returnType == OMNI_INT.toValue()
                || returnType == OMNI_LONG.toValue();
    }

    public static boolean checkUnsupportedArithmetic(ExprNodeGenericFuncDesc node) {
        List<ExprNodeDesc> children = node.getChildren();
        String functionName = node.getGenericUDF().getClass().getSimpleName();
        boolean anyDecimal = children.stream().anyMatch(child -> child.getTypeInfo() instanceof DecimalTypeInfo
                && ((DecimalTypeInfo) child.getTypeInfo()).getPrecision() > 19);
        if ((functionName.equals("GenericUDFOPMultiply") || functionName.equals("GenericUDFOPDivide")
                || functionName.equals("GenericUDFOPMod")) && anyDecimal) {
            return true;
        }
        return false;
    }

    public static boolean checkOmniJsonWhiteList(String filterExpr, String[] projections) {
        // inputTypes will not be checked if parseFormat is json( == 1),
        // only if its parseFormat is String(==0)
        long returnCode = new OmniExprVerify().exprVerifyNative(DataTypeSerializer.serialize(new DataType[0]), 0,
                filterExpr, projections, projections.length, 1);
        return returnCode != 0;
    }
}
