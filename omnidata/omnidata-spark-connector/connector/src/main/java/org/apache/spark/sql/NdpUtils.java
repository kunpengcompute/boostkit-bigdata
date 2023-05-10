/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
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

package org.apache.spark.sql;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.*;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.parseFloat;

import com.huawei.boostkit.omnidata.decode.type.BooleanDecodeType;
import com.huawei.boostkit.omnidata.decode.type.ByteDecodeType;
import com.huawei.boostkit.omnidata.decode.type.DateDecodeType;
import com.huawei.boostkit.omnidata.decode.type.DecimalDecodeType;
import com.huawei.boostkit.omnidata.decode.type.DecodeType;
import com.huawei.boostkit.omnidata.decode.type.DoubleDecodeType;
import com.huawei.boostkit.omnidata.decode.type.FloatDecodeType;
import com.huawei.boostkit.omnidata.decode.type.IntDecodeType;
import com.huawei.boostkit.omnidata.decode.type.LongDecodeType;
import com.huawei.boostkit.omnidata.decode.type.LongToByteDecodeType;
import com.huawei.boostkit.omnidata.decode.type.LongToFloatDecodeType;
import com.huawei.boostkit.omnidata.decode.type.LongToIntDecodeType;
import com.huawei.boostkit.omnidata.decode.type.LongToShortDecodeType;
import com.huawei.boostkit.omnidata.decode.type.ShortDecodeType;
import com.huawei.boostkit.omnidata.decode.type.TimestampDecodeType;
import com.huawei.boostkit.omnidata.decode.type.VarcharDecodeType;
import com.huawei.boostkit.omnidata.model.Column;

import io.airlift.slice.Slice;
import io.prestosql.spi.relation.ConstantExpression;

import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction;
import org.apache.spark.sql.catalyst.util.CharVarcharUtils;
import org.apache.spark.sql.execution.ndp.AggExeInfo;
import org.apache.spark.sql.execution.ndp.LimitExeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * NdpUtils
 *
 * @since 2021-03-30
 */
public class NdpUtils {

    /**
     * Types supported by OmniOperator.
     */
    public static final Set<String> supportTypes = new HashSet<String>() {
        {
            add(StandardTypes.INTEGER);
            add(StandardTypes.DATE);
            add(StandardTypes.SMALLINT);
            add(StandardTypes.BIGINT);
            add(StandardTypes.VARCHAR);
            add(StandardTypes.CHAR);
            add(StandardTypes.DECIMAL);
            add(StandardTypes.ROW);
            add(StandardTypes.DOUBLE);
            add(StandardTypes.VARBINARY);
            add(StandardTypes.BOOLEAN);
        }
    };
    private static final Logger LOG = LoggerFactory.getLogger(NdpUtils.class);

    public static int getColumnOffset(StructType dataSchema, Seq<Attribute> outPut) {
        List<Attribute> attributeList = JavaConverters.seqAsJavaList(outPut);
        String columnName = "";
        int columnTempId = 0;
        if (attributeList.size() > 0) {
            columnName = attributeList.get(0).name();
            columnTempId = NdpUtils.getColumnId(attributeList.get(0).toString());
        }
        Map<String, Integer> columnMap = new HashMap<>();
        scala.collection.Iterator<StructField> allTableSchemas = dataSchema.iterator();
        int dataSchemaColumnNum = 0;
        while (allTableSchemas.hasNext()) {
            StructField structField = allTableSchemas.next();
            columnMap.put(structField.name(), dataSchemaColumnNum++);
        }
        int columnOffset = columnTempId - columnMap.getOrDefault(columnName, columnMap.size());
        return Math.abs(columnOffset);
    }

    public static int getColumnOffsetByAggExeInfo(StructType dataSchema,
                                                  Seq<AggExeInfo> aggExeInfo) {
        String columnName = "";
        int columnTempId = 0;
        boolean isFind = false;
        if (aggExeInfo != null && aggExeInfo.size() > 0) {
            List<AggExeInfo> aggExecutionList = JavaConverters.seqAsJavaList(aggExeInfo);
            for (AggExeInfo aggExeInfoTemp : aggExecutionList) {
                List<AggregateFunction> aggregateExpressions = JavaConverters.seqAsJavaList(
                        aggExeInfoTemp.aggregateExpressions());
                for (AggregateFunction aggregateFunction : aggregateExpressions) {
                    List<Expression> expressions = JavaConverters
                            .seqAsJavaList(aggregateFunction.children());
                    for (Expression expression : expressions) {
                        columnName = expression.toString().split("#")[0].replaceAll("\\(", "");
                        Pattern pattern = Pattern.compile(columnName + "#(\\d+)");
                        Matcher matcher = pattern.matcher(expression.toString());
                        if (matcher.find()) {
                            columnTempId = Integer.parseInt(matcher.group(1));
                            isFind = true;
                            break;
                        }
                    }
                    if (isFind) {
                        break;
                    }
                }
                List<NamedExpression> namedExpressions = JavaConverters.seqAsJavaList(
                        aggExeInfoTemp.groupingExpressions());
                for (NamedExpression namedExpression : namedExpressions) {
                    columnName = namedExpression.name();
                    columnTempId = NdpUtils.getColumnId(namedExpression.toString());
                    break;
                }
            }
        }
        Map<String, Integer> columnMap = new HashMap<>();
        scala.collection.Iterator<StructField> allTableSchemas = dataSchema.iterator();
        int dataSchemaColumnNum = 0;
        while (allTableSchemas.hasNext()) {
            StructField structField = allTableSchemas.next();
            columnMap.put(structField.name(), dataSchemaColumnNum++);
        }
        int columnOffset = columnTempId - columnMap.getOrDefault(columnName, columnMap.size());
        return Math.abs(columnOffset);
    }

    public static int getColumnId(String attribute) {
        if (null == attribute) {
            return -1;
        }
        int columnTempId = 0;
        String[] columnArray = attribute.split("#");
        if (columnArray.length < 2) {
            return -1;
        }
        String columnArrayId = columnArray[1];
        if ('L' == columnArrayId.charAt(columnArrayId.length() - 1)) {
            String adf = columnArrayId.substring(0, columnArrayId.length() - 1);
            columnTempId = Integer.parseInt(adf);
        } else {
            if (columnArrayId.contains(")")) {
                columnTempId = Integer.parseInt(columnArrayId.split("\\)")[0].replaceAll("[^(\\d+)]", ""));
            } else {
                columnTempId = Integer.parseInt(columnArrayId);
            }
        }
        return columnTempId;
    }

    /**
     * transform spark data type to omnidata
     *
     * @param dataType           spark data type
     * @param isSparkUdfOperator is spark udf
     * @return result type
     */
    public static Type transOlkDataType(DataType dataType, boolean isSparkUdfOperator) {
        return transOlkDataType(dataType, null, isSparkUdfOperator);
    }

    public static Type transOlkDataType(DataType dataType, Object attribute, boolean isSparkUdfOperator) {
        String strType;
        Metadata metadata = Metadata.empty();
        if (attribute instanceof Attribute) {
            metadata = ((Attribute) attribute).metadata();
            strType = ((Attribute) attribute).dataType().toString().toLowerCase(Locale.ENGLISH);
        } else {
            strType = dataType.toString().toLowerCase(Locale.ENGLISH);
        }
        if (isSparkUdfOperator && "integertype".equalsIgnoreCase(strType)) {
            strType = "longtype";
        }
        if (strType.contains("decimal")) {
            String[] decimalInfo = strType.split("\\(")[1].split("\\)")[0].split(",");
            int precision = Integer.parseInt(decimalInfo[0]);
            int scale = Integer.parseInt(decimalInfo[1]);
            return DecimalType.createDecimalType(precision, scale);
        }
        switch (strType) {
            case "timestamptype":
                return TIMESTAMP;
            case "longtype":
                return BIGINT;
            case "integertype":
                return INTEGER;
            case "bytetype":
                return TINYINT;
            case "shorttype":
                return SMALLINT;
            case "floattype":
                return REAL;
            case "doubletype":
                return DOUBLE;
            case "booleantype":
                return BOOLEAN;
            case "stringtype":
                if (CharVarcharUtils.getRawTypeString(metadata).isDefined()) {
                    String metadataStr = CharVarcharUtils.getRawTypeString(metadata).get();
                    Pattern pattern = Pattern.compile("(?<=\\()\\d+(?=\\))");
                    Matcher matcher = pattern.matcher(metadataStr);
                    String len = String.valueOf(UNBOUNDED_LENGTH);
                    while (matcher.find()) {
                        len = matcher.group();
                    }
                    if (metadataStr.startsWith("char")) {
                        return CharType.createCharType(Integer.parseInt(len));
                    } else if (metadataStr.startsWith("varchar")) {
                        return createVarcharType(Integer.parseInt(len));
                    }
                } else {
                    return VARCHAR;
                }
            case "datetype":
                return DATE;
            default:
                throw new UnsupportedOperationException("unsupported this type:" + strType);
        }
    }

    public static Type transAggRetType(Type prestoType) {
        if (BIGINT.equals(prestoType) || INTEGER.equals(prestoType) ||
                SMALLINT.equals(prestoType) || TINYINT.equals(prestoType) || REAL.equals(prestoType)) {
            return BIGINT;
        } else {
            return prestoType;
        }
    }

    public static DecodeType transAggDecodeType(Type prestoType) {
        if (BIGINT.equals(prestoType)) {
            return new LongDecodeType();
        }
        if (INTEGER.equals(prestoType)) {
            return new LongToIntDecodeType();
        }
        if (SMALLINT.equals(prestoType)) {
            return new LongToShortDecodeType();
        }
        if (TINYINT.equals(prestoType)) {
            return new LongToByteDecodeType();
        }
        if (DOUBLE.equals(prestoType)) {
            return new DoubleDecodeType();
        }
        if (REAL.equals(prestoType)) {
            return new LongToFloatDecodeType();
        }
        if (BOOLEAN.equals(prestoType)) {
            return new BooleanDecodeType();
        }
        if (VARCHAR.equals(prestoType) || prestoType instanceof CharType) {
            return new VarcharDecodeType();
        }
        if (DATE.equals(prestoType)) {
            return new DateDecodeType();
        }
        throw new UnsupportedOperationException("unsupported this prestoType:" + prestoType);
    }

    public static DecodeType transDecodeType(DataType dataType) {
        String strType = dataType.toString().toLowerCase(Locale.ENGLISH);
        switch (strType) {
            case "timestamptype":
                return new TimestampDecodeType();
            case "integertype":
                return new IntDecodeType();
            case "shorttype":
                return new ShortDecodeType();
            case "longtype":
                return new LongDecodeType();
            case "floattype":
                return new FloatDecodeType();
            case "doubletype":
                return new DoubleDecodeType();
            case "booleantype":
                return new BooleanDecodeType();
            case "bytetype":
                return new ByteDecodeType();
            case "stringtype":
                return new VarcharDecodeType();
            case "datetype":
                return new DateDecodeType();
            default:
                if (strType.contains("decimal")) {
                    String[] decimalInfo = strType.split("\\(")[1].split("\\)")[0].split(",");
                    int precision = Integer.parseInt(decimalInfo[0]);
                    int scale = Integer.parseInt(decimalInfo[1]);
                    return new DecimalDecodeType(precision, scale);
                } else {
                    throw new UnsupportedOperationException("unsupported this type:" + strType);
                }
        }
    }

    /**
     * Convert decimal data to a constant expression
     *
     * @param argumentValue value
     * @param decimalType   decimalType
     * @return ConstantExpression
     */
    public static ConstantExpression transDecimalConstant(String argumentValue,
                                                          DecimalType decimalType) {
        BigInteger bigInteger =
                Decimals.rescale(new BigDecimal(argumentValue), decimalType).unscaledValue();
        if (decimalType.isShort()) {
            return new ConstantExpression(bigInteger.longValue(), decimalType);
        } else {
            Slice argumentValueSlice = Decimals.encodeUnscaledValue(bigInteger);
            long[] base = new long[]{argumentValueSlice.getLong(0), argumentValueSlice.getLong(8)};
            try {
                Field filed = Slice.class.getDeclaredField("base");
                filed.setAccessible(true);
                filed.set(argumentValueSlice, base);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new UnsupportedOperationException("create long decimal data failed");
            }
            return new ConstantExpression(argumentValueSlice, decimalType);
        }
    }

    /**
     * Convert data to a constant expression
     * process 'null' data
     *
     * @param argumentValue value
     * @param argumentType  argumentType
     * @return ConstantExpression
     */
    public static ConstantExpression transConstantExpression(String argumentValue, Type argumentType) {
        if (argumentType instanceof CharType) {
            Slice charValue = utf8Slice(stripEnd(argumentValue, " "));
            return new ConstantExpression(charValue, argumentType);
        }
        if (argumentType instanceof VarcharType) {
            Slice charValue = utf8Slice(argumentValue);
            return new ConstantExpression(charValue, argumentType);
        }
        if (argumentValue.equals("null")) {
            return new ConstantExpression(null, argumentType);
        }
        if (argumentType instanceof DecimalType) {
            return transDecimalConstant(argumentValue, (DecimalType) argumentType);
        }
        String strType = argumentType.toString().toLowerCase(Locale.ENGLISH);
        switch (strType) {
            case "bigint":
            case "integer":
            case "date":
            case "tinyint":
            case "smallint":
                return new ConstantExpression(Long.parseLong(argumentValue), argumentType);
            case "real":
                return new ConstantExpression(
                        (long) floatToIntBits(parseFloat(argumentValue)), argumentType);
            case "double":
                return new ConstantExpression(Double.valueOf(argumentValue), argumentType);
            case "boolean":
                return new ConstantExpression(Boolean.valueOf(argumentValue), argumentType);
            case "timestamp":
                int rawOffset = TimeZone.getDefault().getRawOffset();
                long timestampValue;
                if (argumentValue.contains("-")) {
                    try {
                        timestampValue = java.sql.Timestamp.valueOf(argumentValue).getTime() + rawOffset;
                    } catch (Exception e) {
                        timestampValue = -1;
                    }
                } else {
                    int millisecondsDiffMicroseconds = 3;
                    timestampValue = Long.parseLong(argumentValue.substring(0,
                            argumentValue.length() - millisecondsDiffMicroseconds)) + rawOffset;
                }
                return new ConstantExpression(timestampValue, argumentType);
            default:
                throw new UnsupportedOperationException("unsupported data type " + strType);
        }
    }

    public static Attribute getColumnAttribute(Attribute inputAttribute, List<Attribute> listAtt) {
        String columnName = inputAttribute.name();
        Attribute resAttribute = inputAttribute;
        if (columnName.contains("(")) {
            for (Attribute att : listAtt) {
                if (columnName.contains(att.name())) {
                    resAttribute = att;
                    break;
                }
            }
        }
        return resAttribute;
    }

    public static OptionalLong convertLimitExeInfo(Option<LimitExeInfo> limitExeInfo) {
        return limitExeInfo.isEmpty() ? OptionalLong.empty()
                : OptionalLong.of(limitExeInfo.get().limit());
    }

    public static String getPartitionValue(String filePath, String columnName) {
        String[] filePathStrArray = filePath.split("\\/");
        String partitionValue = "";
        Pattern pn = Pattern.compile(columnName + "\\=");
        for (String strColumn : filePathStrArray) {
            Matcher matcher = pn.matcher(strColumn);
            if (matcher.find()) {
                partitionValue = strColumn.split("\\=")[1];
                if (partitionValue.contains("__HIVE_DEFAULT_PARTITION__")) {
                    partitionValue = null;
                }
                break;
            }
        }
        return partitionValue;
    }

    public static int getFpuHosts(int hostSize) {
        return (int) (Math.random() * hostSize);
    }

    /**
     * Check if the input pages contains datatypes unsuppoted by OmniColumnVector.
     *
     * @param columns Input columns
     * @return false if contains unsupported type
     */
    public static boolean checkOmniOpColumns(List<Column> columns) {
        for (Column column : columns) {
            String base = column.getType().getTypeSignature().getBase();
            if (!supportTypes.contains(base)) {
                LOG.info("Unsupported operator data type {}, rollback", base);
                return false;
            }
        }
        return true;
    }

    public static String stripEnd(String str, String stripChars) {
        int end;
        if (str != null && (end = str.length()) != 0) {
            if (stripChars == null) {
                while (end != 0 && Character.isWhitespace(str.charAt(end - 1))) {
                    --end;
                }
            } else {
                if (stripChars.isEmpty()) {
                    return str;
                }
                while (end != 0 && stripChars.indexOf(str.charAt(end - 1)) != -1) {
                    --end;
                }
            }
            return str.substring(0, end);
        } else {
            return str;
        }
    }
}