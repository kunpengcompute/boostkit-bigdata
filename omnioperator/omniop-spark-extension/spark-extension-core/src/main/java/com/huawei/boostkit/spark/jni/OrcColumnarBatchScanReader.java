/*
 * Copyright (C) 2020-2023. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.spark.jni;
import com.huawei.boostkit.scan.jni.OrcColumnarBatchJniReader;

import com.huawei.boostkit.spark.timestamp.JulianGregorianRebase;
import com.huawei.boostkit.spark.timestamp.TimestampUtil;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.*;

import org.apache.orc.impl.writer.TimestampTreeWriter;
import org.apache.spark.sql.catalyst.util.CharVarcharUtils;
import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.net.URI;
import java.time.LocalDate;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.TimeZone;

public class OrcColumnarBatchScanReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrcColumnarBatchScanReader.class);

    private boolean nativeSupportTimestampRebase;
    private static final Pattern CHAR_TYPE = Pattern.compile("char\\(\\s*(\\d+)\\s*\\)");

    private static final int MAX_LEAF_THRESHOLD = 256;

    public long reader;
    public long recordReader;
    public long batchReader;

    // All ORC fieldNames
    public ArrayList<String> allFieldsNames;

    // Indicate columns to read
    public int[] colsToGet;

    // Actual columns to read
    public ArrayList<String> includedColumns;

    // max threshold for leaf node
    private int leafIndex;

    // spark required schema
    private StructType requiredSchema;

    public OrcColumnarBatchJniReader jniReader;
    public OrcColumnarBatchScanReader() {
        jniReader = new OrcColumnarBatchJniReader();
        allFieldsNames = new ArrayList<String>();
    }

    public String padZeroForDecimals(String [] decimalStrArray, int decimalScale) {
        String decimalVal = "";
        if (decimalStrArray.length == 2) {
            decimalVal = decimalStrArray[1];
        }
        // If the length of the formatted number string is insufficient, pad '0's.
        return String.format("%1$-" + decimalScale + "s", decimalVal).replace(' ', '0');
    }

    private long formatSecs(long secs) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long epoch;
        try {
            epoch = dateFormat.parse(TimestampTreeWriter.BASE_TIMESTAMP_STRING).getTime() /
                    TimestampTreeWriter.MILLIS_PER_SECOND;
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return secs - epoch;
    }

    private long formatNanos(int nanos) {
        if (nanos == 0) {
            return 0;
        } else if (nanos % 100 != 0) {
            return ((long) nanos) << 3;
        } else {
            nanos /= 100;
            int trailingZeros = 1;
            while (nanos % 10 == 0 && trailingZeros < 7) {
                nanos /= 10;
                trailingZeros += 1;
            }
            return ((long) nanos) << 3 | trailingZeros;
        }
    }

    private void addJulianGregorianInfo(JSONObject job) {
        TimestampUtil instance = TimestampUtil.getInstance();
        JulianGregorianRebase julianObject = instance.getJulianObject(TimeZone.getDefault().getID());
        if (julianObject == null) {
            return;
        }
        job.put("tz", julianObject.getTz());
        job.put("switches", julianObject.getSwitches());
        job.put("diffs", julianObject.getDiffs());
        nativeSupportTimestampRebase = true;
    }

    /**
     * Init Orc reader.
     *
     * @param uri split file path
     */
    public long initializeReaderJava(URI uri) {
        JSONObject job = new JSONObject();

        job.put("serializedTail", "");
        job.put("tailLocation", 9223372036854775807L);

        job.put("scheme", uri.getScheme() == null ? "" : uri.getScheme());
        job.put("host", uri.getHost() == null ? "" : uri.getHost());
        job.put("port", uri.getPort());
        job.put("path", uri.getPath() == null ? "" : uri.getPath());

        reader = jniReader.initializeReader(job, allFieldsNames);
        return reader;
    }

    /**
     * Init Orc RecordReader.
     *
     * @param offset split file offset
     * @param length split file read length
     * @param pushedFilter the filter push down to native
     * @param requiredSchema the columns read from native
     */
    public long initializeRecordReaderJava(long offset, long length, Filter pushedFilter, StructType requiredSchema) {
        this.requiredSchema = requiredSchema;
        JSONObject job = new JSONObject();

        job.put("offset", offset);
        job.put("length", length);

        if (pushedFilter != null) {
            JSONObject jsonExpressionTree = new JSONObject();
            JSONObject jsonLeaves = new JSONObject();
            boolean flag = canPushDown(pushedFilter, jsonExpressionTree, jsonLeaves);
            if (flag) {
                job.put("expressionTree", jsonExpressionTree);
                job.put("leaves", jsonLeaves);
            }
        }

        job.put("includedColumns", includedColumns.toArray());
        addJulianGregorianInfo(job);
        recordReader = jniReader.initializeRecordReader(reader, job);
        return recordReader;
    }

    public long initBatchJava(long batchSize) {
        batchReader = jniReader.initializeBatch(recordReader, batchSize);
        return 0;
    }

    public long getNumberOfRowsJava() {
        return jniReader.getNumberOfRows(recordReader, batchReader);
    }

    public long getRowNumber() {
        return jniReader.recordReaderGetRowNumber(recordReader);
    }

    public float getProgress() {
        return jniReader.recordReaderGetProgress(recordReader);
    }

    public void close() {
        jniReader.recordReaderClose(recordReader, reader, batchReader);
    }

    public void seekToRow(long rowNumber) {
        jniReader.recordReaderSeekToRow(recordReader, rowNumber);
    }

    public void convertJulianToGreGorian(IntVec intVec, long rowNumber) {
        int gregorianValue;
        for (int rowIndex = 0; rowIndex < rowNumber; rowIndex++) {
            gregorianValue = RebaseDateTime.rebaseJulianToGregorianDays(intVec.get(rowIndex));
            intVec.set(rowIndex, gregorianValue);
        }
    }

    public void convertJulianToGregorianMicros(LongVec longVec, long rowNumber) {
        long gregorianValue;
        for (int rowIndex = 0; rowIndex < rowNumber; rowIndex++) {
            gregorianValue = RebaseDateTime.rebaseJulianToGregorianMicros(longVec.get(rowIndex));
            longVec.set(rowIndex, gregorianValue);
        }
    }

    public int next(Vec[] vecList, int[] typeIds) {
        long[] vecNativeIds = new long[typeIds.length];
        long rtn = jniReader.recordReaderNext(recordReader, batchReader, typeIds, vecNativeIds);
        if (rtn == 0) {
            return 0;
        }
        int nativeGetId = 0;
        for (int i = 0; i < colsToGet.length; i++) {
            if (colsToGet[i] != 0) {
                continue;
            }
            switch (DataType.DataTypeId.values()[typeIds[nativeGetId]]) {
                case OMNI_BOOLEAN: {
                    vecList[i] = new BooleanVec(vecNativeIds[nativeGetId]);
                    break;
                }
                case OMNI_SHORT: {
                    vecList[i] = new ShortVec(vecNativeIds[nativeGetId]);
                    break;
                }
                case OMNI_DATE32: {
                    vecList[i] = new IntVec(vecNativeIds[nativeGetId]);
                    convertJulianToGreGorian((IntVec)(vecList[i]), rtn);
                    break;
                }
                case OMNI_INT: {
                    vecList[i] = new IntVec(vecNativeIds[nativeGetId]);
                    break;
                }
                case OMNI_LONG:
                case OMNI_DECIMAL64: {
                    vecList[i] = new LongVec(vecNativeIds[nativeGetId]);
                    break;
                }
                case OMNI_TIMESTAMP: {
                    vecList[i] = new LongVec(vecNativeIds[nativeGetId]);
                    if (!this.nativeSupportTimestampRebase) {
                        convertJulianToGregorianMicros((LongVec)(vecList[i]), rtn);
                    }
                    break;
                }
                case OMNI_DOUBLE: {
                    vecList[i] = new DoubleVec(vecNativeIds[nativeGetId]);
                    break;
                }
                case OMNI_VARCHAR: {
                    vecList[i] = new VarcharVec(vecNativeIds[nativeGetId]);
                    break;
                }
                case OMNI_DECIMAL128: {
                    vecList[i] = new Decimal128Vec(vecNativeIds[nativeGetId]);
                    break;
                }
                default: {
                    throw new RuntimeException("UnSupport type for ColumnarFileScan:" +
                        DataType.DataTypeId.values()[typeIds[i]]);
                }
            }
            nativeGetId++;
        }
        return (int)rtn;
    }

    enum OrcOperator {
        OR,
        AND,
        NOT,
        LEAF,
        CONSTANT
    }

    enum OrcLeafOperator {
        EQUALS,
        NULL_SAFE_EQUALS,
        LESS_THAN,
        LESS_THAN_EQUALS,
        IN,
        BETWEEN, // not use, spark transfers it to gt and lt
        IS_NULL
    }

    enum OrcPredicateDataType {
        LONG, // all of integer types
        FLOAT, // float and double
        STRING, // string, char, varchar
        DATE,
        DECIMAL,
        TIMESTAMP,
        BOOLEAN
    }

    private OrcPredicateDataType getOrcPredicateDataType(String attribute) {
        StructField field = requiredSchema.apply(attribute);
        org.apache.spark.sql.types.DataType dataType = field.dataType();
        if (dataType instanceof ShortType || dataType instanceof IntegerType ||
            dataType instanceof LongType) {
            return OrcPredicateDataType.LONG;
        } else if (dataType instanceof DoubleType) {
            return OrcPredicateDataType.FLOAT;
        } else if (dataType instanceof StringType) {
            if (isCharType(field.metadata())) {
                throw new UnsupportedOperationException("Unsupported orc push down filter data type: char");
            }
            return OrcPredicateDataType.STRING;
        } else if (dataType instanceof DateType) {
            return OrcPredicateDataType.DATE;
        } else if (dataType instanceof DecimalType) {
            return OrcPredicateDataType.DECIMAL;
        } else if (dataType instanceof BooleanType) {
            return OrcPredicateDataType.BOOLEAN;
        } else {
            throw new UnsupportedOperationException("Unsupported orc push down filter data type: " +
                dataType.getClass().getSimpleName());
        }
    }

    // Check the type whether is char type, which orc native does not support push down
    private boolean isCharType(Metadata metadata) {
        if (metadata != null) {
            String rawTypeString = CharVarcharUtils.getRawTypeString(metadata).getOrElse(null);
            if (rawTypeString != null) {
                Matcher matcher = CHAR_TYPE.matcher(rawTypeString);
                return matcher.matches();
            }
        }
        return false;
    }

    private boolean canPushDown(Filter pushedFilter, JSONObject jsonExpressionTree,
            JSONObject jsonLeaves) {
        try {
            getExprJson(pushedFilter, jsonExpressionTree, jsonLeaves);
            if (leafIndex > MAX_LEAF_THRESHOLD) {
                throw new UnsupportedOperationException("leaf node nums is " + leafIndex +
                    ", which is bigger than max threshold " + MAX_LEAF_THRESHOLD + ".");
            }
            return true;
        } catch (Exception e) {
            LOGGER.info("Unable to push down orc filter because " + e.getMessage());
            return false;
        }
    }

    private void getExprJson(Filter filterPredicate, JSONObject jsonExpressionTree,
             JSONObject jsonLeaves) {
        if (filterPredicate instanceof And) {
            addChildJson(jsonExpressionTree, jsonLeaves, OrcOperator.AND,
                ((And) filterPredicate).left(), ((And) filterPredicate).right());
        } else if (filterPredicate instanceof Or) {
            addChildJson(jsonExpressionTree, jsonLeaves, OrcOperator.OR,
                ((Or) filterPredicate).left(), ((Or) filterPredicate).right());
        } else if (filterPredicate instanceof Not) {
            addChildJson(jsonExpressionTree, jsonLeaves, OrcOperator.NOT,
                ((Not) filterPredicate).child());
        } else if (filterPredicate instanceof EqualTo) {
            addToJsonExpressionTree("leaf-" + leafIndex, jsonExpressionTree, false);
            addLiteralToJsonLeaves("leaf-" + leafIndex, OrcLeafOperator.EQUALS, jsonLeaves,
                ((EqualTo) filterPredicate).attribute(), ((EqualTo) filterPredicate).value(), null);
            leafIndex++;
        } else if (filterPredicate instanceof GreaterThan) {
            addToJsonExpressionTree("leaf-" + leafIndex, jsonExpressionTree, true);
            addLiteralToJsonLeaves("leaf-" + leafIndex, OrcLeafOperator.LESS_THAN_EQUALS, jsonLeaves,
                ((GreaterThan) filterPredicate).attribute(), ((GreaterThan) filterPredicate).value(), null);
            leafIndex++;
        } else if (filterPredicate instanceof GreaterThanOrEqual) {
            addToJsonExpressionTree("leaf-" + leafIndex, jsonExpressionTree, true);
            addLiteralToJsonLeaves("leaf-" + leafIndex, OrcLeafOperator.LESS_THAN, jsonLeaves,
                ((GreaterThanOrEqual) filterPredicate).attribute(), ((GreaterThanOrEqual) filterPredicate).value(), null);
            leafIndex++;
        } else if (filterPredicate instanceof LessThan) {
            addToJsonExpressionTree("leaf-" + leafIndex, jsonExpressionTree, false);
            addLiteralToJsonLeaves("leaf-" + leafIndex, OrcLeafOperator.LESS_THAN, jsonLeaves,
                ((LessThan) filterPredicate).attribute(), ((LessThan) filterPredicate).value(), null);
            leafIndex++;
        } else if (filterPredicate instanceof LessThanOrEqual) {
            addToJsonExpressionTree("leaf-" + leafIndex, jsonExpressionTree, false);
            addLiteralToJsonLeaves("leaf-" + leafIndex, OrcLeafOperator.LESS_THAN_EQUALS, jsonLeaves,
                ((LessThanOrEqual) filterPredicate).attribute(), ((LessThanOrEqual) filterPredicate).value(), null);
            leafIndex++;
            // For IsNotNull/IsNull/In, pass literal = "" to native to avoid throwing exception.
        } else if (filterPredicate instanceof IsNotNull) {
            addToJsonExpressionTree("leaf-" + leafIndex, jsonExpressionTree, true);
            addLiteralToJsonLeaves("leaf-" + leafIndex, OrcLeafOperator.IS_NULL, jsonLeaves,
                ((IsNotNull) filterPredicate).attribute(), "", null);
            leafIndex++;
        } else if (filterPredicate instanceof IsNull) {
            addToJsonExpressionTree("leaf-" + leafIndex, jsonExpressionTree, false);
            addLiteralToJsonLeaves("leaf-" + leafIndex, OrcLeafOperator.IS_NULL, jsonLeaves,
                ((IsNull) filterPredicate).attribute(), "", null);
            leafIndex++;
        } else if (filterPredicate instanceof In) {
            addToJsonExpressionTree("leaf-" + leafIndex, jsonExpressionTree, false);
            addLiteralToJsonLeaves("leaf-" + leafIndex, OrcLeafOperator.IN, jsonLeaves,
                ((In) filterPredicate).attribute(), "", Arrays.stream(((In) filterPredicate).values()).toArray());
            leafIndex++;
        } else {
            throw new UnsupportedOperationException("Unsupported orc push down filter operation: " +
                filterPredicate.getClass().getSimpleName());
        }
    }

    private void addLiteralToJsonLeaves(String leaf, OrcLeafOperator leafOperator, JSONObject jsonLeaves,
                                        String name, Object literal, Object[] literals) {
        JSONObject leafJson = new JSONObject();
        leafJson.put("op", leafOperator.ordinal());
        leafJson.put("name", name);
        leafJson.put("type", getOrcPredicateDataType(name).ordinal());

        leafJson.put("literal", getLiteralValue(literal));

        ArrayList<String> literalList = new ArrayList<>();
        if (literals != null) {
            for (Object lit: literals) {
                literalList.add(getLiteralValue(lit));
            }
        }
        leafJson.put("literalList", literalList);
        jsonLeaves.put(leaf, leafJson);
    }

    private void addToJsonExpressionTree(String leaf, JSONObject jsonExpressionTree, boolean addNot) {
        if (addNot) {
            jsonExpressionTree.put("op", OrcOperator.NOT.ordinal());
            ArrayList<JSONObject> child = new ArrayList<>();
            JSONObject subJson = new JSONObject();
            subJson.put("op", OrcOperator.LEAF.ordinal());
            subJson.put("leaf", leaf);
            child.add(subJson);
            jsonExpressionTree.put("child", child);
        } else {
            jsonExpressionTree.put("op", OrcOperator.LEAF.ordinal());
            jsonExpressionTree.put("leaf", leaf);
        }
    }

    private void addChildJson(JSONObject jsonExpressionTree, JSONObject jsonLeaves,
                              OrcOperator orcOperator, Filter ... filters) {
        jsonExpressionTree.put("op", orcOperator.ordinal());
        ArrayList<Object> child = new ArrayList<>();
        for (Filter filter: filters) {
            JSONObject subJson = new JSONObject();
            getExprJson(filter, subJson, jsonLeaves);
            child.add(subJson);
        }
        jsonExpressionTree.put("child", child);
    }

    private String getLiteralValue(Object literal) {
        // For null literal, the predicate will not be pushed down.
        if (literal == null) {
            throw new UnsupportedOperationException("Unsupported orc push down filter for literal is null");
        }

        // For Decimal Type, we use the special string format to represent, which is "$decimalVal
        // $precision $scale".
        // e.g., Decimal(9, 3) = 123456.789, it outputs "123456.789 9 3".
        // e.g., Decimal(9, 3) = 123456.7, it outputs "123456.700 9 3".
        if (literal instanceof BigDecimal) {
            BigDecimal value = (BigDecimal) literal;
            int precision = value.precision();
            int scale = value.scale();
            String[] split = value.toString().split("\\.");
            if (scale == 0) {
                return split[0] + " " + precision + " " + scale;
            } else {
                String padded = padZeroForDecimals(split, scale);
                return split[0] + "." + padded + " " + precision + " " + scale;
            }
        }
        // For Date Type, spark uses Gregorian in default but orc uses Julian, which should be converted.
        if (literal instanceof LocalDate) {
            int epochDay = Math.toIntExact(((LocalDate) literal).toEpochDay());
            int rebased = RebaseDateTime.rebaseGregorianToJulianDays(epochDay);
            return String.valueOf(rebased);
        }
        if (literal instanceof String) {
            return (String) literal;
        }
        if (literal instanceof Integer || literal instanceof Long || literal instanceof Boolean ||
            literal instanceof Short || literal instanceof Double) {
            return literal.toString();
        }
        throw new UnsupportedOperationException("Unsupported orc push down filter date type: " +
            literal.getClass().getSimpleName());
    }
}
