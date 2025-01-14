/*
 * Copyright (C) 2021-2023. Huawei Technologies Co., Ltd. All rights reserved.
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

import com.huawei.boostkit.scan.jni.ParquetColumnarBatchJniReader;
import com.huawei.boostkit.spark.timestamp.JulianGregorianRebase;
import com.huawei.boostkit.spark.timestamp.TimestampUtil;
import nova.hetu.omniruntime.vector.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec;
import org.apache.spark.sql.execution.datasources.DataSourceUtils;
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
import org.apache.spark.sql.types.*;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ParquetColumnarBatchScanReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetColumnarBatchScanReader.class);

    public long parquetReader;

    public ParquetColumnarBatchJniReader jniReader;

    private StructType requiredSchema;

    private final RebaseSpec datetimeRebaseSpec;

    private final RebaseSpec int96RebaseSpec;

    private boolean nativeSupportTimestampRebase;

    private final Function1<Object, Object> timestampRebaseFunc;

    private boolean nativeSupportInt96Rebase;

    private final Function1<Object, Object> int96RebaseFunc;

    private final List<Type> parquetTypes;

    private ArrayList<String> allFieldsNames;

    public ParquetColumnarBatchScanReader(StructType requiredSchema, RebaseSpec datetimeRebaseSpec,
                                          RebaseSpec int96RebaseSpec, List<Type> parquetTypes) {
        this.requiredSchema = requiredSchema;
        this.datetimeRebaseSpec = datetimeRebaseSpec;
        this.int96RebaseSpec = int96RebaseSpec;
        this.timestampRebaseFunc = datetimeRebaseSpec.mode() == null ? micros -> micros
                : DataSourceUtils.createTimestampRebaseFuncInRead(datetimeRebaseSpec, "Parquet");
        this.int96RebaseFunc = int96RebaseSpec.mode() == null ? micros -> micros
                : DataSourceUtils.createTimestampRebaseFuncInRead(int96RebaseSpec, "Parquet INT96");
        this.parquetTypes = parquetTypes;
        jniReader = new ParquetColumnarBatchJniReader();
    }

    private void addJulianGregorianInfo(JSONObject job) {
        if (Arrays.stream(requiredSchema.fields()).noneMatch(field -> field.dataType() instanceof TimestampType)) {
            return;
        }
        TimestampUtil instance = TimestampUtil.getInstance();
        JulianGregorianRebase timestampRebaseJulianObject = instance.getJulianObject(datetimeRebaseSpec.timeZone());
        if (timestampRebaseJulianObject != null && datetimeRebaseSpec.mode() != null) {
            JSONObject timestampRebase = new JSONObject();
            timestampRebase.put("tz", timestampRebaseJulianObject.getTz());
            timestampRebase.put("switches", timestampRebaseJulianObject.getSwitches());
            timestampRebase.put("diffs", timestampRebaseJulianObject.getDiffs());
            timestampRebase.put("mode", datetimeRebaseSpec.mode().id());
            job.put("timestampRebase", timestampRebase);
            nativeSupportTimestampRebase = true;
        }
        JulianGregorianRebase int96RebaseJulianObject = instance.getJulianObject(int96RebaseSpec.timeZone());
        if (int96RebaseJulianObject != null && int96RebaseSpec.mode() != null) {
            JSONObject int96Rebase = new JSONObject();
            int96Rebase.put("tz", int96RebaseJulianObject.getTz());
            int96Rebase.put("switches", int96RebaseJulianObject.getSwitches());
            int96Rebase.put("diffs", int96RebaseJulianObject.getDiffs());
            int96Rebase.put("mode", int96RebaseSpec.mode().id());
            job.put("int96Rebase", int96Rebase);
            nativeSupportInt96Rebase = true;
        }
        if (nativeSupportTimestampRebase || nativeSupportInt96Rebase) {
            job.put("lastSwitchJulianTs", RebaseDateTime.lastSwitchJulianTs());
        }
    }

    public long initializeReaderJava(Path path, int capacity) throws UnsupportedEncodingException {
        JSONObject job = new JSONObject();

        URI uri = path.toUri();
        job.put("uri", path.toString());
        job.put("host", uri.getHost() == null ? "" : uri.getHost());
        job.put("scheme", uri.getScheme() == null ? "" : uri.getScheme());
        job.put("port", uri.getPort());
        job.put("path", uri.getPath() == null ? "" : uri.getPath());

        job.put("capacity", capacity);

        String ugi = null;
        try {
            ugi = UserGroupInformation.getCurrentUser().toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        job.put("ugi", ugi);

        addJulianGregorianInfo(job);

        parquetReader = jniReader.initializeReader(job);
        return parquetReader;
    }

    public long initializeRecordReaderJava(long start, long end, String[] fieldNames, Filter pushedFilter)
            throws UnsupportedEncodingException {
        JSONObject job = new JSONObject();
        job.put("start", start);
        job.put("end", end);
        job.put("fieldNames", fieldNames);

        if (pushedFilter != null) {
            pushDownFilter(pushedFilter, job);
        }

        parquetReader = jniReader.initializeRecordReader(parquetReader, job);
        return parquetReader;
    }

    public ArrayList<String> getAllFieldsNames() {
        if (allFieldsNames == null) {
            allFieldsNames = new ArrayList<>();
            jniReader.getAllFieldNames(parquetReader, allFieldsNames);
        }
        return allFieldsNames;
    }

    private void pushDownFilter(Filter pushedFilter, JSONObject job) {
        try {
            JSONObject jsonExpressionTree = getSubJson(pushedFilter);
            job.put("expressionTree", jsonExpressionTree);
        } catch (Exception e) {
            LOGGER.info("Parquet push down filter failed because " + e.getMessage());
        }
        return;
    }

    // more complex filters not supported yet
    enum ParquetPredicateOperator {
        And,
        Or,
        Not,
        Eq,
        Gt,
        GtEq,
        Lt,
        LtEq,
        IsNotNull,
        IsNull,
        In
    }

    // only pushdown spark extension supported types
    enum ParquetPredicateDataType {
        Null,
        Integer,
        Long,
        String,
        Date32,
        Decimal,
        Bool,
        Short,
        Double,
        Timestamp
    }

    private JSONObject getSubJson(Filter filterPredicate) {
        JSONObject jsonObject = new JSONObject();
        if (filterPredicate instanceof And) {
            And and  = (And) filterPredicate;
            putBinaryOp(jsonObject, ParquetPredicateOperator.And, and.left(), and.right());
        } else if (filterPredicate instanceof Or) {
            Or or  = (Or) filterPredicate;
            putBinaryOp(jsonObject, ParquetPredicateOperator.Or, or.left(), or.right());
        } else if (filterPredicate instanceof Not) {
            jsonObject.put("op", ParquetPredicateOperator.Not.ordinal());
            jsonObject.put("predicate", getSubJson(((Not) filterPredicate).child()));
        } else if (filterPredicate instanceof EqualTo) {
            EqualTo eq = (EqualTo) filterPredicate;
            putCompareOp(jsonObject, ParquetPredicateOperator.Eq, eq.attribute(), eq.value());
        } else if (filterPredicate instanceof GreaterThan) {
            GreaterThan gt = (GreaterThan) filterPredicate;
            putCompareOp(jsonObject, ParquetPredicateOperator.Gt, gt.attribute(), gt.value());
        } else if (filterPredicate instanceof GreaterThanOrEqual) {
            GreaterThanOrEqual gtEq = (GreaterThanOrEqual) filterPredicate;
            putCompareOp(jsonObject, ParquetPredicateOperator.GtEq, gtEq.attribute(), gtEq.value());
        } else if (filterPredicate instanceof LessThan) {
            LessThan lt = (LessThan) filterPredicate;
            putCompareOp(jsonObject, ParquetPredicateOperator.Lt, lt.attribute(), lt.value());
        } else if (filterPredicate instanceof LessThanOrEqual) {
            LessThanOrEqual ltEq = (LessThanOrEqual) filterPredicate;
            putCompareOp(jsonObject, ParquetPredicateOperator.LtEq, ltEq.attribute(), ltEq.value());
        } else if (filterPredicate instanceof IsNotNull) {
            IsNotNull isNotNull = (IsNotNull) filterPredicate;
            jsonObject.put("op", ParquetPredicateOperator.IsNotNull.ordinal());
            jsonObject.put("field", isNotNull.attribute());
        } else if (filterPredicate instanceof IsNull) {
            IsNull isNull = (IsNull) filterPredicate;
            jsonObject.put("op", ParquetPredicateOperator.IsNull.ordinal());
            jsonObject.put("field", isNull.attribute());
        } else if (filterPredicate instanceof In) {
            In in = (In) filterPredicate;
            jsonObject.put("op", ParquetPredicateOperator.In.ordinal());
            putInOp(jsonObject, in.attribute(), Arrays.stream(in.values()).toArray());
        } else {
            String simpleName = filterPredicate.getClass().getSimpleName();
            throw new ParquetDecodingException("Unsupported parquet push down filter operation: " + simpleName);
        }
        return jsonObject;
    }

    private void putInOp(JSONObject json, String field, Object[] values) {
        json.put("field", field);
        if (values == null) {
            json.put("type", 0);
        } else {
            putType(json, field);
            String[] literals = new String[values.length];
            for (int i = 0; i < values.length; i++) {
                literals[i] = getValue(values[i]);
            }
            json.put("literal", literals);
        }
    }

    private void putCompareOp(JSONObject json, ParquetPredicateOperator op, String field, Object value) {
        json.put("op", op.ordinal());
        if (allFieldsNames.contains(field)) {
            json.put("field", field);
        } else {
            throw new ParquetDecodingException("Unsupported parquet push down missing columns: " + field);
        }

        if (value == null) {
            json.put("type", 0);
        } else {
            putType(json, field);
            json.put("literal", getValue(value));
        }
    }

    private void putType(JSONObject json, String field) {
        DataType value = requiredSchema.apply(field).dataType();
        json.put("type", getParquetPredicateDataType(value));
        if (value instanceof DecimalType) {
            DecimalType decimal = (DecimalType) value;
            json.put("precision", decimal.precision());
            json.put("scale", decimal.scale());
        }
    }

    private int getParquetPredicateDataType(DataType value) {
        if (value instanceof IntegerType) {
            return ParquetPredicateDataType.Integer.ordinal();
        } else if (value instanceof LongType) {
            return ParquetPredicateDataType.Long.ordinal();
        } else if (value instanceof StringType) {
            return ParquetPredicateDataType.String.ordinal();
        } else if (value instanceof DateType) {
            return ParquetPredicateDataType.Date32.ordinal();
        } else if (value instanceof DecimalType) {
            return ParquetPredicateDataType.Decimal.ordinal();
        } else if (value instanceof BooleanType) {
            return ParquetPredicateDataType.Bool.ordinal();
        } else if (value instanceof ShortType) {
            return ParquetPredicateDataType.Short.ordinal();
        } else if (value instanceof DoubleType) {
            return ParquetPredicateDataType.Double.ordinal();
        } else if (value instanceof TimestampType) {
            return ParquetPredicateDataType.Timestamp.ordinal();
        } else {
            String simpleName = value.getClass().getSimpleName();
            throw new ParquetDecodingException("Unsupported parquet push down filter date type: " + simpleName);
        }
    }

    private String getValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Integer) {
            return value.toString();
        } else if (value instanceof Long) {
            return value.toString();
        } else if (value instanceof String) {
            return (String) value;
        } else if (value instanceof LocalDate) {
            return value.toString();
        } else if (value instanceof BigDecimal) {
            return value.toString();
        } else if (value instanceof Boolean) {
            return value.toString();
        } else if (value instanceof Short) {
            return value.toString();
        } else if (value instanceof Double) {
            return value.toString();
        } else {
            String simpleName = value.getClass().getSimpleName();
            throw new ParquetDecodingException("Unsupported parquet push down filter date type: " + simpleName);
        }
    }

    private void putBinaryOp(JSONObject json, ParquetPredicateOperator op, Filter left, Filter right) {
        json.put("op", op.ordinal());
        json.put("left", getSubJson(left));
        json.put("right", getSubJson(right));
    }

    private void tryToAdjustTimestampVec(LongVec longVec, long rowNumber, int index) {
        if (nativeSupportTimestampRebase && nativeSupportInt96Rebase) {
            return;
        }
        Type parquetType = parquetTypes.get(index);
        if (parquetType == null) {
            throw new RuntimeException("parquetType is null. index: " + index);
        }
        if (parquetType.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
                && !nativeSupportTimestampRebase) {
            long adjustValue;
            for (int rowIndex = 0; rowIndex < rowNumber; rowIndex++) {
                adjustValue = (long) timestampRebaseFunc.apply(longVec.get(rowIndex));
                longVec.set(rowIndex, adjustValue);
            }
            return;
        }
        if (parquetType.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96
                && !nativeSupportInt96Rebase) {
            long adjustValue;
            for (int rowIndex = 0; rowIndex < rowNumber; rowIndex++) {
                adjustValue = (long) int96RebaseFunc.apply(longVec.get(rowIndex));
                longVec.set(rowIndex, adjustValue);
            }
        }
    }

    public int next(Vec[] vecList, boolean[] missingColumns, List<DataType> types) {
        int colsCount = missingColumns.length;
        long[] vecNativeIds = new long[types.size()];
        long rtn = jniReader.recordReaderNext(parquetReader, vecNativeIds);
        if (rtn == 0) {
            return 0;
        }
        int nativeGetId = 0;
        for (int i = 0; i < colsCount; i++) {
            if (missingColumns[i]) {
                continue;
            }
            DataType type = types.get(nativeGetId);
            if (type instanceof LongType) {
                vecList[i] = new LongVec(vecNativeIds[nativeGetId]);
            } else if (type instanceof BooleanType) {
                vecList[i] = new BooleanVec(vecNativeIds[nativeGetId]);
            } else if (type instanceof ShortType) {
                vecList[i] = new ShortVec(vecNativeIds[nativeGetId]);
            } else if (type instanceof IntegerType) {
                vecList[i] = new IntVec(vecNativeIds[nativeGetId]);
            } else if (type instanceof DecimalType) {
                if (DecimalType.is64BitDecimalType(type)) {
                    vecList[i] = new LongVec(vecNativeIds[nativeGetId]);
                } else {
                    vecList[i] = new Decimal128Vec(vecNativeIds[nativeGetId]);
                }
            } else if (type instanceof DoubleType) {
                vecList[i] = new DoubleVec(vecNativeIds[nativeGetId]);
            } else if (type instanceof StringType) {
                vecList[i] = new VarcharVec(vecNativeIds[nativeGetId]);
            } else if (type instanceof DateType) {
                vecList[i] = new IntVec(vecNativeIds[nativeGetId]);
            } else if (type instanceof ByteType) {
                vecList[i] = new VarcharVec(vecNativeIds[nativeGetId]);
            } else if (type instanceof TimestampType) {
                vecList[i] = new LongVec(vecNativeIds[nativeGetId]);
                tryToAdjustTimestampVec((LongVec) vecList[i], rtn, i);
            } else {
                throw new RuntimeException("Unsupport type for ColumnarFileScan: " + type.typeName());
            }
            nativeGetId++;
        }
        return (int)rtn;
    }

    public void close() {
        jniReader.recordReaderClose(parquetReader);
    }
}