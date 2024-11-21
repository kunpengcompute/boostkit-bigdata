/*
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
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
import com.huawei.boostkit.write.jni.OrcColumnarBatchJniWriter;
import com.huawei.boostkit.write.jni.ParquetColumnarBatchJniWriter;

import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.orc.OrcFile;
import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.spark.sql.execution.vectorized.OmniColumnVector;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VarcharType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;

public class ParquetColumnarBatchWriter {
    public ParquetColumnarBatchWriter() {
        jniWriter = new ParquetColumnarBatchJniWriter();
    }

    public enum ParquetLibTypeKind {
        NA,
        /// Boolean as 1 bit, LSB bit-packed ordering
        BOOL,

        /// Unsigned 8-bit little-endian integer
        UINT8,

        /// Signed 8-bit little-endian integer
        INT8,

        /// Unsigned 16-bit little-endian integer
        UINT16,

        /// Signed 16-bit little-endian integer
        INT16,

        /// Unsigned 32-bit little-endian integer
        UINT32,

        /// Signed 32-bit little-endian integer
        INT32,

        /// Unsigned 64-bit little-endian integer
        UINT64,

        /// Signed 64-bit little-endian integer
        INT64,

        /// 2-byte floating point value
        HALF_FLOAT,

        /// 4-byte floating point value
        FLOAT,

        /// 8-byte floating point value
        DOUBLE,

        /// UTF8 variable-length string as List<Char>
        STRING,

        /// Variable-length bytes (no guarantee of UTF8-ness)
        BINARY,

        /// Fixed-size binary. Each value occupies the same number of bytes
        FIXED_SIZE_BINARY,

        /// int32_t days since the UNIX epoch
        DATE32,

        /// int64_t milliseconds since the UNIX epoch
        DATE64,

        /// Exact timestamp encoded with int64 since UNIX epoch
        /// Default unit millisecond
        TIMESTAMP,

        /// Time as signed 32-bit integer, representing either seconds or
        /// milliseconds since midnight
        TIME32,

        /// Time as signed 64-bit integer, representing either microseconds or
        /// nanoseconds since midnight
        TIME64,

        /// YEAR_MONTH interval in SQL style
        INTERVAL_MONTHS,

        /// DAY_TIME interval in SQL style
        INTERVAL_DAY_TIME,

        /// Precision- and scale-based decimal type with 128 bits.
        DECIMAL128,

        /// Defined for backward-compatibility.
        // DECIMAL = DECIMAL128,

        /// Precision- and scale-based decimal type with 256 bits.
        DECIMAL256,

        /// A list of some logical data type
        LIST,

        /// Struct of logical types
        STRUCT,

        /// Sparse unions of logical types
        SPARSE_UNION,

        /// Dense unions of logical types
        DENSE_UNION,

        /// Dictionary-encoded type, also called "categorical" or "factor"
        /// in other programming languages. Holds the dictionary value
        /// type but not the dictionary itself, which is part of the
        /// ArrayData struct
        DICTIONARY,

        /// Map, a repeated struct logical type
        MAP,

        /// Custom data type, implemented by user
        EXTENSION,

        /// Fixed size list of some logical type
        FIXED_SIZE_LIST,

        /// Measure of elapsed time in either seconds, milliseconds, microseconds
        /// or nanoseconds.
        DURATION,

        /// Like STRING, but with 64-bit offsets
        LARGE_STRING,

        /// Like BINARY, but with 64-bit offsets
        LARGE_BINARY,

        /// Like LIST, but with 64-bit offsets
        LARGE_LIST,

        /// Calendar interval type with three fields.
        INTERVAL_MONTH_DAY_NANO,

        // Leave this at the end
        MAX_ID
    }

    public void initializeWriterJava(Path path) throws IOException {
        JSONObject writerOptionsJson = new JSONObject();
        String ugi = UserGroupInformation.getCurrentUser().toString();

        URI uri = path.toUri();

        writerOptionsJson.put("uri", path.toString());
        writerOptionsJson.put("ugi", ugi);

        writerOptionsJson.put("host", uri.getHost() == null ? "" : uri.getHost());
        writerOptionsJson.put("scheme", uri.getScheme() == null ? "" : uri.getScheme());
        writerOptionsJson.put("port", uri.getPort());
        writerOptionsJson.put("path", uri.getPath() == null ? "" : uri.getPath());

        writer = jniWriter.initializeWriter(writerOptionsJson);
    }

    public void convertGreGorianToJulian(IntVec intVec, int startPos, int endPos) {
        int julianValue;
        for (int rowIndex = startPos; rowIndex < endPos; rowIndex++) {
            julianValue = RebaseDateTime.rebaseGregorianToJulianDays(intVec.get(rowIndex));
            intVec.set(rowIndex, julianValue);
        }
    }

    public void initializeSchemaJava(StructType dataSchema){
        int schemaLength = dataSchema.length();
        String [] fieldNames = new String[schemaLength];
        int[] fieldTypes = new int[schemaLength];
        boolean[] nullables = new boolean[schemaLength];
        String [] metaDataKeys = new String[schemaLength];
        String [] metaDataValues = new String[schemaLength];
        for (int i = 0; i < schemaLength; i++){
            StructField field = dataSchema.fields()[i];
            fieldNames[i] = field.name();
            fieldTypes[i] = sparkTypeToParquetLibType(field.dataType());
            nullables[i] = field.nullable();
        }
        jniWriter.initializeSchema(writer, fieldNames, fieldTypes, nullables,extractDecimalParam(dataSchema));
    }

    public int sparkTypeToParquetLibType(DataType dataType) {
        if (dataType instanceof BooleanType) {
            return ParquetLibTypeKind.BOOL.ordinal();
        } else if (dataType instanceof ShortType) {
            return ParquetLibTypeKind.INT16.ordinal();
        } else if (dataType instanceof IntegerType) {
            IntegerType integerType = (IntegerType) dataType;
            switch (integerType.defaultSize()){
                case 1:
                    return ParquetLibTypeKind.INT8.ordinal();
                case 2:
                    return ParquetLibTypeKind.INT16.ordinal();
                case 4:
                    return ParquetLibTypeKind.INT32.ordinal();
                case 8:
                    return ParquetLibTypeKind.DATE64.ordinal();
                default:
                    throw new RuntimeException(
                            "UnSupport size " + integerType.defaultSize() + " of integer type");
            }
        } else if (dataType instanceof LongType) {
            return ParquetLibTypeKind.INT64.ordinal();
        } else if (dataType instanceof DateType) {
            DateType dateType = (DateType) dataType;
            switch (dateType.defaultSize()){
                case 4:
                    return ParquetLibTypeKind.DATE32.ordinal();
                case 8:
                    return ParquetLibTypeKind.DATE64.ordinal();
                default:
                    throw new RuntimeException(
                            "UnSupport size " + dateType.defaultSize() + " of date type");
            }
        } else if (dataType instanceof DoubleType) {
            return ParquetLibTypeKind.DOUBLE.ordinal();
        } else if (dataType instanceof VarcharType) {
            return ParquetLibTypeKind.STRING.ordinal();
        } else if (dataType instanceof StringType) {
            return ParquetLibTypeKind.STRING.ordinal();
        } else if (dataType instanceof CharType) {
            return ParquetLibTypeKind.STRING.ordinal();
        } else if (dataType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) dataType;
            switch (decimalType.defaultSize()){
                case 8:
                    return ParquetLibTypeKind.DECIMAL128.ordinal();
                case 16:
                    return ParquetLibTypeKind.DECIMAL128.ordinal();
                default:
                    throw new RuntimeException(
                            "UnSupport size " + decimalType.defaultSize() + " of decimal type");
            }
        } else {
            throw new RuntimeException(
                    "UnSupport type convert  spark type " + dataType.simpleString() + " to parquet lib type");
        }
    }

    public int[][] extractDecimalParam(StructType dataSchema) {
        int paramNum = 2;
        int precisionIndex = 0;
        int scaleIndex = 1;
        int[][] decimalParams = new int[dataSchema.length()][paramNum];
        for (int i = 0; i < dataSchema.length(); i++) {
            DataType dataType = dataSchema.fields()[i].dataType();
            if (dataType instanceof DecimalType) {
                DecimalType decimal = (DecimalType) dataType;
                decimalParams[i][precisionIndex] = decimal.precision();
                decimalParams[i][scaleIndex] = decimal.scale();
            }
        }
        return decimalParams;
    }

    public void write(int[] omniTypes, boolean[] dataColumnsIds, ColumnarBatch batch) {
        JSONObject job = new JSONObject();

        long[] vecNativeIds = new long[batch.numCols()];
        for (int i = 0; i < batch.numCols(); i++) {
            OmniColumnVector omniVec = (OmniColumnVector) batch.column(i);
            Vec vec = omniVec.getVec();
            vecNativeIds[i] = vec.getNativeVector();
            boolean isDateType = (omniTypes[i] == 8);
            if (isDateType) {
                convertGreGorianToJulian((IntVec) vec, 0, batch.numRows());
            }
        }

        jniWriter.write(writer, vecNativeIds, omniTypes, dataColumnsIds, batch.numRows());
    }

    public void splitWrite(int[] omniTypes, int[] allOmniTypes, boolean[] dataColumnsIds, ColumnarBatch inputBatch, long startPos, long endPos) {
        long[] vecNativeIds = new long[inputBatch.numCols()];
        for (int i = 0; i < inputBatch.numCols(); i++) {
            OmniColumnVector omniVec = (OmniColumnVector) inputBatch.column(i);
            Vec vec = omniVec.getVec();
            vecNativeIds[i] = vec.getNativeVector();
            boolean isDateType = (allOmniTypes[i] == 8);
            if (isDateType) {
                convertGreGorianToJulian((IntVec) vec, (int) startPos, (int) endPos);
            }
        }

        jniWriter.splitWrite(writer, vecNativeIds, omniTypes, dataColumnsIds, startPos, endPos);
    }

    public long writer;

    public long schema;
    public ParquetColumnarBatchJniWriter jniWriter;
}
