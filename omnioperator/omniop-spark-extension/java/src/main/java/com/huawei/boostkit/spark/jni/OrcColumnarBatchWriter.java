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

import static org.apache.orc.CompressionKind.SNAPPY;
import static org.apache.orc.CompressionKind.ZLIB;

import com.huawei.boostkit.write.jni.OrcColumnarBatchJniWriter;

import nova.hetu.omniruntime.vector.*;

import org.apache.orc.OrcFile;
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
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VarcharType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.json.JSONObject;
import org.apache.spark.sql.catalyst.util.RebaseDateTime;

import java.net.URI;
import java.util.ArrayList;

public class OrcColumnarBatchWriter {
    public OrcColumnarBatchWriter() {
        jniWriter = new OrcColumnarBatchJniWriter();
    }

    public enum OrcLibTypeKind {
        BOOLEAN,
        BYTE,
        SHORT,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        STRING,
        BINARY,
        TIMESTAMP,
        LIST,
        MAP,
        STRUCT,
        UNION,
        DECIMAL,
        DATE,
        VARCHAR,
        CHAR,
        TIMESTAMP_INSTANT
    }

    public void initializeOutputStreamJava(URI uri) {
        JSONObject uriJson = new JSONObject();

        uriJson.put("scheme", uri.getScheme() == null ? "" : uri.getScheme());
        uriJson.put("host", uri.getHost() == null ? "" : uri.getHost());
        uriJson.put("port", uri.getPort());
        uriJson.put("path", uri.getPath() == null ? "" : uri.getPath());

        outputStream = jniWriter.initializeOutputStream(uriJson);
    }

    public void convertGreGorianToJulian(IntVec intVec, int startPos, int endPos) {
        int julianValue;
        for (int rowIndex = startPos; rowIndex < endPos; rowIndex++) {
            julianValue = RebaseDateTime.rebaseGregorianToJulianDays(intVec.get(rowIndex));
            intVec.set(rowIndex, julianValue);
        }
    }

    public void initializeSchemaTypeJava(StructType dataSchema) {
        schemaType = jniWriter.initializeSchemaType(sparkTypeToOrcLibType(dataSchema), extractSchemaName(dataSchema),
                extractDecimalParam(dataSchema));
    }

    /**
     * Init Orc writer.
     *
     * @param uri of output file path
     * @param options write file options
     */
    public void initializeWriterJava(URI uri, StructType dataSchema, OrcFile.WriterOptions options) {
        JSONObject writerOptionsJson = new JSONObject();

        JSONObject versionJob = new JSONObject();
        versionJob.put("major", options.getVersion().getMajor());
        versionJob.put("minor", options.getVersion().getMinor());
        writerOptionsJson.put("file version", versionJob);

        writerOptionsJson.put("compression", options.getCompress().ordinal());
        writerOptionsJson.put("strip size", options.getStripeSize());
        writerOptionsJson.put("compression block size", options.getBlockSize());
        writerOptionsJson.put("row index stride", options.getRowIndexStride());
        writerOptionsJson.put("compression strategy", options.getCompressionStrategy().ordinal());
        writerOptionsJson.put("padding tolerance", options.getPaddingTolerance());
        writerOptionsJson.put("columns use bloom filter", options.getBloomFilterColumns());
        writerOptionsJson.put("bloom filter fpp", options.getBloomFilterFpp());

        writer = jniWriter.initializeWriter(outputStream, schemaType, writerOptionsJson);
    }

    public int[] sparkTypeToOrcLibType(StructType dataSchema) {
        int[] orcLibType = new int[dataSchema.length()];
        for (int i = 0; i < dataSchema.length(); i++) {
            orcLibType[i] = sparkTypeToOrcLibType(dataSchema.fields()[i].dataType());
        }
        return orcLibType;
    }

    public int sparkTypeToOrcLibType(DataType dataType) {
        if (dataType instanceof BooleanType) {
            return OrcLibTypeKind.BOOLEAN.ordinal();
        } else if (dataType instanceof ShortType) {
            return OrcLibTypeKind.SHORT.ordinal();
        } else if (dataType instanceof IntegerType) {
            return OrcLibTypeKind.INT.ordinal();
        } else if (dataType instanceof LongType) {
            return OrcLibTypeKind.LONG.ordinal();
        } else if (dataType instanceof DateType) {
            return OrcLibTypeKind.DATE.ordinal();
        } else if (dataType instanceof DoubleType) {
            return OrcLibTypeKind.DOUBLE.ordinal();
        } else if (dataType instanceof VarcharType) {
            return OrcLibTypeKind.VARCHAR.ordinal();
        } else if (dataType instanceof StringType) {
            return OrcLibTypeKind.STRING.ordinal();
        } else if (dataType instanceof CharType) {
            return OrcLibTypeKind.CHAR.ordinal();
        } else if (dataType instanceof DecimalType) {
            return OrcLibTypeKind.DECIMAL.ordinal();
        } else {
            throw new RuntimeException(
                    "UnSupport type convert  spark type " + dataType.simpleString() + " to orc lib type");
        }
    }

    public String[] extractSchemaName(StructType dataSchema) {
        String[] schemaNames = new String[dataSchema.length()];
        for (int i = 0; i < dataSchema.length(); i++) {
            schemaNames[i] = dataSchema.fields()[i].name();
        }
        return schemaNames;
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

    public void write(int[] omniTypes, boolean[] dataColumnsIds, ColumnarBatch columBatch) {
        
        long[] vecNativeIds = new long[columBatch.numCols()];
        for (int i = 0; i < columBatch.numCols(); i++) {
            OmniColumnVector omniVec = (OmniColumnVector) columBatch.column(i);
            Vec vec = omniVec.getVec();
            vecNativeIds[i] = vec.getNativeVector();
            boolean isDateType = (omniTypes[i] == 8);
            if (isDateType) {
                convertGreGorianToJulian((IntVec) vec, 0, columBatch.numRows());
            }
        }

        jniWriter.write(writer, vecNativeIds, omniTypes, dataColumnsIds, columBatch.numRows());
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

    public void close() {
        jniWriter.close(outputStream, schemaType, writer);
    }

    public long outputStream;

    public long schemaType;

    public long writer;

    public OrcColumnarBatchJniWriter jniWriter;
}
