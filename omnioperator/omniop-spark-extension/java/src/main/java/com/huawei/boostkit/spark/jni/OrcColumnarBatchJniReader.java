/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.vector.*;

import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.orc.OrcFile.ReaderOptions;
import org.apache.orc.Reader.Options;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.orc.TypeDescription;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class OrcColumnarBatchJniReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrcColumnarBatchJniReader.class);

    public long reader;
    public long recordReader;
    public long batchReader;
    public int[] colsToGet;
    public int realColsCnt;

    public OrcColumnarBatchJniReader() {
        NativeLoader.getInstance();
    }

    public JSONObject getSubJson(ExpressionTree etNode) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("op", etNode.getOperator().ordinal());
        if (etNode.getOperator().toString().equals("LEAF")) {
            jsonObject.put("leaf", etNode.toString());
            return jsonObject;
        }
        ArrayList<JSONObject> child = new ArrayList<JSONObject>();
        for (ExpressionTree childNode : etNode.getChildren()) {
            JSONObject rtnJson = getSubJson(childNode);
            child.add(rtnJson);
        }
        jsonObject.put("child", child);
        return jsonObject;
    }

    public String padZeroForDecimals(String [] decimalStrArray, int decimalScale) {
        String decimalVal = ""; // Integer without decimals, eg: 12345
        if (decimalStrArray.length == 2) { // Integer with decimals, eg: 12345.6
            decimalVal = decimalStrArray[1];
        }
        // If the length of the formatted number string is insufficient, pad '0's.
        return String.format("%1$-" + decimalScale + "s", decimalVal).replace(' ', '0');
    }

    public JSONObject getLeavesJson(List<PredicateLeaf> leaves, TypeDescription schema) {
        JSONObject jsonObjectList = new JSONObject();
        for (int i = 0; i < leaves.size(); i++) {
            PredicateLeaf pl = leaves.get(i);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("op", pl.getOperator().ordinal());
            jsonObject.put("name", pl.getColumnName());
            jsonObject.put("type", pl.getType().ordinal());
            if (pl.getLiteral() != null) {
                if (pl.getType() == PredicateLeaf.Type.DATE) {
                    jsonObject.put("literal", ((int)Math.ceil(((Date)pl.getLiteral()).getTime()* 1.0/3600/24/1000)) + "");
                } else if (pl.getType() == PredicateLeaf.Type.DECIMAL) {
                    int decimalP = schema.findSubtype(pl.getColumnName()).getPrecision();
                    int decimalS = schema.findSubtype(pl.getColumnName()).getScale();
                    String[] spiltValues = pl.getLiteral().toString().split("\\.");
                    if (decimalS == 0) {
                        jsonObject.put("literal", spiltValues[0] + " " + decimalP + " " + decimalS);
                    } else {
                        String scalePadZeroStr = padZeroForDecimals(spiltValues, decimalS);
                        jsonObject.put("literal", spiltValues[0] + "." + scalePadZeroStr + " " + decimalP + " " + decimalS);
                    }
                } else {
                    jsonObject.put("literal", pl.getLiteral().toString());
                }
            } else {
                jsonObject.put("literal", "");
            }
            if ((pl.getLiteralList() != null) && (pl.getLiteralList().size() != 0)){
                List<String> lst = new ArrayList<String>();
                for (Object ob : pl.getLiteralList()) {
                    if (pl.getType() == PredicateLeaf.Type.DECIMAL) {
                        int decimalP =  schema.findSubtype(pl.getColumnName()).getPrecision();
                        int decimalS =  schema.findSubtype(pl.getColumnName()).getScale();
                        String[] spiltValues = ob.toString().split("\\.");
                        if (decimalS == 0) {
                            lst.add(spiltValues[0] + " " + decimalP + " " + decimalS);
                        } else {
                            String scalePadZeroStr = padZeroForDecimals(spiltValues, decimalS);
                            lst.add(spiltValues[0] + "." + scalePadZeroStr + " " + decimalP + " " + decimalS);
                        }
                    } else if (pl.getType() == PredicateLeaf.Type.DATE) {
                        lst.add(((int)Math.ceil(((Date)pl.getLiteral()).getTime()* 1.0/3600/24/1000)) + "");
                    } else {
                        lst.add(ob.toString());
                    }
                }
                jsonObject.put("literalList", lst);
            } else {
                jsonObject.put("literalList", new ArrayList<String>());
            }
            jsonObjectList.put("leaf-" + i, jsonObject);
        }
        return jsonObjectList;
    }

    /**
     * Init Orc reader.
     *
     * @param path split file path
     * @param options split file options
     */
    public long initializeReaderJava(String path, ReaderOptions options) {
        JSONObject job = new JSONObject();
        if (options.getOrcTail() == null) {
            job.put("serializedTail", "");
        } else {
            job.put("serializedTail", options.getOrcTail().getSerializedTail().toString());
        }
        job.put("tailLocation", 9223372036854775807L);
        reader = initializeReader(path, job);
        return reader;
    }

    /**
     * Init Orc RecordReader.
     *
     * @param options split file options
     */
    public long initializeRecordReaderJava(Options options) {
        JSONObject job = new JSONObject();
        if (options.getInclude() == null) {
            job.put("include", "");
        } else {
            job.put("include", options.getInclude().toString());
        }
        job.put("offset", options.getOffset());
        job.put("length", options.getLength());
        if (options.getSearchArgument() != null) {
            LOGGER.debug("SearchArgument: {}", options.getSearchArgument().toString());
            JSONObject jsonexpressionTree = getSubJson(options.getSearchArgument().getExpression());
            job.put("expressionTree", jsonexpressionTree);
            JSONObject jsonleaves = getLeavesJson(options.getSearchArgument().getLeaves(), options.getSchema());
            job.put("leaves", jsonleaves);
        }

        List<String> allCols;
        if (options.getColumnNames() == null) {
            allCols = Arrays.asList(getAllColumnNames(reader));
        } else {
            allCols = Arrays.asList(options.getColumnNames());
        }
        ArrayList<String> colToInclu = new ArrayList<String>();
        List<String> optionField = options.getSchema().getFieldNames();
        colsToGet = new int[optionField.size()];
        realColsCnt = 0;
        for (int i = 0; i < optionField.size(); i++) {
            if (allCols.contains(optionField.get(i))) {
                colToInclu.add(optionField.get(i));
                colsToGet[i] = 0;
                realColsCnt++;
            } else {
                colsToGet[i] = -1;
            }
        }
        job.put("includedColumns", colToInclu.toArray());
        recordReader = initializeRecordReader(reader, job);
        return recordReader;
    }

    public long initBatchJava(long batchSize) {
        batchReader = initializeBatch(recordReader, batchSize);
        return 0;
    }

    public long getNumberOfRowsJava() {
        return getNumberOfRows(recordReader, batchReader);
    }

    public long getRowNumber() {
        return recordReaderGetRowNumber(recordReader);
    }

    public float getProgress() {
        return recordReaderGetProgress(recordReader);
    }

    public void close() {
        recordReaderClose(recordReader, reader, batchReader);
    }

    public void seekToRow(long rowNumber) {
        recordReaderSeekToRow(recordReader, rowNumber);
    }

    public void convertJulianToGreGorian(IntVec intVec, long rowNumber) {
        int gregorianValue;
        for (int rowIndex = 0; rowIndex < rowNumber; rowIndex++) {
            gregorianValue = RebaseDateTime.rebaseJulianToGregorianDays(intVec.get(rowIndex));
            intVec.set(rowIndex, gregorianValue);
        }
    }

    public int next(Vec[] vecList) {
        int vectorCnt = vecList.length;
        int[] typeIds = new int[realColsCnt];
        long[] vecNativeIds = new long[realColsCnt];
        long rtn = recordReaderNext(recordReader, batchReader, typeIds, vecNativeIds);
        if (rtn == 0) {
            return 0;
        }
        int nativeGetId = 0;
        for (int i = 0; i < vectorCnt; i++) {
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
                case OMNI_DOUBLE: {
                    vecList[i] = new DoubleVec(vecNativeIds[nativeGetId]);
                    break;
                }
                case OMNI_VARCHAR: {
                    vecList[i] = new VarcharVec(vecNativeIds[nativeGetId]);
                    break;
                }
                case OMNI_DECIMAL128: {
                    vecList[i] = new Decimal128Vec(vecNativeIds[nativeGetId], Decimal128DataType.DECIMAL128);
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

    public native long initializeReader(String path, JSONObject job);

    public native long initializeRecordReader(long reader, JSONObject job);

    public native long initializeBatch(long rowReader, long batchSize);

    public native long recordReaderNext(long rowReader, long batchReader, int[] typeId, long[] vecNativeId);

    public native long recordReaderGetRowNumber(long rowReader);

    public native float recordReaderGetProgress(long rowReader);

    public native void recordReaderClose(long rowReader, long reader, long batchReader);

    public native void recordReaderSeekToRow(long rowReader, long rowNumber);

    public native String[] getAllColumnNames(long reader);

    public native long getNumberOfRows(long rowReader, long batch);
}
