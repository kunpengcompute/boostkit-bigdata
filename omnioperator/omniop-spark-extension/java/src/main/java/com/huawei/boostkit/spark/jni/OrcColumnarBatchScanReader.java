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
import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.orc.OrcFile.ReaderOptions;
import org.apache.orc.Reader.Options;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

public class OrcColumnarBatchScanReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrcColumnarBatchScanReader.class);

    private boolean nativeSupportTimestampRebase;
    public long reader;
    public long recordReader;
    public long batchReader;
    public int[] colsToGet;
    public int realColsCnt;

    public ArrayList<String> fildsNames;

    public ArrayList<String> colToInclu;

    public String[] requiredfieldNames;

    public int[] precisionArray;

    public int[] scaleArray;

    public OrcColumnarBatchJniReader jniReader;
    public OrcColumnarBatchScanReader() {
        jniReader = new OrcColumnarBatchJniReader();
        fildsNames = new ArrayList<String>();
    }

    public JSONObject getSubJson(ExpressionTree node) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("op", node.getOperator().ordinal());
        if (node.getOperator().toString().equals("LEAF")) {
            jsonObject.put("leaf", node.toString());
            return jsonObject;
        }
        ArrayList<JSONObject> child = new ArrayList<JSONObject>();
        for (ExpressionTree childNode : node.getChildren()) {
            JSONObject rtnJson = getSubJson(childNode);
            child.add(rtnJson);
        }
        jsonObject.put("child", child);
        return jsonObject;
    }

    public String padZeroForDecimals(String [] decimalStrArray, int decimalScale) {
        String decimalVal = "";
        if (decimalStrArray.length == 2) {
            decimalVal = decimalStrArray[1];
        }
        // If the length of the formatted number string is insufficient, pad '0's.
        return String.format("%1$-" + decimalScale + "s", decimalVal).replace(' ', '0');
    }

    public int getPrecision(String colname) {
        for (int i = 0; i < requiredfieldNames.length; i++) {
            if (colname.equals(requiredfieldNames[i])) {
                return precisionArray[i];
            }
        }

        return -1;
    }

    public int getScale(String colname) {
        for (int i = 0; i < requiredfieldNames.length; i++) {
            if (colname.equals(requiredfieldNames[i])) {
                return scaleArray[i];
            }
        }

        return -1;
    }

    public JSONObject getLeavesJson(List<PredicateLeaf> leaves) {
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
                    int decimalP = getPrecision(pl.getColumnName());
                    int decimalS = getScale(pl.getColumnName());
                    String[] spiltValues = pl.getLiteral().toString().split("\\.");
                    if (decimalS == 0) {
                        jsonObject.put("literal", spiltValues[0] + " " + decimalP + " " + decimalS);
                    } else {
                        String scalePadZeroStr = padZeroForDecimals(spiltValues, decimalS);
                        jsonObject.put("literal", spiltValues[0] + "." + scalePadZeroStr + " " + decimalP + " " + decimalS);
                    }
                } else if (pl.getType() == PredicateLeaf.Type.TIMESTAMP) {
                    Timestamp t = (Timestamp)pl.getLiteral();
                    jsonObject.put("literal",  formatSecs(t.getTime() / TimestampTreeWriter.MILLIS_PER_SECOND) + " " + formatNanos(t.getNanos()));
                } else {
                    jsonObject.put("literal", pl.getLiteral().toString());
                }
            } else {
                jsonObject.put("literal", "");
            }
            if ((pl.getLiteralList() != null) && (pl.getLiteralList().size() != 0)){
                List<String> lst = new ArrayList<>();
                for (Object ob : pl.getLiteralList()) {
                    if (ob == null) {
                        lst.add(null);
                        continue;
                    }
                    if (pl.getType() == PredicateLeaf.Type.DECIMAL) {
                        int decimalP = getPrecision(pl.getColumnName());
                        int decimalS = getScale(pl.getColumnName());
                        String[] spiltValues = ob.toString().split("\\.");
                        if (decimalS == 0) {
                            lst.add(spiltValues[0] + " " + decimalP + " " + decimalS);
                        } else {
                            String scalePadZeroStr = padZeroForDecimals(spiltValues, decimalS);
                            lst.add(spiltValues[0] + "." + scalePadZeroStr + " " + decimalP + " " + decimalS);
                        }
                    } else if (pl.getType() == PredicateLeaf.Type.DATE) {
                        lst.add(((int)Math.ceil(((Date)ob).getTime()* 1.0/3600/24/1000)) + "");
                    } else if (pl.getType() == PredicateLeaf.Type.TIMESTAMP) {
                        Timestamp t = (Timestamp)pl.getLiteral();
                        lst.add(formatSecs(t.getTime() / TimestampTreeWriter.MILLIS_PER_SECOND) + " " + formatNanos(t.getNanos()));
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
     * @param options split file options
     */
    public long initializeReaderJava(URI uri, ReaderOptions options) {
        JSONObject job = new JSONObject();
        if (options.getOrcTail() == null) {
            job.put("serializedTail", "");
        } else {
            job.put("serializedTail", options.getOrcTail().getSerializedTail().toString());
        }
        job.put("tailLocation", 9223372036854775807L);

        job.put("scheme", uri.getScheme() == null ? "" : uri.getScheme());
        job.put("host", uri.getHost() == null ? "" : uri.getHost());
        job.put("path", uri.getPath() == null ? "" : uri.getPath());
        job.put("port", uri.getPort() == -1 ? "" : String.valueOf(uri.getPort()));

        reader = jniReader.initializeReader(job, fildsNames);
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
        // When the number of pushedFilters > hive.CNF_COMBINATIONS_THRESHOLD, the expression is rewritten to
        // 'YES_NO_NULL'. Under the circumstances, filter push down will be skipped.
        if (options.getSearchArgument() != null
                && !options.getSearchArgument().toString().contains("YES_NO_NULL")) {
            LOGGER.debug("SearchArgument: {}", options.getSearchArgument().toString());
            JSONObject jsonexpressionTree = getSubJson(options.getSearchArgument().getExpression());
            job.put("expressionTree", jsonexpressionTree);
            JSONObject jsonleaves = getLeavesJson(options.getSearchArgument().getLeaves());
            job.put("leaves", jsonleaves);
        }

        job.put("includedColumns", colToInclu.toArray());
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
        long[] vecNativeIds = new long[realColsCnt];
        long rtn = jniReader.recordReaderNext(recordReader, batchReader, typeIds, vecNativeIds);
        if (rtn == 0) {
            return 0;
        }
        int nativeGetId = 0;
        for (int i = 0; i < realColsCnt; i++) {
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
}
