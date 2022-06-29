/*
 * Copyright (C) 2022-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

import junit.framework.TestCase;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.util.ArrayList;

import static org.junit.Assert.*;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING )
public class OrcColumnarBatchJniReaderNotPushDownTest extends TestCase {
    public OrcColumnarBatchJniReader orcColumnarBatchJniReader;

    @Before
    public void setUp() throws Exception {
        orcColumnarBatchJniReader = new OrcColumnarBatchJniReader();
        initReaderJava();
        initRecordReaderJava();
        initBatch();
    }

    @After
    public void tearDown() throws Exception {
        System.out.println("orcColumnarBatchJniReader test finished");
    }

    public void initReaderJava() {
        JSONObject job = new JSONObject();
        job.put("serializedTail","");
        job.put("tailLocation",9223372036854775807L);
        File directory = new File("src/test/java/com/huawei/boostkit/spark/jni/orcsrc/000000_0");
        System.out.println(directory.getAbsolutePath());
        orcColumnarBatchJniReader.reader = orcColumnarBatchJniReader.initializeReader(directory.getAbsolutePath(), job);
        assertTrue(orcColumnarBatchJniReader.reader != 0);
    }

    public void initRecordReaderJava() {
        JSONObject job = new JSONObject();
        job.put("include","");
        job.put("offset", 0);
        job.put("length", 3345152);

        ArrayList<String> includedColumns = new ArrayList<String>();
        includedColumns.add("i_item_sk");
        includedColumns.add("i_item_id");
        job.put("includedColumns", includedColumns.toArray());

        orcColumnarBatchJniReader.recordReader = orcColumnarBatchJniReader.initializeRecordReader(orcColumnarBatchJniReader.reader, job);
        assertTrue(orcColumnarBatchJniReader.recordReader != 0);
    }

    public void initBatch() {
        orcColumnarBatchJniReader.batchReader = orcColumnarBatchJniReader.initializeBatch(orcColumnarBatchJniReader.recordReader, 4096);
        assertTrue(orcColumnarBatchJniReader.batchReader != 0);
    }

    @Test
    public void testNext() {
        int[] typeId = new int[2];
        long[] vecNativeId = new long[2];
        long rtn = orcColumnarBatchJniReader.recordReaderNext(orcColumnarBatchJniReader.recordReader, orcColumnarBatchJniReader.reader, orcColumnarBatchJniReader.batchReader, typeId, vecNativeId);
        assertTrue(rtn == 4096);
        LongVec vec1 = new LongVec(vecNativeId[0]);
        VarcharVec vec2 = new VarcharVec(vecNativeId[1]);
        assertTrue(vec1.get(4090) == 4091);
        assertTrue(vec1.get(4000) == 4001);
        String tmp1 = new String(vec2.get(4090));
        String tmp2 = new String(vec2.get(4000));
        assertTrue(tmp1.equals("AAAAAAAAKPPAAAAA"));
        assertTrue(tmp2.equals("AAAAAAAAAKPAAAAA"));
    }
}
