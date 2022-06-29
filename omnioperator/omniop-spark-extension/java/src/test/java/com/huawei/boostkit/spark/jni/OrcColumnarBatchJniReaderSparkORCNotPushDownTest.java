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
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
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
public class OrcColumnarBatchJniReaderSparkORCNotPushDownTest extends TestCase {
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
        File directory = new File("src/test/java/com/huawei/boostkit/spark/jni/orcsrc/part-00000-2d6ca713-08b0-4b40-828c-f7ee0c81bb9a-c000.snappy.orc");
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
        // type long
        includedColumns.add("i_item_sk");
        // type char 16
        includedColumns.add("i_item_id");
        // type char 200
        includedColumns.add("i_item_desc");
        // type int
        includedColumns.add("i_current_price");
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
        int[] typeId = new int[4];
        long[] vecNativeId = new long[4];
        long rtn = orcColumnarBatchJniReader.recordReaderNext(orcColumnarBatchJniReader.recordReader, orcColumnarBatchJniReader.reader, orcColumnarBatchJniReader.batchReader, typeId, vecNativeId);
        assertTrue(rtn == 4096);
        LongVec vec1 = new LongVec(vecNativeId[0]);
        VarcharVec vec2 = new VarcharVec(vecNativeId[1]);
        VarcharVec vec3 = new VarcharVec(vecNativeId[2]);
        IntVec vec4 = new IntVec(vecNativeId[3]);

        assertTrue(vec1.get(4095) == 4096);
        String tmp1 = new String(vec2.get(4095));
        assertTrue(tmp1.equals("AAAAAAAAAAABAAAA"));
        String tmp2 = new String(vec3.get(4095));
        assertTrue(tmp2.equals("Find"));
        assertTrue(vec4.get(4095) == 6);
    }
}
