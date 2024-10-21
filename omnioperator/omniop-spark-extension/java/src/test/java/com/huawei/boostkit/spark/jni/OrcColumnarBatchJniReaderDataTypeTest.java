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
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.OrcFile;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_LONG;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_VARCHAR;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_INT;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING )
public class OrcColumnarBatchJniReaderDataTypeTest extends TestCase {
    public OrcColumnarBatchScanReader orcColumnarBatchScanReader;

    @Before
    public void setUp() throws Exception {
        orcColumnarBatchScanReader = new OrcColumnarBatchScanReader();
        initReaderJava();
        initRecordReaderJava();
        initBatch();
    }

    @After
    public void tearDown() throws Exception {
        System.out.println("orcColumnarBatchScanReader test finished");
    }

    public void initReaderJava() {
        File directory = new File("src/test/java/com/huawei/boostkit/spark/jni/orcsrc/000000_0");
        String absolutePath = directory.getAbsolutePath();
        System.out.println(absolutePath);
        URI uri = null;
        try {
            uri = new URI(absolutePath);
        } catch (URISyntaxException ignore) {
            // if URISyntaxException thrown, next line assertNotNull will interrupt the test
        }
        assertNotNull(uri);
        orcColumnarBatchScanReader.reader = orcColumnarBatchScanReader.initializeReaderJava(uri);
        assertTrue(orcColumnarBatchScanReader.reader != 0);
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

        orcColumnarBatchScanReader.recordReader = orcColumnarBatchScanReader.jniReader.initializeRecordReader(orcColumnarBatchScanReader.reader, job);
        assertTrue(orcColumnarBatchScanReader.recordReader != 0);
    }

    public void initBatch() {
        orcColumnarBatchScanReader.batchReader = orcColumnarBatchScanReader.jniReader.initializeBatch(orcColumnarBatchScanReader.recordReader, 4096);
        assertTrue(orcColumnarBatchScanReader.batchReader != 0);
    }

    @Test
    public void testNext() {
        int[] typeId = new int[] {OMNI_LONG.ordinal(), OMNI_VARCHAR.ordinal(), OMNI_VARCHAR.ordinal(), OMNI_INT.ordinal()};
        long[] vecNativeId = new long[4];
        long rtn = orcColumnarBatchScanReader.jniReader.recordReaderNext(orcColumnarBatchScanReader.recordReader, orcColumnarBatchScanReader.batchReader, typeId, vecNativeId);
        assertTrue(rtn == 4096);
        LongVec vec1 = new LongVec(vecNativeId[0]);
        VarcharVec vec2 = new VarcharVec(vecNativeId[1]);
        VarcharVec vec3 = new VarcharVec(vecNativeId[2]);
        IntVec vec4 = new IntVec(vecNativeId[3]);
        assertTrue(vec1.get(10) == 11);
        String tmp1 = new String(vec2.get(4080));
        assertTrue(tmp1.equals("AAAAAAAABPPAAAAA"));
        String tmp2 = new String(vec3.get(4070));
        assertTrue(tmp2.equals("Particular, arab cases shall like less current, different names. Computers start for the changes. Scottish, trying exercises operate marks; long, supreme miners may ro"));
        assertTrue(0 == vec4.get(1000));
        vec1.close();
        vec2.close();
        vec3.close();
        vec4.close();
    }

    // Here we test OMNI_LONG type instead of OMNI_INT in 4th field.
    @Test
    public void testNextIfSchemaChange() {
        int[] typeId = new int[] {OMNI_LONG.ordinal(), OMNI_VARCHAR.ordinal(), OMNI_VARCHAR.ordinal(), OMNI_LONG.ordinal()};
        long[] vecNativeId = new long[4];
        long rtn = orcColumnarBatchScanReader.jniReader.recordReaderNext(orcColumnarBatchScanReader.recordReader, orcColumnarBatchScanReader.batchReader, typeId, vecNativeId);
        assertTrue(rtn == 4096);
        LongVec vec1 = new LongVec(vecNativeId[0]);
        VarcharVec vec2 = new VarcharVec(vecNativeId[1]);
        VarcharVec vec3 = new VarcharVec(vecNativeId[2]);
        LongVec vec4 = new LongVec(vecNativeId[3]);
        assertTrue(vec1.get(10) == 11);
        String tmp1 = new String(vec2.get(4080));
        assertTrue(tmp1.equals("AAAAAAAABPPAAAAA"));
        String tmp2 = new String(vec3.get(4070));
        assertTrue(tmp2.equals("Particular, arab cases shall like less current, different names. Computers start for the changes. Scottish, trying exercises operate marks; long, supreme miners may ro"));
        assertTrue(0 == vec4.get(1000));
        vec1.close();
        vec2.close();
        vec3.close();
        vec4.close();
    }
}