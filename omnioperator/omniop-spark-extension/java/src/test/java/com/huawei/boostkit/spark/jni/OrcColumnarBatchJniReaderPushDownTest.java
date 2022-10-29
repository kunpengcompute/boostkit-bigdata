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

import static org.junit.Assert.*;
import junit.framework.TestCase;
import org.apache.hadoop.mapred.join.ArrayListBackedIterator;
import org.apache.orc.OrcFile.ReaderOptions;
import org.apache.orc.Reader.Options;
import org.hamcrest.Condition;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;

import java.io.File;
import java.lang.reflect.Array;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING )
public class OrcColumnarBatchJniReaderPushDownTest extends TestCase {
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

        ArrayList<JSONObject> childList1 = new ArrayList<JSONObject>();
        JSONObject child1 = new JSONObject();
        child1.put("op", 3);
        child1.put("leaf", "leaf-0");
        childList1.add(child1);
        JSONObject subChild1 = new JSONObject();
        subChild1.put("op", 2);
        subChild1.put("child", childList1);

        ArrayList<JSONObject> childList2 = new ArrayList<JSONObject>();
        JSONObject child2 = new JSONObject();
        child2.put("op", 3);
        child2.put("leaf", "leaf-1");
        childList2.add(child2);
        JSONObject subChild2 = new JSONObject();
        subChild2.put("op", 2);
        subChild2.put("child", childList2);

        ArrayList<JSONObject> childs = new ArrayList<JSONObject>();
        childs.add(subChild1);
        childs.add(subChild2);

        JSONObject expressionTree = new JSONObject();
        expressionTree.put("op", 1);
        expressionTree.put("child", childs);
        job.put("expressionTree", expressionTree);

        JSONObject leaves = new JSONObject();
        JSONObject leaf0 = new JSONObject();
        leaf0.put("op", 6);
        leaf0.put("name", "i_item_sk");
        leaf0.put("type", 0);
        leaf0.put("literal", "");
        leaf0.put("literalList", new ArrayList<String>());

        JSONObject leaf1 = new JSONObject();
        leaf1.put("op", 3);
        leaf1.put("name", "i_item_sk");
        leaf1.put("type", 0);
        leaf1.put("literal", "100");
        leaf1.put("literalList", new ArrayList<String>());

        leaves.put("leaf-0", leaf0);
        leaves.put("leaf-1", leaf1);
        job.put("leaves", leaves);

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
        long rtn = orcColumnarBatchJniReader.recordReaderNext(orcColumnarBatchJniReader.recordReader, orcColumnarBatchJniReader.batchReader, typeId, vecNativeId);
        assertTrue(rtn == 4096);
        LongVec vec1 = new LongVec(vecNativeId[0]);
        VarcharVec vec2 = new VarcharVec(vecNativeId[1]);
        assertTrue(11 == vec1.get(10));
        assertTrue(21 == vec1.get(20));
        String tmp1 = new String(vec2.get(10));
        String tmp2 = new String(vec2.get(20));
        assertTrue(tmp1.equals("AAAAAAAAKAAAAAAA"));
        assertTrue(tmp2.equals("AAAAAAAAEBAAAAAA"));
        vec1.close();
        vec2.close();
    }

}
