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

import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor;
import junit.framework.TestCase;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.Reader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING )
public class OrcColumnarBatchJniReaderTest extends TestCase {
    public Configuration conf = new Configuration();
    public OrcColumnarBatchScanReader orcColumnarBatchScanReader;
    private int batchSize = 4096;

    private StructType requiredSchema;
    private int[] vecTypeIds;

    private long offset = 0;

    private long length = Integer.MAX_VALUE;

    @Before
    public void setUp() throws Exception {
        orcColumnarBatchScanReader = new OrcColumnarBatchScanReader();
        constructSchema();
        initReaderJava();
        initDataColIds();
        initRecordReaderJava();
        initBatch();
    }

    private void constructSchema() {
        requiredSchema = new StructType()
            .add("i_item_sk", LongType)
            .add("i_item_id", StringType);
    }

    private void initDataColIds() {
        // find requiredS fieldNames
        String[] requiredfieldNames = requiredSchema.fieldNames();
        // save valid cols and numbers of valid cols
        orcColumnarBatchScanReader.colsToGet = new int[requiredfieldNames.length];
        orcColumnarBatchScanReader.includedColumns = new ArrayList<>();
        // collect read cols types
        ArrayList<Integer> typeBuilder = new ArrayList<>();

        for (int i = 0; i < requiredfieldNames.length; i++) {
            String target = requiredfieldNames[i];

            // if not find, set colsToGet value -1, else set colsToGet 0
            boolean is_find = false;
            for (int j = 0; j < orcColumnarBatchScanReader.allFieldsNames.size(); j++) {
                if (target.equals(orcColumnarBatchScanReader.allFieldsNames.get(j))) {
                    orcColumnarBatchScanReader.colsToGet[i] = 0;
                    orcColumnarBatchScanReader.includedColumns.add(requiredfieldNames[i]);
                    typeBuilder.add(OmniExpressionAdaptor.sparkTypeToOmniType(requiredSchema.fields()[i].dataType()));
                    is_find = true;
                    break;
                }
            }

            if (!is_find) {
                orcColumnarBatchScanReader.colsToGet[i] = -1;
            }
        }

        vecTypeIds = typeBuilder.stream().mapToInt(Integer::intValue).toArray();
    }

    @After
    public void tearDown() throws Exception {
        System.out.println("orcColumnarBatchJniReader test finished");
    }

    private void initReaderJava() {
        File directory = new File("src/test/java/com/huawei/boostkit/spark/jni/orcsrc/000000_0");
        String path = directory.getAbsolutePath();
        URI uri = null;
        try {
            uri = new URI(path);
        } catch (URISyntaxException ignore) {
            // if URISyntaxException thrown, next line assertNotNull will interrupt the test
        }
        assertNotNull(uri);
        orcColumnarBatchScanReader.reader = orcColumnarBatchScanReader.initializeReaderJava(uri);
        assertTrue(orcColumnarBatchScanReader.reader != 0);
    }

    private void initRecordReaderJava() {
        orcColumnarBatchScanReader.recordReader = orcColumnarBatchScanReader.
            initializeRecordReaderJava(offset, length, null, requiredSchema);
        assertTrue(orcColumnarBatchScanReader.recordReader != 0);
    }

    private void initBatch() {
        orcColumnarBatchScanReader.initBatchJava(batchSize);
        assertTrue(orcColumnarBatchScanReader.batchReader != 0);
    }

    @Test
    public void testNext() {
        Vec[] vecs = new Vec[2];
        long rtn = orcColumnarBatchScanReader.next(vecs, vecTypeIds);
        assertTrue(rtn == 4096);
        assertTrue(((LongVec) vecs[0]).get(0) == 1);
        String str = new String(((VarcharVec) vecs[1]).get(0));
        assertTrue(str.equals("AAAAAAAABAAAAAAA"));
        vecs[0].close();
        vecs[1].close();
    }

    @Test
    public void testGetProgress() {
        String tmp = "";
        try {
            double progressValue = orcColumnarBatchScanReader.getProgress();
        } catch (Exception e) {
            tmp = e.getMessage();
        } finally {
            assertTrue(tmp.equals("recordReaderGetProgress is unsupported"));
        }
    }
}
