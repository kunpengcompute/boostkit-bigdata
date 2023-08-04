/*
 * Copyright (C) 2022-2023. Huawei Technologies Co., Ltd. All rights reserved.
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
import nova.hetu.omniruntime.vector.*;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class ParquetColumnarBatchJniReaderTest extends TestCase {
    private ParquetColumnarBatchJniReader parquetColumnarBatchJniReader;

    private Vec[] vecs;

    @Before
    public void setUp() throws Exception {
        parquetColumnarBatchJniReader = new ParquetColumnarBatchJniReader();

        List<Integer> rowGroupIndices = new ArrayList<>();
        rowGroupIndices.add(0);
        List<Integer> columnIndices = new ArrayList<>();
        Collections.addAll(columnIndices, 0, 1, 3, 6, 7, 8, 9, 10, 12);
        File file = new File("../cpp/test/tablescan/resources/parquet_data_all_type");
        String path = file.getAbsolutePath();
        parquetColumnarBatchJniReader.initializeReaderJava(path, 100000, rowGroupIndices, columnIndices, "root@sample");
        vecs = new Vec[9];
    }

    @After
    public void tearDown() throws Exception {
        parquetColumnarBatchJniReader.close();
        for (Vec vec : vecs) {
            vec.close();
        }
    }

    @Test
    public void testRead() {
        long num = parquetColumnarBatchJniReader.next(vecs);
        assertTrue(num == 1);
    }
}
