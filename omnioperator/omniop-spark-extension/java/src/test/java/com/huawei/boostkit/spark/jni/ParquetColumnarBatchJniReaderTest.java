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
import nova.hetu.omniruntime.vector.Vec;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.*;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class ParquetColumnarBatchJniReaderTest extends TestCase {
    private ParquetColumnarBatchScanReader parquetColumnarBatchScanReader;

    private Vec[] vecs;

    private List<DataType> types;

    @Before
    public void setUp() throws Exception {
        parquetColumnarBatchScanReader = new ParquetColumnarBatchScanReader();

        List<Integer> rowGroupIndices = new ArrayList<>();
        rowGroupIndices.add(0);
        List<Integer> columnIndices = new ArrayList<>();
        Collections.addAll(columnIndices, 0, 1, 3, 6, 7, 8, 9, 10, 12);
        types = new ArrayList<>();
        Collections.addAll(types, IntegerType, StringType, LongType, DoubleType, createDecimalType(9, 8),
                createDecimalType(18, 5), BooleanType, ShortType, DateType);
        File file = new File("../../omniop-native-reader/cpp/test/tablescan/resources/parquet_data_all_type");
        String path = file.getAbsolutePath();
        path = "file:" + path;
        System.out.println(path);
        parquetColumnarBatchScanReader.initializeReaderJava(new Path(path), 100000, rowGroupIndices, columnIndices, "root@sample");
        vecs = new Vec[9];
    }

    @After
    public void tearDown() throws Exception {
        parquetColumnarBatchScanReader.close();
        for (Vec vec : vecs) {
            vec.close();
        }
    }

    @Test
    public void testRead() {
        long num = parquetColumnarBatchScanReader.next(vecs, types);
        assertTrue(num == 1);
    }
}
