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
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import scala.Option;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.types.DataTypes.*;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class ParquetColumnarBatchJniReaderTest extends TestCase {
    private ParquetColumnarBatchScanReader parquetColumnarBatchScanReader;

    private Vec[] vecs;

    private boolean[] missingColumns;
    private StructType schema;
    private List<DataType> types;

    @Before
    public void setUp() throws Exception {
        constructSchema();
        parquetColumnarBatchScanReader = new ParquetColumnarBatchScanReader(schema,
                new RebaseDateTime.RebaseSpec(null, Option.empty()), new RebaseDateTime.RebaseSpec(null, Option.empty()),
                Arrays.stream(schema.fields()).map(field -> (Type) null).collect(Collectors.toList()));

        File file = new File("src/test/java/com/huawei/boostkit/spark/jni/parquetsrc/parquet_data_all_type");
        String path = file.getAbsolutePath();
        parquetColumnarBatchScanReader.initializeReaderJava(new Path(path), 4096);
        parquetColumnarBatchScanReader.initializeRecordReaderJava(0, 100000, schema.fieldNames(), null);
        missingColumns = new boolean[schema.fieldNames().length];
        Arrays.fill(missingColumns, false);
        vecs = new Vec[schema.fieldNames().length];
    }

    private void constructSchema() {
        schema = new StructType()
            .add("c1", IntegerType)
            .add("c2", StringType)
            .add("c4", LongType)
            .add("c7", DoubleType)
            .add("c8", createDecimalType(9, 8))
            .add("c9", createDecimalType(18, 5))
            .add("c10", BooleanType)
            .add("c11", ShortType)
            .add("c13", DateType);

        types = new ArrayList<>();
        for (StructField f: schema.fields()) {
            types.add(f.dataType());
        }
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
        long num = parquetColumnarBatchScanReader.next(vecs, missingColumns, types);
        assertTrue(num == 1);
    }
}
