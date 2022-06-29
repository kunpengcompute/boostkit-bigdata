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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import junit.framework.TestCase;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcInputFormat;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.apache.hadoop.conf.Configuration;
import java.io.File;
import java.util.ArrayList;
import org.apache.orc.Reader.Options;

import static org.junit.Assert.*;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING )
public class OrcColumnarBatchJniReaderTest extends TestCase {
    public Configuration conf = new Configuration();
    public OrcColumnarBatchJniReader orcColumnarBatchJniReader;
    public int batchSize = 4096;

    @Before
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        TypeDescription schema =
                TypeDescription.fromString("struct<`i_item_sk`:bigint,`i_item_id`:string>");
        Options options = new Options(conf)
                .range(0, Integer.MAX_VALUE)
                .useZeroCopy(false)
                .skipCorruptRecords(false)
                .tolerateMissingSchema(true);

        options.schema(schema);
        options.include(OrcInputFormat.parseInclude(schema,
                null));
        String kryoSarg = "AQEAb3JnLmFwYWNoZS5oYWRvb3AuaGl2ZS5xbC5pby5zYXJnLkV4cHJlc3Npb25UcmXlAQEBamF2YS51dGlsLkFycmF5TGlz9AECAQABAQEBAQEAAQAAAAEEAAEBAwEAAQEBAQEBAAEAAAIIAAEJAAEBAgEBAQIBAscBb3JnLmFwYWNoZS5oYWRvb3AuaGl2ZS5xbC5pby5zYXJnLlNlYXJjaEFyZ3VtZW50SW1wbCRQcmVkaWNhdGVMZWFmSW1wbAEBaV9pdGVtX3PrAAABBwEBAQIBEAkAAAEEEg==";
        String sargColumns = "i_item_sk,i_item_id,i_rec_start_date,i_rec_end_date,i_item_desc,i_current_price,i_wholesale_cost,i_brand_id,i_brand,i_class_id,i_class,i_category_id,i_category,i_manufact_id,i_manufact,i_size,i_formulation,i_color,i_units,i_container,i_manager_id,i_product_name";
        if (kryoSarg != null && sargColumns != null) {
            byte[] sargBytes = Base64.decodeBase64(kryoSarg);
            SearchArgument sarg =
                    new Kryo().readObject(new Input(sargBytes), SearchArgumentImpl.class);
            options.searchArgument(sarg, sargColumns.split(","));
            sarg.getExpression().toString();
        }

        orcColumnarBatchJniReader = new OrcColumnarBatchJniReader();
        initReaderJava();
        initRecordReaderJava(options);
        initBatch(options);
    }

    @After
    public void tearDown() throws Exception {
        System.out.println("orcColumnarBatchJniReader test finished");
    }

    public void initReaderJava() {
        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf);
        File directory = new File("src/test/java/com/huawei/boostkit/spark/jni/orcsrc/000000_0");
        String path = directory.getAbsolutePath();
        orcColumnarBatchJniReader.reader = orcColumnarBatchJniReader.initializeReaderJava(path, readerOptions);
        assertTrue(orcColumnarBatchJniReader.reader != 0);
    }

    public void initRecordReaderJava(Options options) {
        orcColumnarBatchJniReader.recordReader = orcColumnarBatchJniReader.initializeRecordReaderJava(options);
        assertTrue(orcColumnarBatchJniReader.recordReader != 0);
    }

    public void initBatch(Options options) {
        orcColumnarBatchJniReader.initBatchJava(batchSize);
        assertTrue(orcColumnarBatchJniReader.batchReader != 0);
    }

    @Test
    public void testNext() {
        Vec[] vecs = new Vec[2];
        long rtn = orcColumnarBatchJniReader.next(vecs);
        assertTrue(rtn == 4096);
        assertTrue(((LongVec) vecs[0]).get(0) == 1);
        String str = new String(((VarcharVec) vecs[1]).get(0));
        assertTrue(str.equals("AAAAAAAABAAAAAAA"));
    }

}
