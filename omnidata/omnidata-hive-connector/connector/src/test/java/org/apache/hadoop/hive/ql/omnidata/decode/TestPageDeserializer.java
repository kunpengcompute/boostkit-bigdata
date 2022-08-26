/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.omnidata.decode;

import io.hetu.core.transport.execution.buffer.SerializedPage;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.omnidata.decode.type.DecodeType;
import org.apache.hadoop.hive.ql.omnidata.decode.type.LongDecodeType;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * TestPageDeserializer
 *
 * @since 2022-08-24
 */
public class TestPageDeserializer {
    @Test
    public void testDeserialize() {
        DecodeType[] columnTypes = new DecodeType[] {new LongDecodeType(), new LongDecodeType()};
        PageDeserializer pageDeserializer = new PageDeserializer(columnTypes);
        byte[] sliceArray = {
                1, 0, 0, 0, 10, 0, 0, 0, 76, 79, 78, 71, 95, 65, 82, 82, 65, 89, 1, 0, 0, 0, 0, -100, 113, 11, 0, 0, 0, 0, 0
        };
        byte pageCodecMarkers = new Byte("0");
        int positionCount = 1;
        int uncompressedSizeInBytes = 31;
        SerializedPage page = new SerializedPage(sliceArray, pageCodecMarkers, positionCount, uncompressedSizeInBytes);
        List<ColumnVector[]> columnVectors = pageDeserializer.deserialize(page);

        ColumnVector[] testColumnVectors = new ColumnVector[1];
        LongColumnVector testColumnVector = new LongColumnVector(1);
        testColumnVector.isNull[0] = false;
        testColumnVector.vector[0] = 749980;
        testColumnVectors[0] = testColumnVector;
        if (columnVectors.get(0)[0] instanceof LongColumnVector) {
            Assert.assertEquals(((LongColumnVector) columnVectors.get(0)[0]).vector[0],
                    ((LongColumnVector) testColumnVectors[0]).vector[0]);
        }

        // compress page
        try {
            byte compressPageCodecMarkers = new Byte("1");
            int compressedSizeInBytes = 100;
            SerializedPage compressPage = new SerializedPage(sliceArray, compressPageCodecMarkers, positionCount,
                    compressedSizeInBytes);
            pageDeserializer.deserialize(compressPage);
        } catch (UnsupportedOperationException e) {
            Assert.assertEquals(e.getClass(), UnsupportedOperationException.class);
        }
    }
}