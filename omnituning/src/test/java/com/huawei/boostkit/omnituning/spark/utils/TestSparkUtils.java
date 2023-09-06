/*
 * Copyright (C) 2020-2023. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.omnituning.spark.utils;

import org.apache.spark.SparkConf;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSparkUtils {
    @Test
    public void testGetPropertiesFromFile() {
        String filePath = Thread.currentThread().getContextClassLoader().getResource("test-spark.conf").getPath();
        SparkUtils.getPropertiesFromFile(filePath);
    }

    @Test
    public void getApplicationIdFromFile() {
        String fileName = "app_id.ztsd";
        assertEquals(SparkUtils.getApplicationIdFromFile(fileName), "app_id");
    }

    @Test
    public void testLoadCompressionCodec() {
        SparkConf conf = new SparkConf();
        assertTrue(SparkUtils.compressionCodecForLogName(conf, "app_id.zstd").isDefined());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnKnownLoadCompressionCodec() {
        SparkConf conf = new SparkConf();
        SparkUtils.compressionCodecForLogName(conf, "app_id.unknown");
    }
}
