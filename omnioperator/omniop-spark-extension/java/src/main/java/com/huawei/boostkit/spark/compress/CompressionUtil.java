/*
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

package com.huawei.boostkit.spark.compress;

import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.lzo.LzoDecompressor;

public class CompressionUtil {
    public static CompressionCodec createCodec(String compressionCodec) {
        switch (compressionCodec) {
            case "zlib":
                return new ZlibCodec();
            case "snappy":
                return new SnappyCodec();
            case "lzo":
                return new AircompressorCodec(new LzoDecompressor());
            case "lz4":
                return new AircompressorCodec(new Lz4Decompressor());
            default:
                throw new IllegalArgumentException("Unknown compression codec: " +
                        compressionCodec);
        }
    }
}
