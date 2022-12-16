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

import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class ZlibCodec implements CompressionCodec {

    public ZlibCodec() {}

    @Override
    public int decompress(byte[] input, int inputLength, byte[] output) throws IOException {
        Inflater inflater = new Inflater(true);
        int offset = 0;
        int length = output.length;
        try {
            inflater.setInput(input, 0, inputLength);
            while (!(inflater.finished() || inflater.needsDictionary() ||
                    inflater.needsInput())) {
                try {
                    int count = inflater.inflate(output, offset, length - offset);
                    offset += count;
                } catch (DataFormatException dfe) {
                    throw new IOException("Bad compression data", dfe);
                }
            }
        } finally {
            inflater.end();
        }
        return offset;
    }
}
