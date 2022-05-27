/**
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

package com.huawei.boostkit.spark.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class ZlibCodec implements CompressionCodec {

    private int level;
    private int strategy;

    private ZlibCodec() {
        level = Deflater.DEFAULT_COMPRESSION;
        strategy = Deflater.DEFAULT_STRATEGY;
    }

    public ZlibCodec(int level, int strategy) {
        this.level = level;
        this.strategy = strategy;
    }

    @Override
    public boolean compress(ByteBuffer in, ByteBuffer out,
                            ByteBuffer overflow) throws IOException {
        int length = in.remaining();
        int outSize = 0;
        Deflater deflater = new Deflater(level, true);
        try {
            deflater.setStrategy(strategy);
            deflater.setInput(in.array(), in.arrayOffset() + in.position(), length);
            deflater.finish();
            int offset = out.arrayOffset() + out.position();
            while (!deflater.finished() && (length > outSize)) {
                int size = deflater.deflate(out.array(), offset, out.remaining());
                out.position(size + out.position());
                outSize += size;
                offset += size;
                // if we run out of space in the out buffer, use the overflow
                if (out.remaining() == 0) {
                    if (overflow == null) {
                        return false;
                    }
                    out = overflow;
                    offset = out.arrayOffset() + out.position();
                }
            }
        }finally {
            deflater.end();
        }
        return length > outSize;
    }

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

    @Override
    public CompressionCodec modify(/* @Nullable */ EnumSet<Modifier> modifiers){

        if (modifiers == null){
            return this;
        }

        int l = this.level;
        int s = this.strategy;

        for (Modifier m : modifiers){
            switch (m){
                /* filtered == less LZ77, more huffman */
                case BINARY:
                    s = Deflater.FILTERED;
                    break;
                case TEXT:
                    s = Deflater.DEFAULT_STRATEGY;
                    break;
                case FASTEST:
                    // deflate_fast looking for 8 byte patterns
                    l = Deflater.BEST_SPEED;
                    break;
                case FAST:
                    // deflate_fast looking for 16 byte patterns
                    l = Deflater.BEST_SPEED + 1;
                    break;
                case DEFAULT:
                    // deflate_slow looking for 128 byte patterns
                    l = Deflater.DEFAULT_COMPRESSION;
                    break;
                default:
                    break;
            }
        }
        return new ZlibCodec(l,s);
    }

    @Override
    public void reset(){
        level = Deflater.DEFAULT_COMPRESSION;
        strategy = Deflater.DEFAULT_STRATEGY;
    }

    @Override
    public void close(){
    }
}
