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
import java.io.InputStream;

public class DecompressionStream extends InputStream {
    public static final int HEADER_SIZE = 3;
    public static final int UNCOMPRESSED_LENGTH = 64 * 1024;

    protected int compressBlockSize = 64 * 1024;
    private boolean finishedReading = false;
    protected final InputStream in;
    private byte[] compressed;      // 临时原始压缩数据
    private byte[] uncompressed;    // 解压后数据
    private int uncompressedCursor = 0;     // 解压后数组游标
    private int uncompressedLimit = 0;      // 解压后数组最大标

    private final CompressionCodec codec;

    public DecompressionStream(InputStream in, CompressionCodec codec, int compressBlockSize) throws IOException {
        this.compressBlockSize = compressBlockSize;
        this.in = in;
        this.codec = codec;
        this.readHeader();
    }


    public void close() throws IOException {
        this.compressed = null;
        this.uncompressed = null;
        if (this.in != null) {
            this.in.close();
        }
    }

    protected void readHeader() throws IOException {
        int[] b = new int[3];
        for (int i = 0; i < HEADER_SIZE; i++) {
            int ret = in.read();
            if (ret == -1) {
                finishedReading = true;
                return;
            }
            b[i] = ret & 0xff;
        }
        boolean isOriginal = (b[0] & 0x01) == 1;
        int chunkLength = (b[2] << 15) | (b[1] << 7) | (b[0] >> 1);

        uncompressedCursor = 0;
        uncompressedLimit = 0;
        // read the entire input data to the buffer
        compressed = new byte[chunkLength]; // 8K
        int readBytes = 0;
        while (readBytes < chunkLength) {
            int ret = in.read(compressed, readBytes, chunkLength - readBytes);
            if (ret == -1) {
                finishedReading = true;
                break;
            }
            readBytes += ret;
        }
        if (readBytes < chunkLength) {
            throw new IOException("failed to read chunk!");
        }
        if (isOriginal) {
            uncompressed = compressed;
            uncompressedLimit = chunkLength;
            return;
        }
        if (uncompressed == null || UNCOMPRESSED_LENGTH > uncompressed.length) {
            uncompressed = new byte[UNCOMPRESSED_LENGTH];
        }

        int actualUncompressedLength = codec.decompress(compressed, chunkLength, uncompressed);
        uncompressedLimit = actualUncompressedLength;
    }

    public int read(byte[] data, int offset, int length) throws IOException {
        if (!ensureUncompressed()) {
            return -1;
        }
        int actualLength = Math.min(length, uncompressedLimit - uncompressedCursor);
        System.arraycopy(uncompressed, uncompressedCursor, data, offset, actualLength);
        uncompressedCursor += actualLength;
        return actualLength;
    }

    public int read() throws IOException {
        if (!ensureUncompressed()) {
            return -1;
        }
        int data = 0xff & uncompressed[uncompressedCursor];
        uncompressedCursor += 1;
        return data;
    }

    private boolean ensureUncompressed() throws IOException {
        while (uncompressed == null || (uncompressedLimit - uncompressedCursor) == 0) {
            if (finishedReading) {
                return false;
            }
            readHeader();
        }
        return true;
    }

    public int available() throws IOException {
        if (!ensureUncompressed()) {
            return 0;
        }
        return uncompressedLimit - uncompressedCursor;
    }
}
