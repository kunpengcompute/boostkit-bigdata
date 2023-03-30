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

package com.huawei.boostkit.omnidata.serialize;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.ImmutableMap;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.block.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

/**
 * Block Encoding Serde
 *
 * @since 2021-07-31
 */
public final class OmniDataBlockEncodingSerde implements BlockEncodingSerde {
    private final Map<String, BlockEncoding> blockEncodings;

    public OmniDataBlockEncodingSerde() {
        blockEncodings =
                ImmutableMap.<String, BlockEncoding>builder()
                        .put(VariableWidthBlockEncoding.NAME, new VariableWidthBlockEncoding())
                        .put(ByteArrayBlockEncoding.NAME, new ByteArrayBlockEncoding())
                        .put(ShortArrayBlockEncoding.NAME, new ShortArrayBlockEncoding())
                        .put(IntArrayBlockEncoding.NAME, new IntArrayBlockEncoding())
                        .put(LongArrayBlockEncoding.NAME, new LongArrayBlockEncoding())
                        .put(Int128ArrayBlockEncoding.NAME, new Int128ArrayBlockEncoding())
                        .put(DictionaryBlockEncoding.NAME, new DictionaryBlockEncoding())
                        .put(ArrayBlockEncoding.NAME, new ArrayBlockEncoding())
                        .put(RowBlockEncoding.NAME, new RowBlockEncoding())
                        .put(SingleRowBlockEncoding.NAME, new SingleRowBlockEncoding())
                        .put(RunLengthBlockEncoding.NAME, new RunLengthBlockEncoding())
                        .put(LazyBlockEncoding.NAME, new LazyBlockEncoding())
                        .build();
    }

    private static String readLengthPrefixedString(SliceInput sliceInput) {
        int length = sliceInput.readInt();
        byte[] bytes = new byte[length];
        sliceInput.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void writeLengthPrefixedString(SliceOutput sliceOutput, String value) {
        byte[] bytes = value.getBytes(UTF_8);
        sliceOutput.writeInt(bytes.length);
        sliceOutput.writeBytes(bytes);
    }

    @Override
    public Block<?> readBlock(SliceInput input) {
        return blockEncodings.get(readLengthPrefixedString(input)).readBlock(this, input);
    }

    @Override
    public void writeBlock(SliceOutput output, Block block) {
        Block<?> readBlock = block;
        while (true) {
            String encodingName = readBlock.getEncodingName();

            BlockEncoding blockEncoding = blockEncodings.get(encodingName);

            Optional<Block> replacementBlock = blockEncoding.replacementBlockForWrite(readBlock);
            if (replacementBlock.isPresent()) {
                readBlock = replacementBlock.get();
                continue;
            }

            writeLengthPrefixedString(output, encodingName);

            blockEncoding.writeBlock(this, output, readBlock);

            break;
        }
    }
}