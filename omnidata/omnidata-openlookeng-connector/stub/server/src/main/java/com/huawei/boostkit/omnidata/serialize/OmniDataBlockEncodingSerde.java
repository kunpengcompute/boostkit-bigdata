/*
 * Copyright (C) 2022-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.omnidata.serialize;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.block.ArrayBlockEncoding;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncoding;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.ByteArrayBlockEncoding;
import io.prestosql.spi.block.DictionaryBlockEncoding;
import io.prestosql.spi.block.Int128ArrayBlockEncoding;
import io.prestosql.spi.block.IntArrayBlockEncoding;
import io.prestosql.spi.block.LazyBlockEncoding;
import io.prestosql.spi.block.LongArrayBlockEncoding;
import io.prestosql.spi.block.RowBlockEncoding;
import io.prestosql.spi.block.RunLengthBlockEncoding;
import io.prestosql.spi.block.ShortArrayBlockEncoding;
import io.prestosql.spi.block.SingleRowBlockEncoding;
import io.prestosql.spi.block.VariableWidthBlockEncoding;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

public class OmniDataBlockEncodingSerde implements BlockEncodingSerde {
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

    private static String readLengthPrefixedString(SliceInput sliceInput)
    {
        int length = sliceInput.readInt();
        byte[] bytes = new byte[length];
        sliceInput.readBytes(bytes);

        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void writeLengthPrefixedString(SliceOutput sliceOutput, String value)
    {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        sliceOutput.writeInt(bytes.length);
        sliceOutput.writeBytes(bytes);
    }

    @Override
    public Block<?> readBlock(SliceInput input)
    {
        return blockEncodings.get(readLengthPrefixedString(input)).readBlock(this, input);
    }

    @Override
    public void writeBlock(SliceOutput output, Block block)
    {
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
