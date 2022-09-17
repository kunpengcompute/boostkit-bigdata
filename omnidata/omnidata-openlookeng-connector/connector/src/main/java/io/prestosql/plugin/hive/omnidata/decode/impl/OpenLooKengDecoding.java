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

package io.prestosql.plugin.hive.omnidata.decode.impl;

import com.huawei.boostkit.omnidata.decode.AbstractDecoding;
import com.huawei.boostkit.omnidata.decode.type.ArrayDecodeType;
import com.huawei.boostkit.omnidata.decode.type.DecodeType;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.ByteArrayBlock;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.DictionaryId;
import io.prestosql.spi.block.Int128ArrayBlock;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.block.ShortArrayBlock;
import io.prestosql.spi.block.VariableWidthBlock;

import java.util.Optional;
import java.util.stream.IntStream;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.prestosql.spi.block.ArrayBlock.fromElementBlock;
import static io.prestosql.spi.block.RowBlock.fromFieldBlocks;

/**
 * Decode data to block
 *
 * @since 2022-07-18
 */
public class OpenLooKengDecoding
        extends AbstractDecoding<Block<?>>
{
    @Override
    public Block<?> decodeArray(Optional<DecodeType> type, SliceInput sliceInput)
    {
        Optional<DecodeType> elementType = Optional.empty();
        if (type.isPresent()) {
            if (type.get() instanceof ArrayDecodeType) {
                ArrayDecodeType<?> arrayDecodeType = (ArrayDecodeType) type.get();
                elementType = Optional.of((arrayDecodeType).getElementType());
            }
        }
        Block<?> values = decode(elementType, sliceInput);
        int positionCount = sliceInput.readInt();
        int[] offsets = new int[positionCount + 1];
        sliceInput.readBytes(Slices.wrappedIntArray(offsets));
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElseGet(() -> new boolean[positionCount]);

        return fromElementBlock(positionCount, Optional.ofNullable(valueIsNull), offsets, values);
    }

    @Override
    public Block<?> decodeByteArray(Optional<DecodeType> type, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        byte[] values = new byte[positionCount];

        IntStream.range(0, positionCount)
                .forEach(
                        position -> {
                            if (valueIsNull == null || !valueIsNull[position]) {
                                values[position] = sliceInput.readByte();
                            }
                        });

        return new ByteArrayBlock(positionCount, Optional.ofNullable(valueIsNull), values);
    }

    @Override
    public Block<?> decodeBooleanArray(Optional<DecodeType> type, SliceInput sliceInput)
    {
        return decodeByteArray(type, sliceInput);
    }

    @Override
    public Block<?> decodeIntArray(Optional<DecodeType> type, SliceInput sliceInput)
    {
        int posCount = sliceInput.readInt();
        boolean[] valueIsNull = decodeNullBits(sliceInput, posCount).orElse(null);
        int[] values = new int[posCount];

        for (int position = 0; position < posCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                values[position] = sliceInput.readInt();
            }
        }

        return new IntArrayBlock(posCount, Optional.ofNullable(valueIsNull), values);
    }

    @Override
    public Block<?> decodeInt128Array(Optional<DecodeType> type, SliceInput sliceInput)
    {
        int posCount = sliceInput.readInt();
        boolean[] valueIsNull = decodeNullBits(sliceInput, posCount).orElse(null);
        long[] values = new long[posCount * 2];

        for (int position = 0; position < posCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                values[position * 2] = sliceInput.readLong();
                values[(position * 2) + 1] = sliceInput.readLong();
            }
        }

        return new Int128ArrayBlock(posCount, Optional.ofNullable(valueIsNull), values);
    }

    @Override
    public Block<?> decodeShortArray(Optional<DecodeType> type, SliceInput sliceInput)
    {
        int posCount = sliceInput.readInt();
        boolean[] valueIsNull = decodeNullBits(sliceInput, posCount).orElse(null);
        short[] values = new short[posCount];

        for (int position = 0; position < posCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                values[position] = sliceInput.readShort();
            }
        }

        return new ShortArrayBlock(posCount, Optional.ofNullable(valueIsNull), values);
    }

    @Override
    public Block<?> decodeLongArray(Optional<DecodeType> type, SliceInput sliceInput)
    {
        int posCount = sliceInput.readInt();
        boolean[] valueIsNull = decodeNullBits(sliceInput, posCount).orElse(null);
        long[] values = new long[posCount];

        for (int position = 0; position < posCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                values[position] = sliceInput.readLong();
            }
        }

        return new LongArrayBlock(posCount, Optional.ofNullable(valueIsNull), values);
    }

    @Override
    public Block<?> decodeFloatArray(Optional<DecodeType> type, SliceInput sliceInput)
    {
        return decodeLongArray(type, sliceInput);
    }

    @Override
    public Block<?> decodeDoubleArray(Optional<DecodeType> type, SliceInput sliceInput)
    {
        return decodeLongArray(type, sliceInput);
    }

    @Override
    public Block<?> decodeMap(Optional<DecodeType> type, SliceInput sliceInput)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block<?> decodeSingleMap(Optional<DecodeType> type, SliceInput sliceInput)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block<?> decodeVariableWidth(Optional<DecodeType> type, SliceInput sliceInput)
    {
        int posCount = sliceInput.readInt();
        int[] offsets = new int[posCount + 1];

        sliceInput.readBytes(Slices.wrappedIntArray(offsets), SIZE_OF_INT, posCount * SIZE_OF_INT);

        boolean[] valueIsNull = decodeNullBits(sliceInput, posCount).orElse(null);

        int blockSize = sliceInput.readInt();
        Slice slice = sliceInput.readSlice(blockSize);

        return new VariableWidthBlock(posCount, slice, offsets, Optional.ofNullable(valueIsNull));
    }

    @Override
    public Block<?> decodeDictionary(Optional<DecodeType> type, SliceInput sliceInput)
    {
        int posCount = sliceInput.readInt();

        Block<?> dictionaryBlock = decode(type, sliceInput);

        int[] ids = new int[posCount];
        sliceInput.readBytes(Slices.wrappedIntArray(ids));

        long mostSignificantBits = sliceInput.readLong();
        long leastSignificantBits = sliceInput.readLong();
        long sequenceId = sliceInput.readLong();

        // We always compact the dictionary before we send it. However, dictionaryBlock comes from sliceInput, which may
        // over-retain memory.
        // As a result, setting dictionaryIsCompacted to true is not appropriate here.
        // over-retains memory.
        return new DictionaryBlock<>(
                posCount,
                dictionaryBlock,
                ids,
                false,
                new DictionaryId(mostSignificantBits, leastSignificantBits, sequenceId));
    }

    @Override
    public Block<?> decodeRunLength(Optional<DecodeType> type, SliceInput sliceInput)
    {
        int posCount = sliceInput.readInt();

        Block<?> values = decode(type, sliceInput);

        return new RunLengthEncodedBlock<>(values, posCount);
    }

    @Override
    public Block<?> decodeRow(Optional<DecodeType> type, SliceInput sliceInput)
    {
        int numFields = sliceInput.readInt();
        Block[] fieldBlocks = new Block[numFields];
        for (int i = 0; i < numFields; i++) {
            fieldBlocks[i] = decode(type, sliceInput);
        }

        int positionCount = sliceInput.readInt();
        int[] fieldBlockOffsets = new int[positionCount + 1];
        sliceInput.readBytes(Slices.wrappedIntArray(fieldBlockOffsets));
        boolean[] rowIsNull = decodeNullBits(sliceInput, positionCount).orElseGet(() -> new boolean[positionCount]);

        return fromFieldBlocks(positionCount, Optional.of(rowIsNull), fieldBlocks);
    }

    @Override
    public Block<?> decodeDate(Optional<DecodeType> type, SliceInput sliceInput)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block<?> decodeLongToInt(Optional<DecodeType> type, SliceInput sliceInput)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block<?> decodeLongToShort(Optional<DecodeType> type, SliceInput sliceInput)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block<?> decodeLongToByte(Optional<DecodeType> type, SliceInput sliceInput)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block<?> decodeLongToFloat(Optional<DecodeType> type, SliceInput sliceInput)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block decodeDecimal(Optional<DecodeType> type, SliceInput sliceInput, String decodeType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block decodeTimestamp(Optional<DecodeType> type, SliceInput sliceInput)
    {
        throw new UnsupportedOperationException();
    }
}
