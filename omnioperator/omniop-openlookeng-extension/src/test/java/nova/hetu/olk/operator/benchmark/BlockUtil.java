/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package nova.hetu.olk.operator.benchmark;

import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.VarcharType;

import java.math.BigInteger;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.encodeUnscaledValue;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;

public final class BlockUtil
{
    private BlockUtil()
    {
    }

    public static Block createStringSequenceBlock(int start, int end, VarcharType type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, 100);

        for (int i = start; i < end; i++) {
            type.writeString(builder, String.valueOf(i));
        }

        return builder.build();
    }

    public static Block createStringSequenceBlock(int start, int end, CharType type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, 100);

        for (int i = start; i < end; i++) {
            type.writeString(builder, String.valueOf(i));
        }

        return builder.build();
    }

    public static Block createIntegerSequenceBlock(int start, int end)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            INTEGER.writeLong(builder, i);
        }

        return builder.build();
    }

    public static Block createStringDictionaryBlock(int start, int length, VarcharType type)
    {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = type.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            type.writeString(builder, String.valueOf(i));
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createStringDictionaryBlock(int start, int length, CharType type)
    {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = type.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            type.writeString(builder, String.valueOf(i));
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createLongDictionaryBlock(int start, int length)
    {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = BIGINT.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            BIGINT.writeLong(builder, i);
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createIntegerDictionaryBlock(int start, int length)
    {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = INTEGER.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            INTEGER.writeLong(builder, i);
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createRealDictionaryBlock(int start, int length)
    {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = REAL.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            REAL.writeLong(builder, floatToRawIntBits((float) i));
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createDoubleDictionaryBlock(int start, int length)
    {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = DOUBLE.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            DOUBLE.writeDouble(builder, i);
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createBooleanDictionaryBlock(int start, int length)
    {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = BOOLEAN.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            BOOLEAN.writeBoolean(builder, i % 2 == 0);
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createDateDictionaryBlock(int start, int length)
    {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = DATE.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            DATE.writeLong(builder, i);
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createTimestampDictionaryBlock(int start, int length)
    {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = TIMESTAMP.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            TIMESTAMP.writeLong(builder, i);
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createShortDecimalDictionaryBlock(int start, int length, DecimalType type)
    {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        long base = BigInteger.TEN.pow(type.getScale()).longValue();

        BlockBuilder builder = type.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            type.writeLong(builder, base * i);
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createLongDecimalDictionaryBlock(int start, int length, DecimalType type)
    {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BigInteger base = BigInteger.TEN.pow(type.getScale());

        BlockBuilder builder = type.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            type.writeSlice(builder, encodeUnscaledValue(BigInteger.valueOf(i).multiply(base)));
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createIntegerBlock(List<Integer> values)
    {
        int positionCount = values.size();
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(positionCount);
        for (int i = 0; i < positionCount; i++) {
            INTEGER.writeLong(builder, values.get(i));
        }

        return builder.build();
    }

    public static Block createLongBlock(List<Integer> values)
    {
        int positionCount = values.size();
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(positionCount);
        for (int i = 0; i < positionCount; i++) {
            BIGINT.writeLong(builder, values.get(i));
        }

        return builder.build();
    }

    public static Block createRealBlock(List<Integer> values)
    {
        int positionCount = values.size();
        BlockBuilder builder = REAL.createFixedSizeBlockBuilder(positionCount);
        for (int i = 0; i < positionCount; i++) {
            REAL.writeLong(builder, floatToRawIntBits((float) values.get(i)));
        }

        return builder.build();
    }

    public static Block createDoubleBlock(List<Integer> values)
    {
        int positionCount = values.size();
        BlockBuilder builder = DOUBLE.createFixedSizeBlockBuilder(positionCount);
        for (int i = 0; i < positionCount; i++) {
            DOUBLE.writeDouble(builder, (double) values.get(i));
        }

        return builder.build();
    }

    public static Block createStringBlock(String prefix, List<Integer> values, VarcharType type)
    {
        int positionCount = values.size();
        BlockBuilder builder = type.createBlockBuilder(null, positionCount);
        for (int i = 0; i < positionCount; i++) {
            type.writeString(builder, prefix + values.get(i));
        }

        return builder.build();
    }

    public static Block createBooleanBlock(List<Integer> values)
    {
        int positionCount = values.size();
        BlockBuilder builder = BOOLEAN.createFixedSizeBlockBuilder(positionCount);
        for (int i = 0; i < positionCount; i++) {
            BOOLEAN.writeBoolean(builder, values.get(i) == 0);
        }

        return builder.build();
    }

    public static Block createDateBlock(List<Integer> values)
    {
        int positionCount = values.size();
        BlockBuilder builder = DATE.createFixedSizeBlockBuilder(positionCount);
        for (int i = 0; i < positionCount; i++) {
            DATE.writeLong(builder, values.get(i));
        }

        return builder.build();
    }

    public static Block createTimestampBlock(List<Integer> values)
    {
        int positionCount = values.size();
        BlockBuilder builder = TIMESTAMP.createFixedSizeBlockBuilder(positionCount);
        for (int i = 0; i < positionCount; i++) {
            TIMESTAMP.writeLong(builder, values.get(i));
        }

        return builder.build();
    }

    public static Block createShortDecimalBlock(List<Integer> values, DecimalType type)
    {
        int positionCount = values.size();
        long base = BigInteger.TEN.pow(type.getScale()).longValue();
        BlockBuilder builder = type.createFixedSizeBlockBuilder(positionCount);
        for (int i = 0; i < positionCount; i++) {
            type.writeLong(builder, base * values.get(i));
        }

        return builder.build();
    }

    public static Block createLongDecimalBlock(List<Integer> values, DecimalType type)
    {
        int positionCount = values.size();
        BigInteger base = BigInteger.TEN.pow(type.getScale());
        BlockBuilder builder = type.createFixedSizeBlockBuilder(positionCount);
        for (int i = 0; i < positionCount; i++) {
            type.writeSlice(builder, encodeUnscaledValue(BigInteger.valueOf(values.get(i)).multiply(base)));
        }

        return builder.build();
    }

    private static Block createDictionaryBlock(Block block)
    {
        int dictionarySize = block.getPositionCount();
        int[] ids = new int[dictionarySize];
        for (int i = 0; i < dictionarySize; i++) {
            ids[i] = i;
        }
        return new DictionaryBlock(block, ids);
    }

    public static Block createIntegerDictionaryBlock(List<Integer> values)
    {
        Block block = createIntegerBlock(values);
        return createDictionaryBlock(block);
    }

    public static Block createLongDictionaryBlock(List<Integer> values)
    {
        Block block = createLongBlock(values);
        return createDictionaryBlock(block);
    }

    public static Block createRealDictionaryBlock(List<Integer> values)
    {
        Block block = createRealBlock(values);
        return createDictionaryBlock(block);
    }

    public static Block createDoubleDictionaryBlock(List<Integer> values)
    {
        Block block = createDoubleBlock(values);
        return createDictionaryBlock(block);
    }

    public static Block createStringDictionaryBlock(String prefix, List<Integer> values, VarcharType type)
    {
        Block block = createStringBlock(prefix, values, type);
        return createDictionaryBlock(block);
    }

    public static Block createBooleanDictionaryBlock(List<Integer> values)
    {
        Block block = createBooleanBlock(values);
        return createDictionaryBlock(block);
    }

    public static Block createDateDictionaryBlock(List<Integer> values)
    {
        Block block = createDateBlock(values);
        return createDictionaryBlock(block);
    }

    public static Block createTimestampDictionaryBlock(List<Integer> values)
    {
        Block block = createTimestampBlock(values);
        return createDictionaryBlock(block);
    }

    public static Block createShortDecimalDictionaryBlock(List<Integer> values, DecimalType type)
    {
        Block block = createShortDecimalBlock(values, type);
        return createDictionaryBlock(block);
    }

    public static Block createLongDecimalDictionaryBlock(List<Integer> values, DecimalType type)
    {
        Block block = createLongDecimalBlock(values, type);
        return createDictionaryBlock(block);
    }

    public static Block buildVarcharBlock(int rowSize, int width, int offset)
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, rowSize);
        for (int i = 0; i < rowSize; i++) {
            VARCHAR.writeString(blockBuilder, createFixedWidthString(i, offset, width));
        }
        return blockBuilder.build();
    }

    public static Slice[] getBlockSlices(Block block, int rowSize, int width)
    {
        Slice[] slice = new Slice[rowSize];
        for (int i = 0; i < rowSize; i++) {
            slice[i] = block.getSlice(i, 0, width);
        }
        return slice;
    }

    private static String createFixedWidthString(int index, int offset, int width)
    {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder stringBuilder = new StringBuilder();
        for (int j = 0; j < width; j++) {
            stringBuilder.append(str.charAt((index + offset + j) % str.length()));
        }
        return stringBuilder.toString();
    }
}
