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

import io.prestosql.block.BlockAssertions;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import java.util.List;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.isLongDecimal;
import static io.prestosql.spi.type.Decimals.isShortDecimal;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;

public final class PageBuilderUtil
{
    private PageBuilderUtil()
    {
    }

    public static Page createSequencePage(List<? extends Type> types, int length)
    {
        return createSequencePage(types, length, new int[types.size()]);
    }

    public static Page createSequencePage(List<? extends Type> types, int length, int... initialValues)
    {
        Block[] blocks = new Block[initialValues.length];
        for (int i = 0; i < blocks.length; i++) {
            Type type = types.get(i);
            int initialValue = initialValues[i];

            if (type.equals(INTEGER)) {
                blocks[i] = BlockUtil.createIntegerSequenceBlock(initialValue, initialValue + length);
            }
            else if (type.equals(BIGINT)) {
                blocks[i] = BlockAssertions.createLongSequenceBlock(initialValue, initialValue + length);
            }
            else if (type.equals(REAL)) {
                blocks[i] = BlockAssertions.createSequenceBlockOfReal(initialValue, initialValue + length);
            }
            else if (type.equals(DOUBLE)) {
                blocks[i] = BlockAssertions.createDoubleSequenceBlock(initialValue, initialValue + length);
            }
            else if (type instanceof VarcharType) {
                blocks[i] = BlockUtil.createStringSequenceBlock(initialValue, initialValue + length,
                        (VarcharType) type);
            }
            else if (type instanceof CharType) {
                blocks[i] = BlockUtil.createStringSequenceBlock(initialValue, initialValue + length,
                        (CharType) type);
            }
            else if (type.equals(BOOLEAN)) {
                blocks[i] = BlockAssertions.createBooleanSequenceBlock(initialValue, initialValue + length);
            }
            else if (type.equals(DATE)) {
                blocks[i] = BlockAssertions.createDateSequenceBlock(initialValue, initialValue + length);
            }
            else if (type.equals(TIMESTAMP)) {
                blocks[i] = BlockAssertions.createTimestampSequenceBlock(initialValue, initialValue + length);
            }
            else if (isShortDecimal(type)) {
                blocks[i] = BlockAssertions.createShortDecimalSequenceBlock(initialValue, initialValue + length,
                        (DecimalType) type);
            }
            else if (isLongDecimal(type)) {
                blocks[i] = BlockAssertions.createLongDecimalSequenceBlock(initialValue, initialValue + length,
                        (DecimalType) type);
            }
            else {
                throw new IllegalStateException("Unsupported type " + type);
            }
        }

        return new Page(blocks);
    }

    public static Page createSequencePageWithDictionaryBlocks(List<? extends Type> types, int length)
    {
        return createSequencePageWithDictionaryBlocks(types, length, new int[types.size()]);
    }

    public static Page createSequencePageWithDictionaryBlocks(List<? extends Type> types, int length, int... initialValues)
    {
        Block[] blocks = new Block[initialValues.length];
        for (int i = 0; i < blocks.length; i++) {
            Type type = types.get(i);
            int initialValue = initialValues[i];
            if (type.equals(INTEGER)) {
                blocks[i] = BlockUtil.createIntegerDictionaryBlock(initialValue, initialValue + length);
            }
            else if (type.equals(SMALLINT)) {
                blocks[i] = BlockUtil.createShortDictionaryBlock(initialValue, initialValue + length);
            }
            else if (type.equals(BIGINT)) {
                blocks[i] = BlockUtil.createLongDictionaryBlock(initialValue, initialValue + length);
            }
            else if (type.equals(REAL)) {
                blocks[i] = BlockUtil.createRealDictionaryBlock(initialValue, initialValue + length);
            }
            else if (type.equals(DOUBLE)) {
                blocks[i] = BlockUtil.createDoubleDictionaryBlock(initialValue, initialValue + length);
            }
            else if (type instanceof VarcharType) {
                blocks[i] = BlockUtil.createStringDictionaryBlock(initialValue, initialValue + length,
                        (VarcharType) type);
            }
            else if (type instanceof CharType) {
                blocks[i] = BlockUtil.createStringDictionaryBlock(initialValue, initialValue + length,
                        (CharType) type);
            }
            else if (type.equals(BOOLEAN)) {
                blocks[i] = BlockUtil.createBooleanDictionaryBlock(initialValue, initialValue + length);
            }
            else if (type.equals(DATE)) {
                blocks[i] = BlockUtil.createDateDictionaryBlock(initialValue, initialValue + length);
            }
            else if (type.equals(TIMESTAMP)) {
                blocks[i] = BlockUtil.createTimestampDictionaryBlock(initialValue, initialValue + length);
            }
            else if (isShortDecimal(type)) {
                blocks[i] = BlockUtil.createShortDecimalDictionaryBlock(initialValue, initialValue + length,
                        (DecimalType) type);
            }
            else if (isLongDecimal(type)) {
                blocks[i] = BlockUtil.createLongDecimalDictionaryBlock(initialValue, initialValue + length,
                        (DecimalType) type);
            }
            else {
                throw new IllegalStateException("Unsupported type " + type);
            }
        }

        return new Page(blocks);
    }

    public static Page createPage(List<? extends Type> types, String prefix, List<List<Integer>> columnValues)
    {
        Block[] blocks = new Block[types.size()];
        for (int i = 0; i < blocks.length; i++) {
            Type type = types.get(i);
            if (type.equals(INTEGER)) {
                blocks[i] = BlockUtil.createIntegerBlock(columnValues.get(i));
            }
            else if (type.equals(SMALLINT)) {
                blocks[i] = BlockUtil.createShortBlock(columnValues.get(i));
            }
            else if (type.equals(BIGINT)) {
                blocks[i] = BlockUtil.createLongBlock(columnValues.get(i));
            }
            else if (type.equals(REAL)) {
                blocks[i] = BlockUtil.createRealBlock(columnValues.get(i));
            }
            else if (type.equals(DOUBLE)) {
                blocks[i] = BlockUtil.createDoubleBlock(columnValues.get(i));
            }
            else if (type instanceof VarcharType) {
                blocks[i] = BlockUtil.createStringBlock(prefix, columnValues.get(i), (VarcharType) type);
            }
            else if (type.equals(BOOLEAN)) {
                blocks[i] = BlockUtil.createBooleanBlock(columnValues.get(i));
            }
            else if (type.equals(DATE)) {
                blocks[i] = BlockUtil.createDateBlock(columnValues.get(i));
            }
            else if (type.equals(TIMESTAMP)) {
                blocks[i] = BlockUtil.createTimestampBlock(columnValues.get(i));
            }
            else if (isShortDecimal(type)) {
                blocks[i] = BlockUtil.createShortDecimalBlock(columnValues.get(i), (DecimalType) type);
            }
            else if (isLongDecimal(type)) {
                blocks[i] = BlockUtil.createLongDecimalBlock(columnValues.get(i), (DecimalType) type);
            }
            else {
                throw new IllegalStateException("Unsupported type " + type);
            }
        }

        return new Page(blocks);
    }

    public static Page createPageWithDictionaryBlocks(List<? extends Type> types, String prefix, List<List<Integer>> columnValues)
    {
        Block[] blocks = new Block[types.size()];
        for (int i = 0; i < blocks.length; i++) {
            Type type = types.get(i);
            if (type.equals(INTEGER)) {
                blocks[i] = BlockUtil.createIntegerDictionaryBlock(columnValues.get(i));
            }
            else if (type.equals(SMALLINT)) {
                blocks[i] = BlockUtil.createShortDictionaryBlock(columnValues.get(i));
            }
            else if (type.equals(BIGINT)) {
                blocks[i] = BlockUtil.createLongDictionaryBlock(columnValues.get(i));
            }
            else if (type.equals(REAL)) {
                blocks[i] = BlockUtil.createRealDictionaryBlock(columnValues.get(i));
            }
            else if (type.equals(DOUBLE)) {
                blocks[i] = BlockUtil.createDoubleDictionaryBlock(columnValues.get(i));
            }
            else if (type instanceof VarcharType) {
                blocks[i] = BlockUtil.createStringDictionaryBlock(prefix, columnValues.get(i), (VarcharType) type);
            }
            else if (type.equals(BOOLEAN)) {
                blocks[i] = BlockUtil.createBooleanDictionaryBlock(columnValues.get(i));
            }
            else if (type.equals(DATE)) {
                blocks[i] = BlockUtil.createDateDictionaryBlock(columnValues.get(i));
            }
            else if (type.equals(TIMESTAMP)) {
                blocks[i] = BlockUtil.createTimestampDictionaryBlock(columnValues.get(i));
            }
            else if (isShortDecimal(type)) {
                blocks[i] = BlockUtil.createShortDecimalDictionaryBlock(columnValues.get(i), (DecimalType) type);
            }
            else if (isLongDecimal(type)) {
                blocks[i] = BlockUtil.createLongDecimalDictionaryBlock(columnValues.get(i), (DecimalType) type);
            }
            else {
                throw new IllegalStateException("Unsupported type " + type);
            }
        }

        return new Page(blocks);
    }
}
