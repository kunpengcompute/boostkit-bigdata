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

package nova.hetu.olk.block;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.Int128ArrayBlockEncoding;
import io.prestosql.spi.util.BloomFilter;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.Vec;
import org.openjdk.jol.info.ClassLayout;

import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.spi.block.BlockUtil.checkArrayRange;
import static io.prestosql.spi.block.BlockUtil.checkValidRegion;
import static io.prestosql.spi.block.BlockUtil.countUsedPositions;

/**
 * The type Int 128 array omni block.
 *
 * @since 20210630
 */
public class Int128ArrayOmniBlock
        implements Block<long[]>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Int128ArrayOmniBlock.class).instanceSize();

    /**
     * The constant INT128_BYTES.
     */
    public static final int INT128_BYTES = Long.BYTES + Long.BYTES;

    private final int positionCount;

    private final Decimal128Vec values;

    private final long sizeInBytes;

    private final long retainedSizeInBytes;

    private boolean hasNull;

    /**
     * Instantiates a new Int 128 array omni block.
     *
     * @param positionCount the position count
     * @param values the values
     */
    public Int128ArrayOmniBlock(int positionCount, Decimal128Vec values)
    {
        this.positionCount = positionCount;
        this.values = values;
        this.sizeInBytes = (INT128_BYTES + Byte.BYTES) * (long) positionCount;
        this.retainedSizeInBytes = INSTANCE_SIZE + this.values.getCapacityInBytes();
        this.hasNull = values.hasNull();
    }

    /**
     * Instantiates a new Int 128 array omni block.
     *
     * @param positionOffset the position offset
     * @param positionCount the position count
     * @param valueIsNull the value is null
     * @param values the values
     */
    public Int128ArrayOmniBlock(int positionOffset, int positionCount, byte[] valueIsNull,
                                long[] values)
    {
        if (positionOffset < 0) {
            throw new IllegalArgumentException("positionOffset is negative");
        }
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values.length - (positionOffset * 2) < positionCount * 2) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }

        this.values = new Decimal128Vec(positionCount);
        this.values.put(values, 0, positionOffset * 2, positionCount * 2);

        if (valueIsNull != null && valueIsNull.length - positionOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }

        if (valueIsNull != null) {
            this.values.setNulls(0, valueIsNull, positionOffset, positionCount);
            this.hasNull = true;
        }
        sizeInBytes = (INT128_BYTES + Byte.BYTES) * (long) positionCount;
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + this.values.getCapacityInBytes();
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return (INT128_BYTES + Byte.BYTES) * (long) length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        return (INT128_BYTES + Byte.BYTES) * (long) countUsedPositions(positions);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : INT128_BYTES;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(values, (long) values.getCapacityInBytes());
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset == 0) {
            return values.get(position)[0];
        }
        if (offset == 8) {
            return values.get(position)[1];
        }
        throw new IllegalArgumentException("offset must be 0 or 8");
    }

    @Override
    public boolean mayHaveNull()
    {
        return hasNull;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return values.isNull(position);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        blockBuilder.writeLong(values.get(position)[0]);
        blockBuilder.writeLong(values.get(position)[1]);
        blockBuilder.closeEntry();
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new Int128ArrayOmniBlock(0, 1, isNull(position) ? new byte[]{Vec.NULL} : null,
                values.get(position));
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);
        Decimal128Vec newValues = values.copyPositions(positions, offset, length);
        return new Int128ArrayOmniBlock(length, newValues);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);
        Decimal128Vec newValues = values.slice(positionOffset, length);
        return new Int128ArrayOmniBlock(length, newValues);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        Decimal128Vec newValues = values.slice(positionOffset, length);
        values.close();
        return new Int128ArrayOmniBlock(length, newValues);
    }

    @Override
    public String getEncodingName()
    {
        return Int128ArrayBlockEncoding.NAME;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("Int128ArrayOmniBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    @Override
    public boolean[] filter(BloomFilter filter, boolean[] validPositions)
    {
        for (int i = 0; i < values.getSize() / 2; i++) {
            Slice value = Slices.wrappedLongArray(values.get(i));
            validPositions[i] = validPositions[i] && filter.test(value);
        }
        return validPositions;
    }

    @Override
    public int filter(int[] positions, int positionCount, int[] matchedPositions, Function<Object, Boolean> test)
    {
        int matchCount = 0;
        long[] val;
        for (int i = 0; i < positionCount; i++) {
            if (values.isNull(positions[i])) {
                if (test.apply(null)) {
                    matchedPositions[matchCount++] = positions[i];
                }
            }
            else {
                val = values.get(positions[i]);
                if (test.apply(val)) {
                    matchedPositions[matchCount++] = positions[i];
                }
            }
        }

        return matchCount;
    }

    @Override
    public long[] get(int position)
    {
        if (values.isNull(position)) {
            return null;
        }
        return values.get(position);
    }

    @Override
    public void close()
    {
        values.close();
    }

    @Override
    public boolean isExtensionBlock()
    {
        return true;
    }

    @Override
    public Object getValues()
    {
        return values;
    }

    @Override
    public void setClosable(boolean isClosable)
    {
        values.setClosable(isClosable);
    }
}
