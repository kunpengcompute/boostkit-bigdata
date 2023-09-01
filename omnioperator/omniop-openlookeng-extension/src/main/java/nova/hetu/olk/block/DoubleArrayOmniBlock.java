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

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.LongArrayBlockEncoding;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.Vec;
import org.openjdk.jol.info.ClassLayout;

import java.util.function.BiConsumer;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.spi.block.BlockUtil.checkArrayRange;
import static io.prestosql.spi.block.BlockUtil.checkValidRegion;
import static io.prestosql.spi.block.BlockUtil.countUsedPositions;
import static java.lang.Double.doubleToLongBits;

/**
 * The type Double array omni block.
 *
 * @since 20210630
 */
public class DoubleArrayOmniBlock
        implements Block<Double>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DoubleArrayOmniBlock.class).instanceSize();

    private final int positionCount;

    private final DoubleVec values;

    private final long sizeInBytes;

    private final long retainedSizeInBytes;

    private boolean hasNull;

    /**
     * Instantiates a new Double array omni block.
     *
     * @param positionCount the position count
     * @param values the values
     */
    public DoubleArrayOmniBlock(int positionCount, DoubleVec values)
    {
        this.positionCount = positionCount;
        this.values = values;
        this.sizeInBytes = (Double.BYTES + Byte.BYTES) * (long) positionCount;
        this.retainedSizeInBytes = INSTANCE_SIZE + this.values.getCapacityInBytes();
        this.hasNull = values.hasNull();
    }

    /**
     * Instantiates a new Double array omni block.
     *
     * @param arrayOffset the array offset
     * @param positionCount the position count
     * @param valueIsNull the value is null
     * @param values the values
     */
    public DoubleArrayOmniBlock(int arrayOffset, int positionCount, byte[] valueIsNull,
                                double[] values)
    {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }

        this.values = new DoubleVec(positionCount);
        this.values.put(values, 0, arrayOffset, positionCount);

        if (valueIsNull != null && valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }

        if (valueIsNull != null) {
            this.values.setNulls(0, valueIsNull, arrayOffset, positionCount);
            this.hasNull = true;
        }

        sizeInBytes = (Double.BYTES + Byte.BYTES) * (long) positionCount;
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + this.values.getCapacityInBytes();
    }

    @Override
    public Vec getValues()
    {
        return values;
    }

    @Override
    public void setClosable(boolean isClosable)
    {
        values.setClosable(isClosable);
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return (Double.BYTES + Byte.BYTES) * (long) length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        return (Double.BYTES + Byte.BYTES) * (long) countUsedPositions(positions);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : Double.BYTES;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(values.get(0, positionCount), (long) values.getCapacityInBytes());
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public double getDouble(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset != 0) {
            throw new IllegalArgumentException("offset must be zero");
        }
        return values.get(position);
    }

    @Override
    public long getLong(int position, int offset)
    {
        return doubleToLongBits(getDouble(position, offset));
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
    public boolean mayHaveNull()
    {
        return values.hasNull();
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
        blockBuilder.writeDouble(values.get(position));
        blockBuilder.closeEntry();
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new DoubleArrayOmniBlock(0, 1, isNull(position) ? new byte[]{Vec.NULL} : null,
                new double[]{values.get(position)});
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);
        DoubleVec newValues = values.copyPositions(positions, offset, length);
        return new DoubleArrayOmniBlock(length, newValues);
    }

    @Override
    public Block<Double> getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);
        DoubleVec newValues = values.slice(positionOffset, length);
        return new DoubleArrayOmniBlock(length, newValues);
    }

    @Override
    public Block<Double> copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);
        DoubleVec newValues = values.slice(positionOffset, length);
        values.close();
        return new DoubleArrayOmniBlock(length, newValues);
    }

    @Override
    public String getEncodingName()
    {
        return LongArrayBlockEncoding.NAME;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("DoubleArrayOmniBlock{");
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
    public Double get(int position)
    {
        if (values.isNull(position)) {
            return null;
        }

        return values.get(position);
    }
}
