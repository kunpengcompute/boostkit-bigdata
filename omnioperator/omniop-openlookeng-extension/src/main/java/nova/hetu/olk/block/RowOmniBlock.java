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

import io.prestosql.spi.block.AbstractRowBlock;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.type.ContainerDataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.ContainerVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecAllocator;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.function.BiConsumer;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.spi.block.BlockUtil.checkArrayRange;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.VecAllocatorHelper.getVecAllocatorFromBlocks;

/**
 * The type Row omni block.
 *
 * @param <T> the type parameter
 * @since 20210630
 */
public class RowOmniBlock<T>
        extends AbstractRowBlock<T>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(RowOmniBlock.class).instanceSize();

    private final VecAllocator vecAllocator;

    private final int startOffset;

    private final int positionCount;

    private final byte[] rowIsNull;

    private final int[] fieldBlockOffsets;

    private final Block<T>[] fieldBlocks;

    private volatile long sizeInBytes;

    private final long retainedSizeInBytes;

    private final DataType dataType;

    private boolean[] isNull;

    /**
     * Create a row block directly from columnar nulls and field blocks.
     *
     * @param <T> the type parameter
     * @param positionCount the position count
     * @param rowIsNull the row is null
     * @param fieldBlocks the field blocks
     * @return the block
     */
    public static <T> Block<T> fromFieldBlocks(VecAllocator vecAllocator, int positionCount, Optional<byte[]> rowIsNull,
                                               Block<T>[] fieldBlocks, Type blockType)
    {
        int[] fieldBlockOffsets = new int[positionCount + 1];
        for (int position = 0; position < positionCount; position++) {
            fieldBlockOffsets[position + 1] = fieldBlockOffsets[position]
                    + (rowIsNull.isPresent() && rowIsNull.get()[position] == Vec.NULL ? 0 : 1);
        }
        validateConstructorArguments(0, positionCount, rowIsNull.orElse(null), fieldBlockOffsets, fieldBlocks);
        // transform field blocks to off-heap
        Block[] newOffHeapFieldBlocks = new Block[fieldBlocks.length];
        for (int blockIndex = 0; blockIndex < fieldBlocks.length; ++blockIndex) {
            Block<T> block = fieldBlocks[blockIndex];
            newOffHeapFieldBlocks[blockIndex] = OperatorUtils.buildOffHeapBlock(vecAllocator, block,
                    block.getClass().getSimpleName(), block.getPositionCount(),
                    blockType == null ? null : blockType.getTypeParameters().get(blockIndex));
        }
        return new RowOmniBlock(0, positionCount, rowIsNull.orElse(null), fieldBlockOffsets, newOffHeapFieldBlocks,
                OperatorUtils.toDataType(blockType));
    }

    /**
     * Create a row block directly without per element validations.
     *
     * @param startOffset the start offset
     * @param positionCount the position count
     * @param rowIsNull the row is null
     * @param fieldBlockOffsets the field block offsets
     * @param fieldBlocks the field blocks
     * @param dataType data type of block
     * @return the row omni block
     */
    static RowOmniBlock createRowBlockInternal(int startOffset, int positionCount, @Nullable byte[] rowIsNull,
                                               int[] fieldBlockOffsets, Block[] fieldBlocks, DataType dataType)
    {
        validateConstructorArguments(startOffset, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks);
        return new RowOmniBlock(startOffset, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks, dataType);
    }

    /**
     * validate constructor arguments
     *
     * @param startOffset the start offset
     * @param positionCount the position count
     * @param rowIsNull the row is null
     * @param fieldBlockOffsets the field block offsets
     * @param fieldBlocks the field blocks
     */
    public static void validateConstructorArguments(int startOffset, int positionCount, @Nullable byte[] rowIsNull,
                                                    int[] fieldBlockOffsets, Block[] fieldBlocks)
    {
        if (startOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }

        if (rowIsNull != null && rowIsNull.length - startOffset < positionCount) {
            throw new IllegalArgumentException("rowIsNull length is less than positionCount");
        }

        requireNonNull(fieldBlockOffsets, "fieldBlockOffsets is null");
        if (fieldBlockOffsets.length - startOffset < positionCount + 1) {
            throw new IllegalArgumentException("fieldBlockOffsets length is less than positionCount");
        }

        requireNonNull(fieldBlocks, "fieldBlocks is null");

        if (fieldBlocks.length <= 0) {
            throw new IllegalArgumentException("Number of fields in RowOmniBlock must be positive");
        }

        int firstFieldBlockPositionCount = fieldBlocks[0].getPositionCount();
        for (int i = 1; i < fieldBlocks.length; i++) {
            if (firstFieldBlockPositionCount != fieldBlocks[i].getPositionCount()) {
                throw new IllegalArgumentException(format("length of field blocks differ: field 0: %s, block %s: %s",
                        firstFieldBlockPositionCount, i, fieldBlocks[i].getPositionCount()));
            }
        }
    }

    @Override
    public Vec getValues()
    {
        Block[] rawFieldBlocks = this.getRawFieldBlocks();
        int numFields = rawFieldBlocks.length;
        long[] vectorAddresses = new long[numFields];
        DataType[] dataTypes = new DataType[numFields];
        for (int i = 0; i < numFields; ++i) {
            Vec vec = (Vec) rawFieldBlocks[i].getValues();
            long nativeVectorAddress = vec.getNativeVector();
            vectorAddresses[i] = nativeVectorAddress;
        }
        ContainerVec containerVec = new ContainerVec(vecAllocator, numFields, this.getPositionCount(), vectorAddresses,
                ((ContainerDataType) dataType).getFieldTypes());
        containerVec.setNulls(0, this.getRowIsNull(), 0, this.getPositionCount());
        return containerVec;
    }

    /**
     * Use createRowBlockInternal or fromFieldBlocks instead of this method. The
     * caller of this method is assumed to have validated the arguments with
     * validateConstructorArguments.
     *
     * @param startOffset the start offset
     * @param positionCount the position count
     * @param rowIsNull the row is null
     * @param fieldBlockOffsets the field block offsets
     * @param fieldBlocks the field blocks
     * @param dataType data type of block
     */
    public RowOmniBlock(int startOffset, int positionCount, @Nullable byte[] rowIsNull, int[] fieldBlockOffsets,
                        Block[] fieldBlocks, DataType dataType)
    {
        super(fieldBlocks.length);
        this.vecAllocator = getVecAllocatorFromBlocks(fieldBlocks);

        this.startOffset = startOffset;
        this.positionCount = positionCount;
        this.rowIsNull = rowIsNull;
        this.fieldBlockOffsets = fieldBlockOffsets;
        this.fieldBlocks = fieldBlocks;

        this.sizeInBytes = -1;
        long retainedSizeInBytes = INSTANCE_SIZE + sizeOf(fieldBlockOffsets) + sizeOf(rowIsNull);
        for (Block fieldBlock : fieldBlocks) {
            retainedSizeInBytes += fieldBlock.getRetainedSizeInBytes();
        }
        this.retainedSizeInBytes = retainedSizeInBytes;
        this.dataType = dataType;
    }

    @Override
    public boolean isExtensionBlock()
    {
        return true;
    }

    @Override
    public Block[] getRawFieldBlocks()
    {
        return fieldBlocks;
    }

    @Override
    protected int[] getFieldBlockOffsets()
    {
        return fieldBlockOffsets;
    }

    @Override
    protected int getOffsetBase()
    {
        return startOffset;
    }

    @Override
    @Nullable
    public boolean[] getRowIsNull()
    {
        if (isNull != null) {
            return isNull;
        }
        isNull = new boolean[rowIsNull.length];
        for (int i = 0; i < rowIsNull.length; i++) {
            isNull[i] = rowIsNull[i] == Vec.NULL;
        }
        return isNull;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        if (sizeInBytes < 0) {
            calculateSize();
        }
        return sizeInBytes;
    }

    private void calculateSize()
    {
        int startFieldBlockOffset = fieldBlockOffsets[startOffset];
        int endFieldBlockOffset = fieldBlockOffsets[startOffset + positionCount];
        int fieldBlockLength = endFieldBlockOffset - startFieldBlockOffset;

        long sizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) positionCount;
        for (int i = 0; i < numFields; i++) {
            sizeInBytes += fieldBlocks[i].getRegionSizeInBytes(startFieldBlockOffset, fieldBlockLength);
        }
        this.sizeInBytes = sizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        for (int i = 0; i < numFields; i++) {
            consumer.accept(fieldBlocks[i], fieldBlocks[i].getRetainedSizeInBytes());
        }
        consumer.accept(fieldBlockOffsets, sizeOf(fieldBlockOffsets));
        consumer.accept(rowIsNull, sizeOf(rowIsNull));
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public String toString()
    {
        return format("RowOmniBlock{numFields=%d, positionCount=%d}", numFields, getPositionCount());
    }

    @Override
    public Block getLoadedBlock()
    {
        boolean allLoaded = true;
        Block[] loadedFieldBlocks = new Block[fieldBlocks.length];

        for (int i = 0; i < fieldBlocks.length; i++) {
            loadedFieldBlocks[i] = fieldBlocks[i].getLoadedBlock();
            if (loadedFieldBlocks[i] != fieldBlocks[i]) {
                allLoaded = false;
            }
        }

        if (allLoaded) {
            return this;
        }
        return createRowBlockInternal(startOffset, positionCount, rowIsNull, fieldBlockOffsets, loadedFieldBlocks,
                dataType);
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        int[] newOffsets = new int[length + 1];
        boolean[] newRowIsNull = new boolean[length];

        IntArrayList fieldBlockPositions = new IntArrayList(length);
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            if (isNull(position)) {
                newRowIsNull[i] = true;
                newOffsets[i + 1] = newOffsets[i];
            }
            else {
                newOffsets[i + 1] = newOffsets[i] + 1;
                fieldBlockPositions.add(getFieldBlockOffset(position));
            }
        }

        Block[] newBlocks = new Block[numFields];
        for (int i = 0; i < numFields; i++) {
            newBlocks[i] = getRawFieldBlocks()[i].copyPositions(fieldBlockPositions.elements(), 0, fieldBlockPositions.size());
        }
        return createRowBlockInternal(0, length, OperatorUtils.transformBooleanToByte(newRowIsNull), newOffsets, newBlocks,
                dataType);
    }

    @Override
    public void close()
    {
        for (Block<T> fieldBlock : fieldBlocks) {
            fieldBlock.close();
        }
    }
}
