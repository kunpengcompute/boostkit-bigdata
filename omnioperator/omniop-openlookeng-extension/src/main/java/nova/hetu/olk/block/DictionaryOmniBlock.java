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
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.ByteArrayBlock;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.DictionaryBlockEncoding;
import io.prestosql.spi.block.DictionaryId;
import io.prestosql.spi.block.Int128ArrayBlock;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.block.ShortArrayBlock;
import io.prestosql.spi.block.VariableWidthBlock;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.ContainerVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.ShortVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecEncoding;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.spi.block.BlockUtil.checkArrayRange;
import static io.prestosql.spi.block.BlockUtil.checkValidPosition;
import static io.prestosql.spi.block.BlockUtil.checkValidPositions;
import static io.prestosql.spi.block.BlockUtil.checkValidRegion;
import static io.prestosql.spi.block.BlockUtil.countUsedPositions;
import static io.prestosql.spi.block.DictionaryId.randomDictionaryId;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.buildRowOmniBlock;

/**
 * The type Dictionary omni block.
 *
 * @param <T> the type parameter
 * @since 20210630
 */
public class DictionaryOmniBlock<T>
        implements Block<T>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DictionaryOmniBlock.class).instanceSize()
            + ClassLayout.parseClass(DictionaryId.class).instanceSize();

    private final int positionCount;

    private final Block<T> dictionary;

    private final int idsOffset;

    private final int[] ids;

    private final long retainedSizeInBytes;

    private final DictionaryId dictionarySourceId;

    private final DictionaryVec dictionaryVec;

    private volatile long sizeInBytes = -1;

    private volatile long logicalSizeInBytes = -1;

    private volatile int uniqueIds = -1;

    /**
     * Instantiates a new Dictionary omni block.
     *
     * @param dictionary the dictionary
     * @param ids the ids
     */
    public DictionaryOmniBlock(Vec dictionary, int[] ids)
    {
        this(requireNonNull(ids, "ids is null").length, dictionary, ids);
    }

    /**
     * Instantiates a new Dictionary omni block.
     *
     * @param positionCount the position count
     * @param dictionary the dictionary
     * @param ids the ids
     */
    public DictionaryOmniBlock(int positionCount, Vec dictionary, int[] ids)
    {
        this(0, positionCount, dictionary, ids, false, randomDictionaryId());
    }

    /**
     * Instantiates a new Dictionary omni block.
     *
     * @param positionCount the position count
     * @param dictionary the dictionary
     * @param ids the ids
     * @param dictionaryIsCompacted the dictionary is compacted
     */
    public DictionaryOmniBlock(int positionCount, Vec dictionary, int[] ids, boolean dictionaryIsCompacted)
    {
        this(0, positionCount, dictionary, ids, dictionaryIsCompacted, randomDictionaryId());
    }

    /**
     * Instantiates a new Dictionary omni block.
     *
     * @param positionCount the position count
     * @param dictionary the dictionary
     * @param ids the ids
     * @param dictionaryIsCompacted the dictionary is compacted
     * @param dictionarySourceId the dictionary source id
     */
    public DictionaryOmniBlock(int positionCount, Vec dictionary, int[] ids, boolean dictionaryIsCompacted,
                               DictionaryId dictionarySourceId)
    {
        this(0, positionCount, dictionary, ids, dictionaryIsCompacted, dictionarySourceId);
    }

    /**
     * Instantiates a new Dictionary omni block.
     *
     * @param idsOffset the ids offset
     * @param positionCount the position count
     * @param dictionary the dictionary
     * @param ids the ids
     * @param dictionaryIsCompacted the dictionary is compacted
     * @param dictionarySourceId the dictionary source id
     */
    public DictionaryOmniBlock(int idsOffset, int positionCount, Vec dictionary, int[] ids,
                               boolean dictionaryIsCompacted, DictionaryId dictionarySourceId)
    {
        requireNonNull(dictionary, "dictionary is null");
        requireNonNull(ids, "ids is null");

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }

        this.idsOffset = idsOffset;
        if (ids.length - idsOffset < positionCount) {
            throw new IllegalArgumentException("ids length is less than positionCount");
        }

        this.positionCount = positionCount;
        this.dictionaryVec = new DictionaryVec(dictionary, ids);
        this.dictionary = createFlatBlock(dictionary.getType().getId(), dictionary);
        this.ids = ids;
        this.dictionarySourceId = requireNonNull(dictionarySourceId, "dictionarySourceId is null");
        this.retainedSizeInBytes = INSTANCE_SIZE + this.dictionary.getRetainedSizeInBytes() + sizeOf(ids);

        if (dictionaryIsCompacted) {
            this.sizeInBytes = this.retainedSizeInBytes;
            this.uniqueIds = this.dictionary.getPositionCount();
        }
    }

    /**
     * Instantiates a new Dictionary omni block.
     *
     * @param dictionaryIsCompacted the dictionary is compacted
     * @param dictionarySourceId the dictionary source id
     */
    public DictionaryOmniBlock(DictionaryVec dictionaryVec, boolean dictionaryIsCompacted,
                               DictionaryId dictionarySourceId)
    {
        this.positionCount = dictionaryVec.getSize();
        this.idsOffset = 0;
        this.dictionary = expandDictionary(dictionaryVec);
        this.ids = getIds(positionCount);
        this.dictionarySourceId = requireNonNull(dictionarySourceId, "dictionarySourceId is null");
        this.retainedSizeInBytes = INSTANCE_SIZE + dictionary.getRetainedSizeInBytes() + sizeOf(ids);
        this.dictionaryVec = dictionaryVec;

        if (dictionaryIsCompacted) {
            this.sizeInBytes = this.retainedSizeInBytes;
            this.uniqueIds = dictionaryVec.getSize();
        }
    }

    private static Block buildBlock(Vec dictionary)
    {
        DataType dataType = dictionary.getType();
        Block dictionaryBlock;
        VecEncoding vecEncoding = dictionary.getEncoding();
        switch (vecEncoding) {
            case OMNI_VEC_ENCODING_FLAT:
                dictionaryBlock = createFlatOmniBlock(dataType.getId(), dictionary);
                break;
            case OMNI_VEC_ENCODING_DICTIONARY:
                dictionaryBlock = new DictionaryOmniBlock((DictionaryVec) dictionary, false, randomDictionaryId());
                break;
            case OMNI_VEC_ENCODING_CONTAINER:
                dictionaryBlock = buildRowOmniBlock((ContainerVec) dictionary);
                break;
            default:
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support encoding " + vecEncoding);
        }
        return dictionaryBlock;
    }

    private static Block createFlatOmniBlock(DataType.DataTypeId dataTypeId, Vec dictionary)
    {
        Block dictionaryBlock;
        switch (dataTypeId) {
            case OMNI_BOOLEAN:
                dictionaryBlock = new ByteArrayOmniBlock(dictionary.getSize(), (BooleanVec) dictionary);
                break;
            case OMNI_INT:
            case OMNI_DATE32:
                dictionaryBlock = new IntArrayOmniBlock(dictionary.getSize(), (IntVec) dictionary);
                break;
            case OMNI_SHORT:
                dictionaryBlock = new ShortArrayOmniBlock(dictionary.getSize(), (ShortVec) dictionary);
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                dictionaryBlock = new LongArrayOmniBlock(dictionary.getSize(), (LongVec) dictionary);
                break;
            case OMNI_DOUBLE:
                dictionaryBlock = new DoubleArrayOmniBlock(dictionary.getSize(), (DoubleVec) dictionary);
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                dictionaryBlock = new VariableWidthOmniBlock(dictionary.getSize(), (VarcharVec) dictionary);
                break;
            case OMNI_DECIMAL128:
                dictionaryBlock = new Int128ArrayOmniBlock(dictionary.getSize(), (Decimal128Vec) dictionary);
                break;
            default:
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support Type " + dataTypeId);
        }
        return dictionaryBlock;
    }

    private static Block createVariableWidthBlock(Vec vec, int positionCount)
    {
        VarcharVec varcharVec = (VarcharVec) vec;
        Slice slice = Slices.wrappedBuffer(varcharVec.get(0, positionCount));
        int[] offsets = new int[positionCount + 1];
        for (int i = 0; i < positionCount; i++) {
            offsets[i + 1] = offsets[i] + varcharVec.getDataLength(i);
        }

        return new VariableWidthBlock(positionCount, slice, offsets,
                varcharVec.hasNull()
                        ? Optional.of(varcharVec.getValuesNulls(0, positionCount))
                        : Optional.empty());
    }

    private static Block createInt128ArrayBlock(Vec vec, int positionCount)
    {
        Decimal128Vec decimal128Vec = (Decimal128Vec) vec;
        return new Int128ArrayBlock(positionCount, Optional.of(decimal128Vec.getValuesNulls(0, positionCount)),
                decimal128Vec.get(0, positionCount));
    }

    private static Block createDoubleArrayBlock(Vec vec, int positionCount)
    {
        DoubleVec doubleVec = (DoubleVec) vec;
        boolean[] valuesNulls = doubleVec.getValuesNulls(0, positionCount);
        long[] values = new long[positionCount];
        for (int j = 0; j < positionCount; j++) {
            if (!vec.isNull(j)) {
                values[j] = doubleToLongBits(doubleVec.get(j));
            }
        }
        return new LongArrayBlock(positionCount, Optional.of(valuesNulls), values);
    }

    private static Block createShortArrayBlock(Vec vec, int positionCount)
    {
        ShortVec shortVec = (ShortVec) vec;
        return new ShortArrayBlock(positionCount, Optional.of(shortVec.getValuesNulls(0, positionCount)),
                shortVec.get(0, positionCount));
    }

    private static Block createLongArrayBlock(Vec vec, int positionCount)
    {
        LongVec longVec = (LongVec) vec;
        return new LongArrayBlock(positionCount, Optional.of(longVec.getValuesNulls(0, positionCount)),
                longVec.get(0, positionCount));
    }

    private static Block createIntArrayBlock(Vec vec, int positionCount)
    {
        IntVec intVec = (IntVec) vec;
        return new IntArrayBlock(positionCount, Optional.of(intVec.getValuesNulls(0, positionCount)),
                intVec.get(0, positionCount));
    }

    private static Block createByteArrayBlock(Vec vec, int positionCount)
    {
        BooleanVec booleanVec = (BooleanVec) vec;
        byte[] bytes = booleanVec.getValuesBuf().getBytes(0, positionCount);
        return new ByteArrayBlock(positionCount, Optional.of(booleanVec.getValuesNulls(0, positionCount)),
                bytes);
    }

    private static Block createFlatBlock(DataType.DataTypeId dataTypeId, Vec dictionary)
    {
        switch (dataTypeId) {
            case OMNI_BOOLEAN:
                return createByteArrayBlock(dictionary, dictionary.getSize());
            case OMNI_INT:
            case OMNI_DATE32:
                return createIntArrayBlock(dictionary, dictionary.getSize());
            case OMNI_SHORT:
                return createShortArrayBlock(dictionary, dictionary.getSize());
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                return createLongArrayBlock(dictionary, dictionary.getSize());
            case OMNI_DOUBLE:
                return createDoubleArrayBlock(dictionary, dictionary.getSize());
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                return createVariableWidthBlock(dictionary, dictionary.getSize());
            case OMNI_DECIMAL128:
                return createInt128ArrayBlock(dictionary, dictionary.getSize());
            default:
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support Type " + dataTypeId);
        }
    }

    public static Block expandDictionary(DictionaryVec dictionaryVec)
    {
        Vec vec = expandDictionaryVec(dictionaryVec);
        DataType dataType = vec.getType();
        Block dictionaryBlock = createFlatBlock(dataType.getId(), vec);
        vec.close();
        return dictionaryBlock;
    }

    public static int[] getIds(int positionCount)
    {
        int[] ids = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            ids[i] = i;
        }
        return ids;
    }

    /**
     * DictionaryVec transfer to Vec
     *
     * @return vector
     */
    public static Vec expandDictionaryVec(DictionaryVec dictionaryVec)
    {
        return dictionaryVec.expandDictionary();
    }

    @Override
    public Vec getValues()
    {
        return dictionaryVec;
    }

    @Override
    public void close()
    {
        dictionaryVec.close();
    }

    @Override
    public int getSliceLength(int position)
    {
        return dictionary.getSliceLength(getId(position));
    }

    @Override
    public byte getByte(int position, int offset)
    {
        return dictionary.getByte(getId(position), offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        return dictionary.getShort(getId(position), offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        return dictionary.getInt(getId(position), offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        return dictionary.getLong(getId(position), offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        return dictionary.getSlice(getId(position), offset, length);
    }

    @Override
    public String getString(int position, int offset, int length)
    {
        return dictionary.getString(getId(position), offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        return dictionary.getObject(getId(position), clazz);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        return dictionary.bytesEqual(getId(position), offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        return dictionary.bytesCompare(getId(position), offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        dictionary.writeBytesTo(getId(position), offset, length, blockBuilder);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        dictionary.writePositionTo(getId(position), blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        return dictionary.equals(getId(position), offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        return dictionary.hash(getId(position), offset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition,
                         int rightOffset, int rightLength)
    {
        return dictionary.compareTo(getId(leftPosition), leftOffset, leftLength, rightBlock, rightPosition, rightOffset,
                rightLength);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        return dictionary.getSingleValueBlock(getId(position));
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
            calculateCompactSize();
        }
        return sizeInBytes;
    }

    private void calculateCompactSize()
    {
        int uniqueIds = 0;
        boolean[] used = new boolean[dictionary.getPositionCount()];
        for (int i = 0; i < positionCount; i++) {
            int position = getId(i);
            if (!used[position]) {
                uniqueIds++;
                used[position] = true;
            }
        }
        this.sizeInBytes = dictionary.getPositionsSizeInBytes(used) + (Integer.BYTES * (long) positionCount);
        this.uniqueIds = uniqueIds;
    }

    @Override
    public long getLogicalSizeInBytes()
    {
        if (logicalSizeInBytes >= 0) {
            return logicalSizeInBytes;
        }

        // Calculation of logical size can be performed as part of
        // calculateCompactSize() with minor modifications.
        // Keeping this calculation separate as this is a little more expensive and may
        // not be called as often.
        long sizeInBytes = 0;
        long[] seenSizes = new long[dictionary.getPositionCount()];
        Arrays.fill(seenSizes, -1L);
        for (int i = 0; i < getPositionCount(); i++) {
            int position = getId(i);
            if (seenSizes[position] < 0) {
                seenSizes[position] = dictionary.getRegionSizeInBytes(position, 1);
            }
            sizeInBytes += seenSizes[position];
        }

        logicalSizeInBytes = sizeInBytes;
        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int positionOffset, int length)
    {
        if (positionOffset == 0 && length == getPositionCount()) {
            // Calculation of getRegionSizeInBytes is expensive in this class.
            // On the other hand, getSizeInBytes result is cached.
            return getSizeInBytes();
        }

        boolean[] used = new boolean[dictionary.getPositionCount()];
        for (int i = positionOffset; i < positionOffset + length; i++) {
            used[getId(i)] = true;
        }
        return dictionary.getPositionsSizeInBytes(used) + Integer.BYTES * (long) length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        checkValidPositions(positions, positionCount);

        boolean[] used = new boolean[dictionary.getPositionCount()];
        for (int i = 0; i < positions.length; i++) {
            if (positions[i]) {
                used[getId(i)] = true;
            }
        }
        return dictionary.getPositionsSizeInBytes(used) + (Integer.BYTES * (long) countUsedPositions(positions));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return dictionary.getEstimatedDataSizeForStats(getId(position));
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(dictionary, dictionary.getRetainedSizeInBytes());
        consumer.accept(ids, sizeOf(ids));
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        return DictionaryBlockEncoding.NAME;
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        int[] newIds = new int[length];

        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            newIds[i] = getId(position);
        }
        return new DictionaryBlock(dictionary, newIds);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);
        return new DictionaryBlock(idsOffset + positionOffset, length, dictionary, ids, false, dictionarySourceId);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        checkValidRegion(positionCount, position, length);
        int[] newIds = Arrays.copyOfRange(ids, idsOffset + position, idsOffset + position + length);
        DictionaryBlock dictionaryBlock = new DictionaryBlock(dictionary, newIds);
        return dictionaryBlock.compact();
    }

    @Override
    public boolean isNull(int position)
    {
        return dictionary.isNull(getId(position));
    }

    @Override
    public Block getPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        int[] newIds = new int[length];
        boolean isCompact = isCompact() && length >= dictionary.getPositionCount();
        boolean[] seen = null;
        if (isCompact) {
            seen = new boolean[dictionary.getPositionCount()];
        }
        for (int i = 0; i < length; i++) {
            newIds[i] = getId(positions[offset + i]);
            if (isCompact) {
                seen[newIds[i]] = true;
            }
        }

        for (int i = 0; i < dictionary.getPositionCount() && isCompact; i++) {
            isCompact &= seen[i];
        }
        return new DictionaryBlock(newIds.length, dictionary, newIds, isCompact, getDictionarySourceId());
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("DictionaryOmniBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    @Override
    public Block getLoadedBlock()
    {
        Block loadedDictionary = dictionary.getLoadedBlock();

        if (loadedDictionary == dictionary) {
            return this;
        }

        return new DictionaryBlock(idsOffset, getPositionCount(), loadedDictionary, ids, false, randomDictionaryId());
    }

    /**
     * Gets dictionary.
     *
     * @return the dictionary
     */
    public Block getDictionary()
    {
        return dictionary;
    }

    /**
     * Gets ids.
     *
     * @return the ids
     */
    public int[] getIds()
    {
        return ids;
    }

    /**
     * Gets id.
     *
     * @param position the position
     * @return the id
     */
    public int getId(int position)
    {
        checkValidPosition(position, positionCount);
        return ids[position + idsOffset];
    }

    /**
     * Gets dictionary source id.
     *
     * @return the dictionary source id
     */
    public DictionaryId getDictionarySourceId()
    {
        return dictionarySourceId;
    }

    /**
     * Is compact boolean.
     *
     * @return the boolean
     */
    public boolean isCompact()
    {
        if (uniqueIds < 0) {
            calculateCompactSize();
        }
        return uniqueIds == dictionary.getPositionCount();
    }

    /**
     * Compact dictionary omni block.
     *
     * @return the dictionary omni block
     */
    public DictionaryOmniBlock compact()
    {
        if (isCompact()) {
            return this;
        }

        // determine which dictionary entries are referenced and build a reindex for
        // them
        int dictionarySize = dictionary.getPositionCount();
        IntArrayList dictionaryPositionsToCopy = new IntArrayList(min(dictionarySize, positionCount));
        int[] remapIndex = new int[dictionarySize];
        Arrays.fill(remapIndex, -1);

        int newIndex = 0;
        for (int i = 0; i < positionCount; i++) {
            int dictionaryIndex = getId(i);
            if (remapIndex[dictionaryIndex] == -1) {
                dictionaryPositionsToCopy.add(dictionaryIndex);
                remapIndex[dictionaryIndex] = newIndex;
                newIndex++;
            }
        }

        // entire dictionary is referenced
        if (dictionaryPositionsToCopy.size() == dictionarySize) {
            return this;
        }

        // compact the dictionary
        int[] newIds = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            int newId = remapIndex[getId(i)];
            if (newId == -1) {
                throw new IllegalStateException("reference to a non-existent key");
            }
            newIds[i] = newId;
        }
        try {
            Block compactDictionary = dictionary.copyPositions(dictionaryPositionsToCopy.elements(), 0,
                    dictionaryPositionsToCopy.size());
            DictionaryOmniBlock dictionaryOmniBlock = new DictionaryOmniBlock(positionCount,
                    (Vec) compactDictionary.getValues(), newIds, true);
            compactDictionary.close();
            return dictionaryOmniBlock;
        }
        catch (UnsupportedOperationException e) {
            // ignore if copy positions is not supported for the dictionary block
            return this;
        }
    }

    @Override
    public int filter(int[] positions, int positionCount, int[] matchedPositions, Function<Object, Boolean> test)
    {
        int matchCount = 0;
        for (int i = 0; i < positionCount; i++) {
            if (dictionary.isNull(getId(positions[i]))) {
                if (test.apply(null)) {
                    matchedPositions[matchCount++] = positions[i];
                }
            }
            else {
                T value = dictionary.get(getId(positions[i]));
                if (test.apply(value)) {
                    matchedPositions[matchCount++] = positions[i];
                }
            }
        }

        return matchCount;
    }

    @Override
    public T get(int position)
    {
        if (dictionary.isNull(getId(position))) {
            return null;
        }
        return dictionary.get(getId(position));
    }

    @Override
    public boolean isExtensionBlock()
    {
        return true;
    }
}
