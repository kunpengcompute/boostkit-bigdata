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

package nova.hetu.olk.tool;

import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.block.AbstractVariableWidthBlock;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.ByteArrayBlock;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.Int128ArrayBlock;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.block.RowBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.block.VariableWidthBlock;
import io.prestosql.spi.sql.expression.Types;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import nova.hetu.olk.block.ByteArrayOmniBlock;
import nova.hetu.olk.block.DictionaryOmniBlock;
import nova.hetu.olk.block.DoubleArrayOmniBlock;
import nova.hetu.olk.block.Int128ArrayOmniBlock;
import nova.hetu.olk.block.IntArrayOmniBlock;
import nova.hetu.olk.block.LazyOmniBlock;
import nova.hetu.olk.block.LongArrayOmniBlock;
import nova.hetu.olk.block.RowOmniBlock;
import nova.hetu.olk.block.VariableWidthOmniBlock;
import nova.hetu.omniruntime.constants.OmniWindowFrameBoundType;
import nova.hetu.omniruntime.constants.OmniWindowFrameType;
import nova.hetu.omniruntime.type.BooleanDataType;
import nova.hetu.omniruntime.type.CharDataType;
import nova.hetu.omniruntime.type.ContainerDataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.Date32DataType;
import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.type.Decimal64DataType;
import nova.hetu.omniruntime.type.DoubleDataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.type.VarcharDataType;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.ContainerVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.type.Decimals.MAX_SHORT_PRECISION;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.longBitsToDouble;
import static javassist.bytecode.StackMap.DOUBLE;

/**
 * The type Operator utils.
 *
 * @since 20210630
 */
public final class OperatorUtils
{
    private static final Logger log = Logger.get(OperatorUtils.class);

    private OperatorUtils()
    {
    }

    /**
     * convert type [] to data type [ ].
     *
     * @param types the types
     * @return the data type [ ]
     */
    public static DataType[] toDataTypes(List<? extends Type> types)
    {
        DataType[] dataTypes = types.stream().map(OperatorUtils::toDataType).toArray(DataType[]::new);
        return dataTypes;
    }

    /**
     * convert type to data type.
     *
     * @param type the type
     * @return the data type
     */
    public static DataType toDataType(Type type)
    {
        TypeSignature signature = type.getTypeSignature();
        String base = signature.getBase();

        switch (base) {
            case StandardTypes.INTEGER:
                return IntDataType.INTEGER;
            case StandardTypes.BIGINT:
                return LongDataType.LONG;
            case StandardTypes.DOUBLE:
                return DoubleDataType.DOUBLE;
            case StandardTypes.BOOLEAN:
                return BooleanDataType.BOOLEAN;
            case StandardTypes.VARBINARY:
                return new VarcharDataType(0);
            case StandardTypes.VARCHAR:
                int width = signature.getParameters().get(0).getLongLiteral().intValue();
                return new VarcharDataType(width);
            case StandardTypes.CHAR:
                return new CharDataType(signature.getParameters().get(0).getLongLiteral().intValue());
            case StandardTypes.DECIMAL:
                int precision = signature.getParameters().get(0).getLongLiteral().intValue();
                int scale = signature.getParameters().get(1).getLongLiteral().intValue();
                if (precision <= MAX_SHORT_PRECISION) {
                    return new Decimal64DataType(precision, scale);
                }
                return new Decimal128DataType(precision, scale);
            case StandardTypes.DATE:
                return Date32DataType.DATE32;
            case StandardTypes.ROW:
                RowType rowType = (RowType) type;
                return new ContainerDataType(toDataTypes(rowType.getTypeParameters()));
            default:
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support data Type " + base);
        }
    }

    /**
     * convert hetu WindowFrameType to OmniWindowFrameType.
     *
     * @param type hetu WindowFrameType
     * @return the omni window frame type
     */
    public static OmniWindowFrameType toWindowFrameType(Types.WindowFrameType type)
    {
        switch (type) {
            case RANGE:
                return OmniWindowFrameType.OMNI_FRAME_TYPE_RANGE;
            case ROWS:
                return OmniWindowFrameType.OMNI_FRAME_TYPE_ROWS;
            default:
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support window frame tmype " + type);
        }
    }

    /**
     * convert hetu FrameBoundType to OmniWindowFrameBoundType.
     *
     * @param type hetu frame bound type
     * @return the omni window frame bound type
     */
    public static OmniWindowFrameBoundType toWindowFrameBoundType(Types.FrameBoundType type)
    {
        switch (type) {
            case UNBOUNDED_PRECEDING:
                return OmniWindowFrameBoundType.OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING;
            case PRECEDING:
                return OmniWindowFrameBoundType.OMNI_FRAME_BOUND_PRECEDING;
            case CURRENT_ROW:
                return OmniWindowFrameBoundType.OMNI_FRAME_BOUND_CURRENT_ROW;
            case FOLLOWING:
                return OmniWindowFrameBoundType.OMNI_FRAME_BOUND_FOLLOWING;
            case UNBOUNDED_FOLLOWING:
                return OmniWindowFrameBoundType.OMNI_FRAME_BOUND_UNBOUNDED_FOLLOWING;
            default:
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED,
                        "Not support window frame bound type " + type);
        }
    }

    /**
     * Create expressions string [ ].
     *
     * @param columns the columns
     * @return the string [ ]
     */
    public static String[] createExpressions(List<Integer> columns)
    {
        return createExpressions(Ints.toArray(columns));
    }

    /**
     * Create expressions string [ ].
     *
     * @param columns the columns
     * @return the string [ ]
     */
    public static String[] createExpressions(int[] columns)
    {
        String[] expressions = new String[columns.length];
        for (int i = 0; i < columns.length; i++) {
            expressions[i] = "#" + columns[i];
        }
        return expressions;
    }

    /**
     * Create blank vectors for given size and types.
     *
     * @param vecAllocator VecAllocator used to create vectors
     * @param dataTypes data types
     * @param totalPositions Size for all the vectors
     * @return List contains blank vectors
     */
    public static List<Vec> createBlankVectors(VecAllocator vecAllocator, DataType[] dataTypes, int totalPositions)
    {
        List<Vec> vecsResult = new ArrayList<>();
        for (int i = 0; i < dataTypes.length; i++) {
            DataType type = dataTypes[i];
            switch (type.getId()) {
                case OMNI_INT:
                case OMNI_DATE32:
                    vecsResult.add(new IntVec(vecAllocator, totalPositions));
                    break;
                case OMNI_LONG:
                case OMNI_DECIMAL64:
                    vecsResult.add(new LongVec(vecAllocator, totalPositions));
                    break;
                case OMNI_DOUBLE:
                    vecsResult.add(new DoubleVec(vecAllocator, totalPositions));
                    break;
                case OMNI_BOOLEAN:
                    vecsResult.add(new BooleanVec(vecAllocator, totalPositions));
                    break;
                case OMNI_VARCHAR:
                case OMNI_CHAR:
                    vecsResult.add(new VarcharVec(vecAllocator, totalPositions * ((VarcharDataType) type).getWidth(),
                            totalPositions));
                    break;
                case OMNI_DECIMAL128:
                    vecsResult.add(new Decimal128Vec(vecAllocator, totalPositions));
                    break;
                case OMNI_CONTAINER:
                    vecsResult.add(createBlankContainerVector(vecAllocator, type, totalPositions));
                    break;
                default:
                    throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support data type " + type);
            }
        }
        return vecsResult;
    }

    private static ContainerVec createBlankContainerVector(VecAllocator vecAllocator, DataType type,
                                                           int totalPositions)
    {
        if (!(type instanceof ContainerDataType)) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "type is not container type:" + type);
        }
        ContainerDataType containerDataType = (ContainerDataType) type;
        List<Vec> fieldVecs = createBlankVectors(vecAllocator, containerDataType.getFieldTypes(), totalPositions);
        long[] nativeVec = new long[fieldVecs.size()];
        for (int i = 0; i < fieldVecs.size(); i++) {
            nativeVec[i] = fieldVecs.get(i).getNativeVector();
        }
        return new ContainerVec(vecAllocator, containerDataType.size(), totalPositions, nativeVec,
                containerDataType.getFieldTypes());
    }

    /**
     * Transfer to off heap pages list.
     *
     * @param vecAllocator vector allocator
     * @param pages the pages
     * @return the list
     */
    public static List<Page> transferToOffHeapPages(VecAllocator vecAllocator, List<Page> pages)
    {
        List<Page> offHeapInput = new ArrayList<>();
        for (Page page : pages) {
            Block[] blocks = getOffHeapBlocks(vecAllocator, page.getBlocks(), null);
            offHeapInput.add(new Page(blocks));
        }
        return offHeapInput;
    }

    /**
     * Transfer to off heap pages page.
     *
     * @param vecAllocator vector allocator
     * @param page the page
     * @return the page
     */
    public static Page transferToOffHeapPages(VecAllocator vecAllocator, Page page)
    {
        if (page.getBlocks().length == 0) {
            return page;
        }
        Block[] blocks = getOffHeapBlocks(vecAllocator, page.getBlocks(), null);
        return new Page(blocks);
    }

    /**
     * Transfer to off heap pages page with types.
     *
     * @param vecAllocator vector allocator
     * @param page the page
     * @param blockTypes types
     * @return the page
     */
    public static Page transferToOffHeapPages(VecAllocator vecAllocator, Page page, List<Type> blockTypes)
    {
        if (page.getBlocks().length == 0) {
            return page;
        }
        Block[] blocks = getOffHeapBlocks(vecAllocator, page.getBlocks(), blockTypes);
        return new Page(blocks);
    }

    private static Block[] getOffHeapBlocks(VecAllocator vecAllocator, Block[] blocks, List<Type> blockTypes)
    {
        Block[] res = new Block[blocks.length];
        if (blockTypes == null || blockTypes.isEmpty()) {
            for (int i = 0; i < blocks.length; i++) {
                res[i] = buildOffHeapBlock(vecAllocator, blocks[i]);
            }
        }
        else {
            for (int i = 0; i < blocks.length; i++) {
                res[i] = buildOffHeapBlock(vecAllocator, blocks[i], blocks[i].getClass().getSimpleName(),
                        blocks[i].getPositionCount(), blockTypes.get(i));
            }
        }
        return res;
    }

    /**
     * Gets off heap block.
     *
     * @param vecAllocator vector allocator
     * @param block the block
     * @return the off heap block
     */
    public static Block buildOffHeapBlock(VecAllocator vecAllocator, Block block)
    {
        return buildOffHeapBlock(vecAllocator, block, block.getClass().getSimpleName(), block.getPositionCount(), null);
    }

    private static double[] transformLongArrayToDoubleArray(long[] values)
    {
        double[] doubles = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            doubles[i] = longBitsToDouble(values[i]);
        }
        return doubles;
    }

    public static byte[] transformBooleanToByte(boolean[] values)
    {
        if (values == null) {
            return null;
        }
        byte[] transformedBytes = new byte[values.length];
        for (int i = 0; i < values.length; ++i) {
            if (values[i]) {
                transformedBytes[i] = 1;
            }
        }
        return transformedBytes;
    }

    private static void fillLongArray(long[] val, long[] longs)
    {
        for (int i = 0; i < longs.length; i++) {
            if (i % 2 == 0) {
                longs[i] = val[0];
            }
            else {
                longs[i] = val[1];
            }
        }
    }

    private static Block buildByteArrayOmniBlock(VecAllocator vecAllocator, Block block, int positionCount,
                                                 boolean isRLE)
    {
        if (isRLE) {
            byte[] valueIsNull = null;
            byte[] bytes = new byte[positionCount];
            if (positionCount != 0) {
                if (block.isNull(0)) {
                    valueIsNull = new byte[positionCount];
                    Arrays.fill(valueIsNull, Vec.NULL);
                }
                else {
                    Arrays.fill(bytes, (byte) block.get(0));
                }
            }
            return new ByteArrayOmniBlock(vecAllocator, 0, positionCount, valueIsNull, bytes);
        }
        else {
            boolean[] valueIsNull = block.getValueNulls();
            int offset = block.getBlockOffset();
            byte[] bytes = ((ByteArrayBlock) block).getValues();
            return new ByteArrayOmniBlock(vecAllocator, offset, positionCount, transformBooleanToByte(valueIsNull), bytes);
        }
    }

    private static Block buildIntArrayOmniBLock(VecAllocator vecAllocator, Block block, int positionCount,
                                                boolean isRLE)
    {
        if (isRLE) {
            byte[] valueIsNull = null;
            int[] values = new int[positionCount];
            if (positionCount != 0) {
                if (block.isNull(0)) {
                    valueIsNull = new byte[positionCount];
                    Arrays.fill(valueIsNull, Vec.NULL);
                }
                else {
                    Arrays.fill(values, (int) block.get(0));
                }
            }
            return new IntArrayOmniBlock(vecAllocator, 0, positionCount, valueIsNull, values);
        }
        else {
            boolean[] valueIsNull = block.getValueNulls();
            int offset = block.getBlockOffset();
            int[] values = ((IntArrayBlock) block).getValues();
            return new IntArrayOmniBlock(vecAllocator, offset, positionCount, transformBooleanToByte(valueIsNull), values);
        }
    }

    private static Block buildLongArrayOmniBLock(VecAllocator vecAllocator, Block block, int positionCount,
                                                 boolean isRLE)
    {
        if (isRLE) {
            byte[] valueIsNull = null;
            long[] values = new long[positionCount];
            if (positionCount != 0) {
                if (block.isNull(0)) {
                    valueIsNull = new byte[positionCount];
                    Arrays.fill(valueIsNull, Vec.NULL);
                }
                else {
                    Arrays.fill(values, (long) block.get(0));
                }
            }
            return new LongArrayOmniBlock(vecAllocator, 0, positionCount, valueIsNull, values);
        }
        else {
            boolean[] valueIsNull = block.getValueNulls();
            int offset = block.getBlockOffset();
            long[] values = ((LongArrayBlock) block).getValues();
            return new LongArrayOmniBlock(vecAllocator, offset, positionCount, transformBooleanToByte(valueIsNull), values);
        }
    }

    private static Block buildDoubleArrayOmniBLock(VecAllocator vecAllocator, Block block, int positionCount,
                                                   boolean isRLE)
    {
        if (isRLE) {
            byte[] valueIsNull = null;
            double[] doubles = new double[positionCount];
            if (positionCount != 0) {
                if (block.isNull(0)) {
                    valueIsNull = new byte[positionCount];
                    Arrays.fill(valueIsNull, Vec.NULL);
                }
                else {
                    Arrays.fill(doubles, longBitsToDouble((long) block.get(0)));
                }
            }
            return new DoubleArrayOmniBlock(vecAllocator, 0, positionCount, valueIsNull, doubles);
        }
        else {
            boolean[] valueIsNull = block.getValueNulls();
            int offset = block.getBlockOffset();
            long[] values = ((LongArrayBlock) block).getValues();
            double[] doubles = transformLongArrayToDoubleArray(values);
            return new DoubleArrayOmniBlock(vecAllocator, offset, positionCount, transformBooleanToByte(valueIsNull), doubles);
        }
    }

    private static Block buildInt128ArrayOmniBlock(VecAllocator vecAllocator, Block block, int positionCount,
                                                   boolean isRLE)
    {
        if (isRLE) {
            byte[] valueIsNull = null;
            long[] longs = new long[positionCount * 2];
            if (positionCount != 0) {
                if (block.isNull(0)) {
                    valueIsNull = new byte[positionCount];
                    Arrays.fill(valueIsNull, Vec.NULL);
                }
                else {
                    long[] val = (long[]) block.get(0);
                    fillLongArray(val, longs);
                }
            }
            return new Int128ArrayOmniBlock(vecAllocator, 0, positionCount, valueIsNull, longs);
        }
        else {
            boolean[] valueIsNull = block.getValueNulls();
            int offset = block.getBlockOffset();
            long[] longs = ((Int128ArrayBlock) block).getValues();
            return new Int128ArrayOmniBlock(vecAllocator, offset, positionCount, transformBooleanToByte(valueIsNull), longs);
        }
    }

    private static VariableWidthOmniBlock buildVariableWidthOmniBlock(VecAllocator vecAllocator, Block block, int positionCount,
                                                                      boolean isRLE)
    {
        if (!isRLE) {
            int[] offsets = ((VariableWidthBlock) block).getOffsets();
            int offset = block.getBlockOffset();
            boolean[] valueIsNull = block.getValueNulls();
            Slice slice = ((VariableWidthBlock) block).getRawSlice(0);
            return new VariableWidthOmniBlock(vecAllocator, offset, positionCount, slice, offsets,
                    transformBooleanToByte(valueIsNull));
        }
        else {
            AbstractVariableWidthBlock variableWidthBlock = (AbstractVariableWidthBlock) ((RunLengthEncodedBlock) block)
                    .getValue();
            VarcharVec vec = new VarcharVec(vecAllocator, variableWidthBlock.getSliceLength(0) * positionCount,
                    positionCount);
            for (int i = 0; i < positionCount; i++) {
                if (block.isNull(i)) {
                    vec.setNull(i);
                }
                else {
                    vec.set(i, (byte[]) block.get(i));
                }
            }
            return new VariableWidthOmniBlock(positionCount, vec);
        }
    }

    private static Block buildDictionaryOmniBlock(VecAllocator vecAllocator, Block inputBlock, Type blockType)
    {
        DictionaryBlock dictionaryBlock = (DictionaryBlock) inputBlock;
        Block block = dictionaryBlock.getDictionary();
        Block omniDictionary = buildOffHeapBlock(vecAllocator, block, block.getClass().getSimpleName(),
                block.getPositionCount(), blockType);
        Block dictionaryOmniBlock = new DictionaryOmniBlock(inputBlock.getPositionCount(), (Vec) omniDictionary.getValues(),
                dictionaryBlock.getIdsArray());
        omniDictionary.close();
        return dictionaryOmniBlock;
    }

    private static Block buildRowOmniBlock(VecAllocator vecAllocator, Block block, int positionCount, Type blockType)
    {
        byte[] valueIsNull = new byte[positionCount];
        RowBlock rowBlock = (RowBlock) block;
        for (int j = 0; j < positionCount; j++) {
            if (rowBlock.isNull(j)) {
                valueIsNull[j] = Vec.NULL;
            }
        }
        return RowOmniBlock.fromFieldBlocks(vecAllocator, rowBlock.getPositionCount(), Optional.of(valueIsNull),
                rowBlock.getRawFieldBlocks(), blockType);
    }

    /**
     * Gets off heap block.
     *
     * @param vecAllocator vector allocator
     * @param block the block
     * @param type the actual block type, e.g. RunLengthEncodedBlock or
     * DictionaryBlock
     * @param positionCount the position count of the block
     * @return the off heap block
     */
    public static Block buildOffHeapBlock(VecAllocator vecAllocator, Block block, String type, int positionCount,
                                          Type blockType)
    {
        return buildOffHeapBlock(vecAllocator, block, type, positionCount, false, blockType);
    }

    private static Block buildOffHeapBlock(VecAllocator vecAllocator, Block block, String type, int positionCount, boolean isRLE, Type blockType)
    {
        if (block.isExtensionBlock()) {
            return block;
        }

        switch (type) {
            case "ByteArrayBlock":
                return buildByteArrayOmniBlock(vecAllocator, block, positionCount, isRLE);
            case "IntArrayBlock":
                return buildIntArrayOmniBLock(vecAllocator, block, positionCount, isRLE);
            case "LongArrayBlock":
                if (blockType != null && blockType.equals(DOUBLE)) {
                    return buildDoubleArrayOmniBLock(vecAllocator, block, positionCount, isRLE);
                }
                return buildLongArrayOmniBLock(vecAllocator, block, positionCount, isRLE);
            case "Int128ArrayBlock":
                return buildInt128ArrayOmniBlock(vecAllocator, block, positionCount, isRLE);
            case "VariableWidthBlock":
                return buildVariableWidthOmniBlock(vecAllocator, block, positionCount, isRLE);
            case "DictionaryBlock":
                return buildDictionaryOmniBlock(vecAllocator, block, blockType);
            case "RunLengthEncodedBlock":
                return buildOffHeapBlock(vecAllocator, block,
                        ((RunLengthEncodedBlock) block).getValue().getClass().getSimpleName(), positionCount, true,
                        blockType);
            case "LazyBlock":
                return new LazyOmniBlock(vecAllocator, (LazyBlock) block, blockType);
            case "RowBlock":
                return buildRowOmniBlock(vecAllocator, block, positionCount, blockType);
            default:
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support block:" + type);
        }
    }

    /**
     * Build a vector from block.
     *
     * @param vecAllocator vector allocator.
     * @param block block
     * @return vector instance.
     */
    public static Vec buildVec(VecAllocator vecAllocator, Block block)
    {
        if (!block.isExtensionBlock()) {
            return (Vec) OperatorUtils.buildOffHeapBlock(vecAllocator, block).getValues();
        }
        else {
            return (Vec) block.getValues();
        }
    }

    /**
     * Build a vector by {@link Block}
     *
     * @param vecAllocator VecAllocator to create vectors
     * @param page the page
     * @param object the operator
     * @return the vec batch
     */
    public static VecBatch buildVecBatch(VecAllocator vecAllocator, Page page, Object object)
    {
        List<Vec> vecList = new ArrayList<>();

        for (int i = 0; i < page.getChannelCount(); i++) {
            Block block = page.getBlock(i);
            Vec vec = buildVec(vecAllocator, block);
            vecList.add(vec);
        }

        return new VecBatch(vecList, page.getPositionCount());
    }

    /**
     * This method is used to merge the buffered VecBatches together into a final
     * result VecBatch. It invokes append method defined natively to perform merge
     * operation.
     *
     * @param resultVecBatch Stores final resulting vectors
     */
    public static void merge(VecBatch resultVecBatch, List<Page> pages, VecAllocator vecAllocator)
    {
        for (int channel = 0; channel < resultVecBatch.getVectorCount(); channel++) {
            int offset = 0;
            for (Page page : pages) {
                int positionCount = page.getPositionCount();
                Block block = page.getBlock(channel);
                Vec src;
                if (!block.isExtensionBlock()) {
                    block = OperatorUtils.buildOffHeapBlock(vecAllocator, block);
                }
                src = (Vec) block.getValues();
                Vec dest = resultVecBatch.getVector(channel);
                dest.append(src, offset, positionCount);

                offset += positionCount;
                src.close();
            }
        }
    }

    /**
     * build a RowOmniBlock from a ContainerVec
     *
     * @param containerVec a container vector
     * @return the RowOmniBlock
     */
    public static RowOmniBlock buildRowOmniBlock(ContainerVec containerVec)
    {
        DataType[] dataTypes = containerVec.getDataTypes();
        int positionCount = containerVec.getPositionCount();
        Block[] rowBlocks = new Block[dataTypes.length];
        int vectorCount = containerVec.getDataTypes().length;
        for (int vecIdx = 0; vecIdx < vectorCount; ++vecIdx) {
            DataType dataType = dataTypes[vecIdx];
            switch (dataType.getId()) {
                case OMNI_BOOLEAN:
                    rowBlocks[vecIdx] = new ByteArrayOmniBlock(positionCount,
                            new BooleanVec(containerVec.getVector(vecIdx)));
                    break;
                case OMNI_INT:
                case OMNI_DATE32:
                    rowBlocks[vecIdx] = new IntArrayOmniBlock(positionCount,
                            new IntVec(containerVec.getVector(vecIdx)));
                    break;
                case OMNI_LONG:
                case OMNI_DECIMAL64:
                    rowBlocks[vecIdx] = new LongArrayOmniBlock(positionCount,
                            new LongVec(containerVec.getVector(vecIdx)));
                    break;
                case OMNI_DOUBLE:
                    rowBlocks[vecIdx] = new DoubleArrayOmniBlock(positionCount,
                            new DoubleVec(containerVec.getVector(vecIdx)));
                    break;
                case OMNI_VARCHAR:
                case OMNI_CHAR:
                    rowBlocks[vecIdx] = new VariableWidthOmniBlock(positionCount,
                            new VarcharVec(containerVec.getVector(vecIdx)));
                    break;
                case OMNI_DECIMAL128:
                    rowBlocks[vecIdx] = new Int128ArrayOmniBlock(positionCount,
                            new Decimal128Vec(containerVec.getVector(vecIdx), dataType));
                    break;
                default:
                    throw new PrestoException(GENERIC_INTERNAL_ERROR,
                            "Unsupported data type " + dataTypes[vecIdx].getId());
            }
        }
        int[] fieldBlockOffsets = new int[positionCount + 1];
        byte[] nulls = containerVec.getRawValueNulls();
        for (int position = 0; position < positionCount; position++) {
            fieldBlockOffsets[position + 1] = fieldBlockOffsets[position] + (nulls[position] == Vec.NULL ? 0 : 1);
        }
        return new RowOmniBlock(0, positionCount, nulls, fieldBlockOffsets, rowBlocks,
                new ContainerDataType(dataTypes));
    }

    /**
     * Transfer to on heap pages list.
     *
     * @param pages the the off heap pages
     * @return the on heap page list
     */
    public static List<Page> transferToOnHeapPages(List<Page> pages)
    {
        List<Page> onHeapPages = new ArrayList<>();
        for (Page page : pages) {
            Block[] blocks = getOnHeapBlocks(page.getBlocks());
            onHeapPages.add(new Page(blocks));
        }
        return onHeapPages;
    }

    /**
     * Transfer to on heap pages page.
     *
     * @param page the off heap page
     * @return the on heap page
     */
    public static Page transferToOnHeapPage(Page page)
    {
        if (page.getBlocks().length == 0) {
            return page;
        }
        Block[] blocks = getOnHeapBlocks(page.getBlocks());
        return new Page(blocks);
    }

    private static Block[] getOnHeapBlocks(Block[] blocks)
    {
        Block[] res = new Block[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            res[i] = buildOnHeapBlock(blocks[i]);
        }
        return res;
    }

    /**
     * Gets on heap block.
     *
     * @param block the off heap block
     * @return the on heap block
     */
    public static Block buildOnHeapBlock(Block block)
    {
        return buildOnHeapBlock(block, block.getClass().getSimpleName(), block.getPositionCount());
    }

    private static Block buildOnHeapBlock(Block block, String type, int positionCount)
    {
        checkArgument(block.isExtensionBlock(), "block should be omni block!");
        switch (type) {
            case "ByteArrayOmniBlock":
                return buildByteArrayBlock(block, positionCount);
            case "IntArrayOmniBlock":
                return buildIntArrayBLock(block, positionCount);
            case "LongArrayOmniBlock":
                return buildLongArrayBLock(block, positionCount);
            case "DoubleArrayOmniBlock":
                return buildDoubleArrayBLock(block, positionCount);
            case "Int128ArrayOmniBlock":
                return buildInt128ArrayBlock(block, positionCount);
            case "VariableWidthOmniBlock":
                return buildVariableWidthBlock(block, positionCount);
            case "DictionaryOmniBlock":
                return buildDictionaryBlock(block, positionCount);
            case "LazyOmniBlock":
                return buildLazyBlock(block);
            case "RowOmniBlock":
                return buildRowBlock(block, positionCount);
            default:
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support block:" + type);
        }
    }

    private static Block buildRowBlock(Block block, int positionCount)
    {
        boolean[] rowIsNull = ((RowOmniBlock) block).getRowIsNull();
        Block[] rawFieldBlocks = ((RowOmniBlock<?>) block).getRawFieldBlocks();
        Block[] newFieldBlocks = new Block[rawFieldBlocks.length];
        for (int blockIndex = 0; blockIndex < rawFieldBlocks.length; ++blockIndex) {
            newFieldBlocks[blockIndex] = buildOnHeapBlock(rawFieldBlocks[blockIndex]);
        }
        return RowBlock.fromFieldBlocks(positionCount, Optional.of(rowIsNull), newFieldBlocks);
    }

    private static Block buildLazyBlock(Block block)
    {
        return ((LazyOmniBlock) block).getLazyBlock();
    }

    private static Block buildDictionaryBlock(Block block, int positionCount)
    {
        DictionaryVec dictionaryVec = (DictionaryVec) block.getValues();
        int[] newIds = dictionaryVec.getIds(positionCount);
        Block dictionary = buildOnHeapBlock(((DictionaryOmniBlock) block).getDictionary());
        return new DictionaryBlock(dictionary, newIds);
    }

    private static Block buildVariableWidthBlock(Block block, int positionCount)
    {
        VarcharVec varcharVec = (VarcharVec) block.getValues();
        int[] offsets = varcharVec.getValueOffset(0, positionCount);
        int startOffset = varcharVec.getValueOffset(0);
        int endOffset = varcharVec.getValueOffset(positionCount);
        byte[] data = varcharVec.getData(startOffset, endOffset - startOffset);
        Slice slice = Slices.wrappedBuffer(data);
        return new VariableWidthBlock(positionCount, slice, offsets,
                varcharVec.hasNullValue()
                        ? Optional.of(varcharVec.getValuesNulls(0, positionCount))
                        : Optional.empty());
    }

    private static Block buildInt128ArrayBlock(Block block, int positionCount)
    {
        Decimal128Vec decimal128Vec = (Decimal128Vec) block.getValues();
        return new Int128ArrayBlock(positionCount, Optional.of(decimal128Vec.getValuesNulls(0, positionCount)),
                decimal128Vec.get(0, positionCount));
    }

    private static Block buildDoubleArrayBLock(Block block, int positionCount)
    {
        DoubleVec doubleVec = (DoubleVec) block.getValues();
        boolean[] valuesNulls = doubleVec.getValuesNulls(0, positionCount);
        long[] values = new long[positionCount];
        for (int j = 0; j < positionCount; j++) {
            if (!block.isNull(j)) {
                values[j] = doubleToLongBits(doubleVec.get(j));
            }
        }
        return new LongArrayBlock(positionCount, Optional.of(valuesNulls), values);
    }

    private static Block buildLongArrayBLock(Block block, int positionCount)
    {
        LongVec longVec = (LongVec) block.getValues();
        return new LongArrayBlock(positionCount, Optional.of(longVec.getValuesNulls(0, positionCount)),
                longVec.get(0, positionCount));
    }

    private static Block buildIntArrayBLock(Block block, int positionCount)
    {
        IntVec intVec = (IntVec) block.getValues();
        return new IntArrayBlock(positionCount, Optional.of(intVec.getValuesNulls(0, positionCount)),
                intVec.get(0, positionCount));
    }

    private static Block buildByteArrayBlock(Block block, int positionCount)
    {
        BooleanVec booleanVec = (BooleanVec) block.getValues();
        byte[] bytes = booleanVec.getValuesBuf().getBytes(booleanVec.getOffset(), positionCount);
        return new ByteArrayBlock(positionCount, Optional.of(booleanVec.getValuesNulls(0, positionCount)), bytes);
    }
}
