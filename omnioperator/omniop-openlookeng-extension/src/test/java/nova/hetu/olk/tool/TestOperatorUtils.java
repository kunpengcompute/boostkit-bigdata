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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.block.RowBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.block.ByteArrayOmniBlock;
import nova.hetu.olk.block.DictionaryOmniBlock;
import nova.hetu.olk.block.DoubleArrayOmniBlock;
import nova.hetu.olk.block.Int128ArrayOmniBlock;
import nova.hetu.olk.block.IntArrayOmniBlock;
import nova.hetu.olk.block.LazyOmniBlock;
import nova.hetu.olk.block.LongArrayOmniBlock;
import nova.hetu.olk.block.RowOmniBlock;
import nova.hetu.olk.block.VariableWidthOmniBlock;
import nova.hetu.olk.operator.benchmark.PageBuilderUtil;
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
import org.junit.runner.RunWith;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static nova.hetu.olk.tool.OperatorUtils.transferToOffHeapPages;
import static nova.hetu.olk.tool.OperatorUtils.transferToOnHeapPage;
import static nova.hetu.olk.tool.OperatorUtils.transferToOnHeapPages;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;
import static org.testng.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({VecAllocator.class,
        Vec.class,
        OperatorUtils.class
})
@SuppressStaticInitializationFor({"nova.hetu.omniruntime.vector.VecAllocator",
        "nova.hetu.omniruntime.vector.Vec"
})
@PowerMockIgnore("javax.management.*")
public class TestOperatorUtils
        extends PowerMockTestCase
{
    List<Type> types = new ImmutableList.Builder<Type>().add(INTEGER).add(BIGINT).add(REAL).add(DOUBLE).add(VARCHAR)
            .add(DATE).add(TIMESTAMP).add(BOOLEAN).add(createDecimalType(20, 10)).add(createDecimalType(10, 10)).build();
    LazyOmniBlock lazyOmniBlock;
    RowOmniBlock rowOmniBlock;
    IntVec intVec;
    LongVec longVec;
    DoubleVec doubleVec;
    BooleanVec booleanVec;
    VarcharVec varcharVec;
    Decimal128Vec decimal128Vec;
    ContainerVec containerVec;
    DictionaryVec dictionaryVec;

    @BeforeMethod
    public void setUp() throws Exception
    {
        mockSupports();
    }

    @Test
    public void testBasicTransfer()
    {
        List<Page> pages = buildPages(types, false, 100);
        // transfer on-feap page to off-heap
        List<Page> offHeapPages = transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, pages);
        // transfer off-heap page to on-heap
        List<Page> onHeapPages = transferToOnHeapPages(offHeapPages);
        freeNativeMemory(offHeapPages);
    }

    @Test
    public void testDictionayTransfer()
    {
        List<Page> pages = buildPages(types, true, 100);
        // transfer on-feap page to off-heap
        List<Page> offHeapPages = transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, pages);
        // transfer off-heap page to on-heap
        List<Page> onHeapPages = transferToOnHeapPages(offHeapPages);
        freeNativeMemory(offHeapPages);
    }

    @Test
    public void testRowBlockTransfer()
    {
        Type type = BIGINT;
        Page page = new Page(buildRowBlockByBuilder(type));
        // transfer on-heap page to off-heap
        Page offHeapPage = transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, page,
                ImmutableList.of(RowType.from(ImmutableList.of(RowType.field(type)))));
        // transfer off-heap page to on-heap
        Page onHeapPage = transferToOnHeapPage(offHeapPage);
        BlockUtils.freePage(offHeapPage);
    }

    @Test
    public void testRunLengthEncodedBlockTransfer()
    {
        List<Page> pages = buildPages(types, false, 1);
        List<Page> runLengthEncodedPages = new ArrayList<>();
        for (int i = 0; i < pages.size(); i++) {
            Page page = pages.get(i);
            List<Block> blocks = new ArrayList<>();
            for (int j = 0; j < page.getBlocks().length; j++) {
                blocks.add(new RunLengthEncodedBlock(page.getBlock(j), 1));
            }
            blocks.add(new DoubleArrayOmniBlock(1, new DoubleVec(1)));
            blocks.add(lazyOmniBlock);
            runLengthEncodedPages.add(new Page(blocks.toArray(new Block[blocks.size()])));
        }
        // transfer on-heap page to off-heap
        List<Page> offHeapPages = transferToOffHeapPages(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, runLengthEncodedPages);
        // transfer off-heap page to on-heap
        List<Page> onHeapPages = transferToOnHeapPages(offHeapPages);
        freeNativeMemory(offHeapPages);
    }

    @Test
    public void testBlockTypeTransfer()
    {
        Page page = buildPages(new ImmutableList.Builder<Type>().add(DOUBLE).build(), false, 1).get(0);
        Block block = OperatorUtils.buildOffHeapBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, page.getBlock(0), "LongArrayBlock", 1, DOUBLE);
        OperatorUtils.buildOffHeapBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, new RunLengthEncodedBlock(page.getBlock(0), 1), "RunLengthEncodedBlock", 1, DOUBLE);
    }

    @Test
    public void testDataType()
    {
        assertEquals(IntDataType.INTEGER, OperatorUtils.toDataType(INTEGER));
        assertEquals(LongDataType.LONG, OperatorUtils.toDataType(BIGINT));
        assertEquals(DoubleDataType.DOUBLE, OperatorUtils.toDataType(DOUBLE));
        assertEquals(BooleanDataType.BOOLEAN, OperatorUtils.toDataType(BOOLEAN));
        assertEquals(new VarcharDataType(0), OperatorUtils.toDataType(VARBINARY));
        assertEquals(new VarcharDataType(Integer.MAX_VALUE), OperatorUtils.toDataType(VARCHAR));
        assertEquals(new CharDataType(100), OperatorUtils.toDataType(CharType.createCharType(100)));
        assertEquals(new Decimal128DataType(20, 10), OperatorUtils.toDataType(createDecimalType(20, 10)));
        assertEquals(new Decimal64DataType(10, 10), OperatorUtils.toDataType(createDecimalType(10, 10)));
        assertEquals(Date32DataType.DATE32, OperatorUtils.toDataType(DATE));
        assertEquals(new ContainerDataType(new DataType[]{DoubleDataType.DOUBLE}), OperatorUtils.toDataType(RowType.anonymous(Arrays.asList(DOUBLE))));
    }

    @Test
    public void testExpression()
    {
        List<Integer> columns = Arrays.asList(1, 2, 3);
        String[] strings = new String[]{"#1", "#2", "#3"};
        assertEquals(strings, OperatorUtils.createExpressions(columns));
    }

    @Test
    public void testCreateBlankVectors()
    {
        DataType[] dataTypes = new DataType[]{IntDataType.INTEGER, Date32DataType.DATE32, LongDataType.LONG,
                Decimal64DataType.DECIMAL64, DoubleDataType.DOUBLE, BooleanDataType.BOOLEAN, VarcharDataType.VARCHAR,
                CharDataType.CHAR, Decimal128DataType.DECIMAL128, new ContainerDataType(new DataType[]{IntDataType.INTEGER})};
        List<Vec> vecs = new ArrayList<>();
        vecs.add(intVec);
        vecs.add(intVec);
        vecs.add(longVec);
        vecs.add(longVec);
        vecs.add(doubleVec);
        vecs.add(booleanVec);
        vecs.add(varcharVec);
        vecs.add(varcharVec);
        vecs.add(decimal128Vec);
        vecs.add(containerVec);
        assertEquals(vecs, OperatorUtils.createBlankVectors(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, dataTypes, 1));
    }

    @Test
    public void testRowOmniBlock()
    {
        assertEquals(rowOmniBlock, OperatorUtils.buildRowOmniBlock(containerVec));
    }

    private void freeNativeMemory(List<Page> offHeapPages)
    {
        for (Page page : offHeapPages) {
            BlockUtils.freePage(page);
        }
    }

    private List<Page> buildPages(List<Type> types, boolean dictionaryBlocks, int rows)
    {
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            if (dictionaryBlocks) {
                pages.add(PageBuilderUtil.createSequencePageWithDictionaryBlocks(types, rows));
            }
            else {
                pages.add(PageBuilderUtil.createSequencePage(types, rows));
            }
        }
        return pages;
    }

    private Block buildRowBlockByBuilder(Type type)
    {
        BlockBuilder rowBlockBuilder = type.createBlockBuilder(null, 4);
        type.writeLong(rowBlockBuilder, 1);
        type.writeLong(rowBlockBuilder, 10);
        type.writeLong(rowBlockBuilder, 100);
        type.writeLong(rowBlockBuilder, 1000);
        Block block = rowBlockBuilder.build();

        boolean[] rowIsNull = new boolean[1];
        int[] fieldBlockOffsets = {0, 1};
        Block[] blocks = new Block[1];
        blocks[0] = block;
        return RowBlock.fromFieldBlocks(1, Optional.of(rowIsNull), blocks);
    }

    private void mockSupports() throws Exception
    {
        //mock GLOBAL_VECTOR_ALLOCATOR
        VecAllocator vecAllocator = mock(VecAllocator.class);
        MemberModifier.field(VecAllocator.class, "GLOBAL_VECTOR_ALLOCATOR").set(VecAllocator.class, vecAllocator);

        ByteArrayOmniBlock byteArrayOmniBlock = mock(ByteArrayOmniBlock.class);
        when(byteArrayOmniBlock.isExtensionBlock()).thenReturn(true);
        when(byteArrayOmniBlock.getPositionCount()).thenReturn(1);
        whenNew(ByteArrayOmniBlock.class).withAnyArguments().thenReturn(byteArrayOmniBlock);
        booleanVec = mock(BooleanVec.class, RETURNS_DEEP_STUBS);
        when(booleanVec.getValuesBuf().getBytes(anyInt(), anyInt())).thenReturn(new byte[]{1});
        when(booleanVec.getOffset()).thenReturn(0);
        when(booleanVec.getValuesNulls(anyInt(), anyInt())).thenReturn(new boolean[]{true});
        whenNew(BooleanVec.class).withAnyArguments().thenReturn(booleanVec);
        when(byteArrayOmniBlock.getValues()).thenReturn(booleanVec);

        IntArrayOmniBlock intArrayOmniBlock = mock(IntArrayOmniBlock.class);
        when(intArrayOmniBlock.isExtensionBlock()).thenReturn(true);
        when(intArrayOmniBlock.getPositionCount()).thenReturn(1);
        whenNew(IntArrayOmniBlock.class).withAnyArguments().thenReturn(intArrayOmniBlock);
        intVec = mock(IntVec.class);
        when(intVec.get(anyInt(), anyInt())).thenReturn(new int[]{1});
        when(intVec.getValuesNulls(anyInt(), anyInt())).thenReturn(new boolean[]{true});
        whenNew(IntVec.class).withAnyArguments().thenReturn(intVec);
        when(intArrayOmniBlock.getValues()).thenReturn(intVec);

        DoubleArrayOmniBlock doubleArrayOmniBlock = mock(DoubleArrayOmniBlock.class);
        when(doubleArrayOmniBlock.isExtensionBlock()).thenReturn(true);
        when(doubleArrayOmniBlock.getPositionCount()).thenReturn(1);
        whenNew(DoubleArrayOmniBlock.class).withAnyArguments().thenReturn(doubleArrayOmniBlock);
        doubleVec = mock(DoubleVec.class);
        when(doubleVec.get(anyInt())).thenReturn(1d);
        when(doubleVec.getValuesNulls(anyInt(), anyInt())).thenReturn(new boolean[]{true});
        whenNew(DoubleVec.class).withAnyArguments().thenReturn(doubleVec);
        when(doubleArrayOmniBlock.getValues()).thenReturn(doubleVec);

        LongArrayOmniBlock longArrayOmniBlock = mock(LongArrayOmniBlock.class);
        when(longArrayOmniBlock.isExtensionBlock()).thenReturn(true);
        when(longArrayOmniBlock.getPositionCount()).thenReturn(1);
        whenNew(LongArrayOmniBlock.class).withAnyArguments().thenReturn(longArrayOmniBlock);
        longVec = mock(LongVec.class);
        when(longVec.get(anyInt(), anyInt())).thenReturn(new long[]{1L});
        when(longVec.getValuesNulls(anyInt(), anyInt())).thenReturn(new boolean[]{true});
        whenNew(LongVec.class).withAnyArguments().thenReturn(longVec);
        when(longArrayOmniBlock.getValues()).thenReturn(longVec);

        Int128ArrayOmniBlock int128ArrayOmniBlock = mock(Int128ArrayOmniBlock.class);
        when(int128ArrayOmniBlock.isExtensionBlock()).thenReturn(true);
        when(int128ArrayOmniBlock.getPositionCount()).thenReturn(1);
        whenNew(Int128ArrayOmniBlock.class).withAnyArguments().thenReturn(int128ArrayOmniBlock);
        decimal128Vec = mock(Decimal128Vec.class);
        when(decimal128Vec.get(anyInt(), anyInt())).thenReturn(new long[]{0L, 1L});
        when(decimal128Vec.getValuesNulls(anyInt(), anyInt())).thenReturn(new boolean[]{true});
        whenNew(Decimal128Vec.class).withAnyArguments().thenReturn(decimal128Vec);
        when(int128ArrayOmniBlock.getValues()).thenReturn(decimal128Vec);

        VariableWidthOmniBlock variableWidthOmniBlock = mock(VariableWidthOmniBlock.class);
        when(variableWidthOmniBlock.isExtensionBlock()).thenReturn(true);
        when(variableWidthOmniBlock.getPositionCount()).thenReturn(1);
        whenNew(VariableWidthOmniBlock.class).withAnyArguments().thenReturn(variableWidthOmniBlock);
        varcharVec = mock(VarcharVec.class);
        when(varcharVec.hasNullValue()).thenReturn(false);
        when(varcharVec.getValuesNulls(anyInt(), anyInt())).thenReturn(new boolean[]{true});
        when(varcharVec.getValueOffset(anyInt())).thenAnswer(n -> n.getArguments()[0]);
        when(varcharVec.getValueOffset(anyInt(), anyInt())).thenReturn(new int[]{0, 1});
        when(varcharVec.getData(anyInt(), anyInt())).thenReturn(new byte[]{1});
        whenNew(VarcharVec.class).withAnyArguments().thenReturn(varcharVec);
        when(variableWidthOmniBlock.getValues()).thenReturn(varcharVec);

        DictionaryOmniBlock dictionaryOmniBlock = mock(DictionaryOmniBlock.class);
        when(dictionaryOmniBlock.isExtensionBlock()).thenReturn(true);
        when(dictionaryOmniBlock.getPositionCount()).thenReturn(1);
        when(dictionaryOmniBlock.getDictionary()).thenReturn(byteArrayOmniBlock);
        whenNew(DictionaryOmniBlock.class).withAnyArguments().thenReturn(dictionaryOmniBlock);
        dictionaryVec = mock(DictionaryVec.class);
        when(dictionaryVec.getIds(anyInt())).thenReturn(new int[]{1});
        when(dictionaryVec.getValuesNulls(anyInt(), anyInt())).thenReturn(new boolean[]{true});
        whenNew(DictionaryVec.class).withAnyArguments().thenReturn(dictionaryVec);
        when(dictionaryOmniBlock.getValues()).thenReturn(dictionaryVec);

        lazyOmniBlock = mock(LazyOmniBlock.class);
        when(lazyOmniBlock.isExtensionBlock()).thenReturn(true);
        when(lazyOmniBlock.getLazyBlock()).thenReturn(new LazyBlock(1, lazyBlock -> {}));
        whenNew(LazyOmniBlock.class).withAnyArguments().thenReturn(lazyOmniBlock);

        rowOmniBlock = mock(RowOmniBlock.class);
        when(rowOmniBlock.isExtensionBlock()).thenReturn(true);
        when(rowOmniBlock.getPositionCount()).thenReturn(1);
        when(rowOmniBlock.getRowIsNull()).thenReturn(new boolean[]{false});
        when(rowOmniBlock.getRawFieldBlocks()).thenReturn(new Block[]{buildRowBlockByBuilder(BIGINT)});
        whenNew(RowOmniBlock.class).withAnyArguments().thenReturn(rowOmniBlock);

        containerVec = mock(ContainerVec.class);
        when(containerVec.getDataTypes()).thenReturn(new DataType[]{IntDataType.INTEGER, Date32DataType.DATE32, LongDataType.LONG,
                Decimal64DataType.DECIMAL64, DoubleDataType.DOUBLE, BooleanDataType.BOOLEAN, VarcharDataType.VARCHAR,
                CharDataType.CHAR, Decimal128DataType.DECIMAL128});
        whenNew(ContainerVec.class).withAnyArguments().thenReturn(containerVec);
    }
}
