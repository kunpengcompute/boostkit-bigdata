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

package nova.hetu.olk.mock;

import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LazyBlock;
import nova.hetu.olk.block.ByteArrayOmniBlock;
import nova.hetu.olk.block.DictionaryOmniBlock;
import nova.hetu.olk.block.DoubleArrayOmniBlock;
import nova.hetu.olk.block.Int128ArrayOmniBlock;
import nova.hetu.olk.block.IntArrayOmniBlock;
import nova.hetu.olk.block.LazyOmniBlock;
import nova.hetu.olk.block.LongArrayOmniBlock;
import nova.hetu.olk.block.RowOmniBlock;
import nova.hetu.olk.block.ShortArrayOmniBlock;
import nova.hetu.olk.block.VariableWidthOmniBlock;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.ShortVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static io.prestosql.spi.block.DictionaryId.randomDictionaryId;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

public class MockUtil
{
    private MockUtil()
    {
    }

    public static class BlockModel
    {
        boolean rowBlock;
        boolean dictionary;
        boolean lazy;
        Object[] values;
    }

    public static BlockModel block(boolean lazy, boolean dictionary, Object[] object)
    {
        BlockModel blockModel = new BlockModel();
        blockModel.values = object;
        blockModel.dictionary = dictionary;
        blockModel.lazy = lazy;
        return blockModel;
    }

    public static BlockModel rowBlock(boolean lazy, boolean dictionary, Object[] object)
    {
        BlockModel blockModel = new BlockModel();
        blockModel.values = object;
        blockModel.rowBlock = true;
        blockModel.dictionary = dictionary;
        blockModel.lazy = lazy;
        return blockModel;
    }

    public static <T> T[] fill(T[] container, Function<Integer, T> creator)
    {
        for (int i = 0; i < container.length; i++) {
            container[i] = creator.apply(i);
        }
        return container;
    }

    public static Page mockPage(BlockModel... blockModels)
    {
        Map<Integer, Block<?>> blocks = new HashMap<>();
        VecAllocator vecAllocator = mock(VecAllocator.class);
        for (int j = 0; j < blockModels.length; j++) {
            BlockModel blockModel = blockModels[j];
            if (blockModel.rowBlock) {
                for (Object value : blockModel.values) {
                    Page page = mockPage(block(blockModel.lazy, blockModel.dictionary, (Object[]) value));
                    blocks.put(j, RowOmniBlock.fromFieldBlocks(vecAllocator, page.getPositionCount(), Optional.empty(), page.getBlocks(), null));
                }
            }

            Block block = null;
            Vec vec = null;
            if (blockModel.values instanceof Boolean[]) {
                vec = mockVec(BooleanVec.class, blockModel.values, vecAllocator);
                block = new ByteArrayOmniBlock(blockModel.values.length, (BooleanVec) vec);
            }
            if (blockModel.values instanceof Integer[]) {
                vec = mockVec(IntVec.class, blockModel.values, vecAllocator);
                block = new IntArrayOmniBlock(blockModel.values.length, (IntVec) vec);
            }
            if (blockModel.values instanceof Double[]) {
                vec = mockVec(DoubleVec.class, blockModel.values, vecAllocator);
                block = new DoubleArrayOmniBlock(blockModel.values.length, (DoubleVec) vec);
            }
            if (blockModel.values instanceof Short[]) {
                vec = mockVec(ShortVec.class, blockModel.values, vecAllocator);
                block = new ShortArrayOmniBlock(blockModel.values.length, (ShortVec) vec);
            }
            if (blockModel.values instanceof Long[]) {
                vec = mockVec(LongVec.class, blockModel.values, vecAllocator);
                block = new LongArrayOmniBlock(blockModel.values.length, (LongVec) vec);
            }
            if (blockModel.values instanceof Long[][]) {
                vec = mockVec(Decimal128Vec.class, blockModel.values, vecAllocator);
                doAnswer(invocationOnMock -> {
                    Long[] result = (Long[]) blockModel.values[(int) invocationOnMock.getArguments()[0]];

                    long[] toReturn = new long[result.length];
                    for (int i = 0; i < result.length; i++) {
                        toReturn[i] = result[i];
                    }
                    return toReturn;
                }).when((Decimal128Vec) vec).get(anyInt());
                block = new Int128ArrayOmniBlock(blockModel.values.length, (Decimal128Vec) vec);
            }
            if (blockModel.values instanceof String[]) {
                vec = mockVec(VarcharVec.class, blockModel.values, vecAllocator);
                int[] offsets = new int[blockModel.values.length + 1];
                int startPosition = 0;
                for (int i = 0; i < blockModel.values.length; i++) {
                    offsets[i + 1] = startPosition;
                    startPosition += ((String[]) blockModel.values)[i].length();
                }
                when(((VarcharVec) vec).getRawValueOffset()).thenReturn(offsets);
                block = new VariableWidthOmniBlock(blockModel.values.length, (VarcharVec) vec);
            }

            if (block != null) {
                if (blockModel.dictionary) {
                    DictionaryVec dictionaryVec = mock(DictionaryVec.class);
                    int[] idIndex = new int[blockModel.values.length];
                    for (int i = 0; i < blockModel.values.length; i++) {
                        idIndex[i] = i;
                    }
                    when(dictionaryVec.getIds()).thenReturn(idIndex);
                    when(dictionaryVec.getSize()).thenReturn(blockModel.values.length);
                    when(dictionaryVec.getDictionary()).thenReturn(vec);
                    when(dictionaryVec.slice(anyInt(), anyInt())).thenReturn(dictionaryVec);
                    when(dictionaryVec.getAllocator()).thenReturn(vecAllocator);
                    blocks.put(j, new DictionaryOmniBlock(0, blockModel.values.length, dictionaryVec, idIndex, false, randomDictionaryId()));
                }
                else {
                    blocks.put(j, block);
                }
            }
        }
        return blocks.size() == 0 ? null : new Page(blocks.entrySet().stream().map(entry -> {
            if (blockModels[entry.getKey()].lazy) {
                LazyBlock lazyBlock = new LazyBlock(entry.getValue().getPositionCount(), instance -> {});
                lazyBlock.setBlock(entry.getValue());
                return new LazyOmniBlock(vecAllocator, lazyBlock, null);
            }
            else {
                return entry.getValue();
            }
        }).toArray(Block[]::new));
    }

    public static Block<?> mockBlock(boolean lazy, boolean dictionary, VecAllocator vecAllocator, Object[] object)
    {
        Block<?> block = null;
        Vec vec = null;
        if (object instanceof Boolean[]) {
            vec = mockVec(BooleanVec.class, object, vecAllocator);
            when(((BooleanVec) vec).get(anyInt())).thenAnswer(invocationOnMock -> object[(int) invocationOnMock.getArguments()[0]]);
            block = new ByteArrayOmniBlock(object.length, (BooleanVec) vec);
        }
        if (object instanceof Integer[]) {
            vec = mockVec(IntVec.class, object, vecAllocator);
            when(((IntVec) vec).get(anyInt())).thenAnswer(invocationOnMock -> object[(int) invocationOnMock.getArguments()[0]]);
            block = new IntArrayOmniBlock(object.length, (IntVec) vec);
        }
        if (object instanceof Double[]) {
            vec = mockVec(DoubleVec.class, object, vecAllocator);
            when(((DoubleVec) vec).get(anyInt())).thenAnswer(invocationOnMock -> object[(int) invocationOnMock.getArguments()[0]]);
            block = new DoubleArrayOmniBlock(object.length, (DoubleVec) vec);
        }
        if (object instanceof Short[]) {
            vec = mockVec(ShortVec.class, object, vecAllocator);
            when(((ShortVec) vec).get(anyInt())).thenAnswer(invocationOnMock -> object[(int) invocationOnMock.getArguments()[0]]);
            block = new ShortArrayOmniBlock(object.length, (ShortVec) vec);
        }
        if (object instanceof Long[]) {
            vec = mockVec(LongVec.class, object, vecAllocator);
            when(((LongVec) vec).get(anyInt())).thenAnswer(invocationOnMock -> object[(int) invocationOnMock.getArguments()[0]]);
            block = new LongArrayOmniBlock(object.length, (LongVec) vec);
        }
        if (object instanceof Long[][]) {
            vec = mockVec(Decimal128Vec.class, object, vecAllocator);
            doAnswer(invocationOnMock -> {
                Long[] result = (Long[]) object[(int) invocationOnMock.getArguments()[0]];
                long[] toReturn = new long[result.length];
                for (int i = 0; i < result.length; i++) {
                    toReturn[i] = result[i];
                }
                return toReturn;
            }).when((Decimal128Vec) vec).get(anyInt());
            block = new Int128ArrayOmniBlock(object.length, (Decimal128Vec) vec);
        }
        if (object instanceof String[]) {
            vec = mockVec(VarcharVec.class, object, vecAllocator);
            int[] offsets = new int[object.length + 1];
            int startPosition = 0;
            for (int i = 0; i < object.length; i++) {
                offsets[i + 1] = startPosition;
                startPosition += ((String[]) object)[i].length();
            }
            when(((VarcharVec) vec).getRawValueOffset()).thenReturn(offsets);
            when(((VarcharVec) vec).get(anyInt())).thenAnswer(invocationOnMock -> object[(int) invocationOnMock.getArguments()[0]]);
            block = new VariableWidthOmniBlock(object.length, (VarcharVec) vec);
        }
        if (block != null) {
            if (dictionary) {
                DictionaryVec dictionaryVec = mock(DictionaryVec.class);
                int[] idIndex = new int[object.length];
                for (int i = 0; i < object.length; i++) {
                    idIndex[i] = i;
                }
                when(dictionaryVec.getIds()).thenReturn(idIndex);
                when(dictionaryVec.getSize()).thenReturn(object.length);
                when(dictionaryVec.getDictionary()).thenReturn(vec);
                when(dictionaryVec.slice(anyInt(), anyInt())).thenReturn(dictionaryVec);
                when(dictionaryVec.getAllocator()).thenReturn(vecAllocator);
                block = new DictionaryOmniBlock<Block<?>>(0, object.length, dictionaryVec, idIndex, false, randomDictionaryId());
            }
            if (lazy) {
                LazyBlock<?> lazyBlock = new LazyBlock<Block<?>>(block.getPositionCount(), instance -> {});
                lazyBlock.setBlock(block);
                return new LazyOmniBlock<Block<?>>(vecAllocator, lazyBlock, null);
            }
            return block;
        }
        return null;
    }

    public static <T> T mockNewWithWithAnyArguments(Class<T> target)
    {
        T instance = mock(target);

        try {
            whenNew(target).withAnyArguments().thenReturn(instance);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return instance;
    }

    public static <T extends Vec> T mockVec(Class<T> vecClass, Object[] values, VecAllocator vecAllocator)
    {
        T vec = mock(vecClass);
        when(vec.getSize()).thenReturn(values.length);
        when(vec.slice(anyInt(), anyInt())).thenReturn(vec);
        when(vec.getAllocator()).thenReturn(vecAllocator);
        when(vec.copyPositions(any(), anyInt(), anyInt())).thenReturn(vec);
        when(vec.slice(anyInt(), anyInt())).thenReturn(vec);
        return vec;
    }

    public static OmniOperator mockOmniOperator()
    {
        OmniOperator omniOperator = mock(OmniOperator.class);
        List<VecBatch> innerVec = new ArrayList<>();
        doAnswer(invocation -> {
            innerVec.add((VecBatch) invocation.getArguments()[0]);
            return 0;
        }).when(omniOperator).addInput(any());
        doAnswer(invocation -> innerVec.listIterator()).when(omniOperator).getOutput();
        when(omniOperator.getVecAllocator()).thenReturn(mock(VecAllocator.class));
        return omniOperator;
    }
}
