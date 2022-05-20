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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.UUID;

import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.block.RowOmniBlock.fromFieldBlocks;
import static nova.hetu.olk.mock.MockUtil.fill;
import static nova.hetu.olk.mock.MockUtil.mockBlock;
import static nova.hetu.olk.mock.MockUtil.mockNewWithWithAnyArguments;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@PrepareForTest(RowOmniBlock.class)
public class RowOmniBlockTest
        extends AbstractBlockTest
{
    private Block<?> rowBlock(Block<?> block, Type dataType)
    {
        return fromFieldBlocks(getVecAllocator(), block.getPositionCount(), Optional.empty(), new Block[]{block}, dataType);
    }

    @Override
    protected void setupMock()
    {
        super.setupMock();
        mockNewWithWithAnyArguments(BooleanVec.class);
        mockNewWithWithAnyArguments(IntVec.class);
        mockNewWithWithAnyArguments(DoubleVec.class);
    }

    @Override
    protected void checkNull(Block<?> block)
    {
    }

    @Override
    protected void checkBlock(Block<?> block)
    {
        super.checkBlock(block);
        assertEquals(((RowOmniBlock<?>) block).getOffsetBase(), 0);
    }

    @Override
    protected Block<?>[] blocksForTest()
    {
        return new Block<?>[]{
                rowBlock(requireNonNull(mockBlock(false, false, getVecAllocator(), fill(new String[1], index -> UUID.randomUUID().toString()))),
                        RowType.from(ImmutableList.of(RowType.field(VARCHAR)))),
                rowBlock(requireNonNull(mockBlock(false, false, getVecAllocator(), fill(new String[2], index -> UUID.randomUUID().toString()))),
                        RowType.from(ImmutableList.of(RowType.field(VARCHAR)))),
                rowBlock(requireNonNull(mockBlock(false, false, getVecAllocator(), fill(new String[3], index -> UUID.randomUUID().toString()))),
                        RowType.from(ImmutableList.of(RowType.field(VARCHAR))))
        };
    }

    @Test(dataProvider = "blockProvider")
    public void testFunctionCall(int index)
    {
        Block<?> block = getBlockForTest(index);
        block.retainedBytesForEachPart((offset, position) -> {});
        block.getLoadedBlock();
        block.writePositionTo(0, mock(BlockBuilder.class));
    }

    @Test(dataProvider = "blockProvider")
    public void testDestroy(int index)
    {
        super.defaultDestroyTest(getBlockForTest(index));
    }
}
