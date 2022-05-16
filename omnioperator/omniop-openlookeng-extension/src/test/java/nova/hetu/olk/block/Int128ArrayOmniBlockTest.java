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
import io.prestosql.spi.block.Int128ArrayBlockEncoding;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.annotations.Test;

import java.util.Random;

import static nova.hetu.olk.mock.MockUtil.fill;
import static nova.hetu.olk.mock.MockUtil.mockBlock;
import static nova.hetu.olk.mock.MockUtil.mockNewWithWithAnyArguments;
import static org.junit.Assert.assertTrue;

@PrepareForTest(Int128ArrayOmniBlock.class)
public class Int128ArrayOmniBlockTest
        extends AbstractBlockTest
{
    @Override
    protected String encodingName()
    {
        return Int128ArrayBlockEncoding.NAME;
    }

    @Override
    protected void setupMock()
    {
        super.setupMock();
        mockNewWithWithAnyArguments(Decimal128Vec.class);
    }

    @Override
    protected void checkBlock(Block<?> block)
    {
        super.checkBlock(block);
        assertTrue(block.getValues() instanceof Decimal128Vec);
    }

    @Override
    protected Block<?>[] blocksForTest()
    {
        return new Block<?>[]{
                mockBlock(false, false, getVecAllocator(), fill(new Long[1][], index -> fill(new Long[2], idx -> new Random().nextLong()))),
                mockBlock(false, false, getVecAllocator(), fill(new Long[2][], index -> fill(new Long[2], idx -> new Random().nextLong()))),
                mockBlock(false, false, getVecAllocator(), fill(new Long[3][], index -> fill(new Long[2], idx -> new Random().nextLong())))
        };
    }

    @Test(dataProvider = "blockProvider")
    public void testFunctionCall(int index)
    {
        Block<?> block = getBlockForTest(index);
        block.copyRegion(0, block.getPositionCount());
        block.copyPositions(new int[block.getPositionCount()], 0, block.getPositionCount());
        block.retainedBytesForEachPart((offset, position) -> {});
        block.getSingleValueBlock(0);
    }

    @Test(dataProvider = "blockProvider")
    public void testFilter(int index)
    {
        super.defaultFilterTest(getBlockForTest(index));
    }

    @Test(dataProvider = "blockProvider")
    public void testDestroy(int index)
    {
        super.defaultDestroyTest(getBlockForTest(index));
    }
}
