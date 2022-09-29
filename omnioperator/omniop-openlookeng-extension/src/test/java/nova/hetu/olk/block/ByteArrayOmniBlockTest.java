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
import io.prestosql.spi.block.ByteArrayBlockEncoding;
import nova.hetu.omniruntime.vector.BooleanVec;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.annotations.Test;

import java.util.Random;

import static nova.hetu.olk.mock.MockUtil.fill;
import static nova.hetu.olk.mock.MockUtil.mockBlock;
import static nova.hetu.olk.mock.MockUtil.mockNewVecWithAnyArguments;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@PrepareForTest(ByteArrayOmniBlock.class)
public class ByteArrayOmniBlockTest
        extends AbstractBlockTest
{
    @Override
    protected String encodingName()
    {
        return ByteArrayBlockEncoding.NAME;
    }

    @Override
    protected void setupMock()
    {
        super.setupMock();
        mockNewVecWithAnyArguments(BooleanVec.class);
    }

    @Override
    protected void checkBlock(Block<?> block)
    {
        super.checkBlock(block);
        assertTrue(block.getValues() instanceof BooleanVec);
        for (int i = 0; i < block.getPositionCount(); i++) {
            assertTrue(block.get(i) instanceof Byte);
        }
    }

    @Override
    protected Block<?>[] blocksForTest()
    {
        return new Block<?>[]{
                mockBlock(false, false, getVecAllocator(), fill(new Boolean[1], index -> new Random().nextBoolean())),
                mockBlock(false, false, getVecAllocator(), fill(new Boolean[2], index -> new Random().nextBoolean())),
                mockBlock(false, false, getVecAllocator(), fill(new Boolean[3], index -> new Random().nextBoolean())),
        };
    }

    @Test(dataProvider = "blockProvider")
    public void testFunctionCall(int index)
    {
        Block<?> block = getBlockForTest(index);
        block.copyRegion(0, block.getPositionCount());
        block.copyPositions(new int[block.getPositionCount()], 0, block.getPositionCount());
        block.retainedBytesForEachPart((offset, position) -> {});
        block.getByte(0, 0);
        block.getLong(0, 0);
        block.writePositionTo(0, mock(BlockBuilder.class));
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
