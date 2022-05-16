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
import io.prestosql.spi.block.LazyBlock;
import nova.hetu.olk.mock.MockUtil;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.FixedWidthVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LazyVec;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.annotations.Test;

import java.util.Random;

import static nova.hetu.olk.mock.MockUtil.fill;
import static nova.hetu.olk.mock.MockUtil.mockBlock;
import static nova.hetu.olk.mock.MockUtil.mockNewWithWithAnyArguments;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@PrepareForTest({
        LazyOmniBlock.class,
        LazyBlock.class,
        MockUtil.class,
        ByteArrayOmniBlock.class,
        IntArrayOmniBlock.class,
        DoubleArrayOmniBlock.class
})
public class LazyOmniBlockTest
        extends AbstractBlockTest
{
    @Override
    protected void setupMock()
    {
        super.setupMock();
        mockNewWithWithAnyArguments(LazyVec.class);
        mockNewWithWithAnyArguments(BooleanVec.class);
        mockNewWithWithAnyArguments(IntVec.class);
        mockNewWithWithAnyArguments(DoubleVec.class);
        mockNewWithWithAnyArguments(FixedWidthVec.class);
    }

    @Override
    protected void checkBlock(Block<?> block)
    {
        LazyOmniBlock<?> original = (LazyOmniBlock<?>) block;
        LazyBlock<?> lazyBlock = original.getLazyBlock();
        assertTrue(block.isExtensionBlock());
        assertTrue(block.getValues() instanceof LazyVec);
        for (int i = 0; i < original.getPositionCount(); i++) {
            assertEquals(original.getEncodingName(), lazyBlock.getEncodingName());
        }
    }

    @Override
    protected Block<?>[] blocksForTest()
    {
        setupMock();
        return new Block<?>[]{
                mockBlock(true, false, getVecAllocator(), fill(new Boolean[3], index -> new Random().nextBoolean())),
                mockBlock(true, false, getVecAllocator(), fill(new Integer[3], index -> new Random().nextInt())),
                mockBlock(true, false, getVecAllocator(), fill(new Double[3], index -> new Random().nextDouble())),
        };
    }

    @Test(dataProvider = "blockProvider")
    public void testFunctionCall(int index)
    {
        Block<?> block = getBlockForTest(index);
        block.getLoadedBlock();
        block.retainedBytesForEachPart((offset, position) -> {});
        block.getSingleValueBlock(0);
        for (int i = 0; i < block.getPositionCount(); i++) {
            block.getEstimatedDataSizeForStats(i);
        }
    }

    @Test(dataProvider = "blockProvider")
    public void testDestroy(int index)
    {
        super.defaultDestroyTest(getBlockForTest(index));
    }
}
