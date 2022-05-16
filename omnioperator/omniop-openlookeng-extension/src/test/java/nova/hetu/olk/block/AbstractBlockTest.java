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
import nova.hetu.olk.mock.MockUtil;
import nova.hetu.omniruntime.vector.VecAllocator;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressStaticInitializationFor({
        "nova.hetu.omniruntime.vector.VecAllocator",
        "nova.hetu.omniruntime.vector.Vec"
})
@PowerMockIgnore("javax.management.*")
public class AbstractBlockTest
        extends PowerMockTestCase
{
    private VecAllocator vecAllocator;

    protected final VecAllocator getVecAllocator()
    {
        return vecAllocator;
    }

    @DataProvider(name = "blockProvider")
    public Object[][] dataProvider()
    {
        AtomicInteger atomicBiInteger = new AtomicInteger(0);
        return Arrays.stream(blocksForTest()).map(page -> new Object[]{atomicBiInteger.getAndIncrement()}).toArray(Object[][]::new);
    }

    protected Block<?>[] blocksForTest()
    {
        return new Block<?>[0];
    }

    protected final Block<?> getBlockForTest(int i)
    {
        Block<?> block = blocksForTest()[i];
        checkBlock(block);
        return block;
    }

    @BeforeMethod
    public void setUp()
    {
        vecAllocator = MockUtil.mockNewWithWithAnyArguments(VecAllocator.class);
        this.setupMock();
    }

    protected void setupMock()
    {
    }

    protected void checkBlock(Block<?> block)
    {
        assertTrue(block.isExtensionBlock());
        assertTrue(block.toString().contains("positionCount=" + block.getPositionCount()));
        assertTrue(block.getSizeInBytes() > 0);
        assertTrue(block.getRetainedSizeInBytes() > 0);
        assertTrue(block.getPositionCount() > 0);
        assertTrue(block.getLogicalSizeInBytes() > 0);
        assertTrue(encodingName() == null || encodingName().equals(block.getEncodingName()));
        checkNull(block);
    }

    protected String encodingName()
    {
        return null;
    }

    protected void checkNull(Block<?> block)
    {
        assertFalse(block.mayHaveNull());
        for (int i = 0; i < block.getPositionCount(); i++) {
            assertFalse(block.isNull(i));
        }
    }

    protected final void defaultFilterTest(Block<?> block)
    {
        int[] positions = new int[block.getPositionCount()];
        int[] matchedPositions = new int[block.getPositionCount()];
        int expectMatchCount = new Random().nextInt(block.getPositionCount()) + 1;
        AtomicInteger counter = new AtomicInteger(0);
        assertEquals(expectMatchCount, block.filter(positions, block.getPositionCount(), matchedPositions, vec -> counter.getAndIncrement() < expectMatchCount));
        boolean[] validPositions = new boolean[block.getPositionCount()];
        Arrays.fill(validPositions, false);
        boolean[] result = block.filter(null, validPositions);
        for (boolean valid : result) {
            assertFalse(valid);
        }
    }

    protected final void defaultDestroyTest(Block<?> block)
    {
        block.setClosable(true);
        block.close();
    }
}
