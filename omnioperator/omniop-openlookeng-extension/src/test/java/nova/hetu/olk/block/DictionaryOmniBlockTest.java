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
import io.prestosql.spi.block.DictionaryBlockEncoding;
import nova.hetu.omniruntime.vector.DictionaryVec;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.function.Supplier;

import static nova.hetu.olk.mock.MockUtil.fill;
import static nova.hetu.olk.mock.MockUtil.mockBlock;
import static nova.hetu.olk.mock.MockUtil.mockNewWithWithAnyArguments;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@PrepareForTest(DictionaryOmniBlock.class)
public class DictionaryOmniBlockTest
        extends AbstractBlockTest
{
    private <T> void assertOperationEqual(Supplier<T> firstOperation, Supplier<T> secondOperation)
    {
        Exception exception = null;
        T firstResult = null;
        try {
            firstResult = firstOperation.get();
        }
        catch (Exception e) {
            exception = e;
        }
        if (exception == null) {
            assertEquals(firstResult, secondOperation.get());
        }
        else {
            assertThrows(exception.getMessage(), exception.getClass(), secondOperation::get);
        }
    }

    @Override
    protected void setupMock()
    {
        super.setupMock();
        mockNewWithWithAnyArguments(DictionaryVec.class);
    }

    @Override
    protected String encodingName()
    {
        return DictionaryBlockEncoding.NAME;
    }

    @Override
    protected void checkBlock(Block<?> block)
    {
        super.checkBlock(block);
        DictionaryOmniBlock<?> omniBlock = (DictionaryOmniBlock<?>) block;
        Block<?> originalBlock = ((DictionaryOmniBlock<?>) block).getDictionary();
        assertTrue(omniBlock.compact() != null && omniBlock.isCompact());
        for (int i = 0; i < block.getPositionCount(); i++) {
            int index = i;
            assertOperationEqual(() -> omniBlock.getSliceLength(index), () -> originalBlock.getSliceLength(index));
            assertOperationEqual(() -> omniBlock.getByte(index, 0), () -> originalBlock.getByte(index, 0));
            assertOperationEqual(() -> omniBlock.getShort(index, 0), () -> originalBlock.getShort(index, 0));
            assertOperationEqual(() -> omniBlock.getInt(index, 0), () -> originalBlock.getInt(index, 0));
            assertOperationEqual(() -> omniBlock.getLong(index, 0), () -> originalBlock.getLong(index, 0));
            assertOperationEqual(() -> omniBlock.getSlice(index, 0, 1), () -> originalBlock.getSlice(index, 0, 1));
            assertOperationEqual(() -> omniBlock.getString(index, 0, 1), () -> originalBlock.getString(index, 0, 1));
            assertOperationEqual(() -> omniBlock.getObject(index, Object.class), () -> originalBlock.getObject(index, Object.class));
        }
    }

    @Override
    protected void checkNull(Block<?> block)
    {
    }

    @Override
    protected Block<?>[] blocksForTest()
    {
        return new Block<?>[]{
                mockBlock(false, true, getVecAllocator(), fill(new Boolean[1], index -> new Random().nextBoolean())),
                mockBlock(false, true, getVecAllocator(), fill(new Boolean[2], index -> new Random().nextBoolean())),
                mockBlock(false, true, getVecAllocator(), fill(new Boolean[3], index -> new Random().nextBoolean())),
        };
    }

    @Test(dataProvider = "blockProvider")
    public void testFunctionCall(int index)
    {
        Block<?> block = getBlockForTest(index);
        block.retainedBytesForEachPart((offset, position) -> {});
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
