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

import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.block.Block;
import nova.hetu.olk.block.ByteArrayOmniBlock;
import nova.hetu.olk.block.DictionaryOmniBlock;
import nova.hetu.olk.block.DoubleArrayOmniBlock;
import nova.hetu.olk.block.Int128ArrayOmniBlock;
import nova.hetu.olk.block.IntArrayOmniBlock;
import nova.hetu.olk.block.LongArrayOmniBlock;
import nova.hetu.olk.block.ShortArrayOmniBlock;
import nova.hetu.olk.block.VariableWidthOmniBlock;
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
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.Iterator;

import static io.prestosql.spi.block.DictionaryId.randomDictionaryId;
import static nova.hetu.olk.tool.OperatorUtils.buildRowOmniBlock;

/**
 * The type Vec batch to page iterator.
 *
 * @since 20210630
 */
public class VecBatchToPageIterator
        implements Iterator<Page>
{
    private final Iterator<VecBatch> vecBatchIterator;

    /**
     * Instantiates a new Vec batch to page iterator.
     *
     * @param vecBatchIterator the vec batch iterator
     */
    public VecBatchToPageIterator(Iterator<VecBatch> vecBatchIterator)
    {
        this.vecBatchIterator = vecBatchIterator;
    }

    @Override
    public boolean hasNext()
    {
        return vecBatchIterator.hasNext();
    }

    @Override
    public Page next()
    {
        VecBatch vecBatch = vecBatchIterator.next();
        int positionCount = vecBatch.getRowCount();
        Vec[] vectors = vecBatch.getVectors();
        int channelCount = vectors.length;
        Block[] blocks = new Block[channelCount];
        for (int i = 0; i < channelCount; i++) {
            if (vectors[i] instanceof DoubleVec) {
                blocks[i] = new DoubleArrayOmniBlock(positionCount, ((DoubleVec) vectors[i]));
            }
            else if (vectors[i] instanceof BooleanVec) {
                blocks[i] = new ByteArrayOmniBlock(positionCount, ((BooleanVec) vectors[i]));
            }
            else if (vectors[i] instanceof ShortVec) {
                blocks[i] = new ShortArrayOmniBlock(positionCount, (ShortVec) vectors[i]);
            }
            else if (vectors[i] instanceof LongVec) {
                blocks[i] = new LongArrayOmniBlock(positionCount, (LongVec) vectors[i]);
            }
            else if (vectors[i] instanceof IntVec) {
                blocks[i] = new IntArrayOmniBlock(positionCount, (IntVec) vectors[i]);
            }
            else if (vectors[i] instanceof VarcharVec) {
                blocks[i] = new VariableWidthOmniBlock(positionCount, (VarcharVec) vectors[i]);
            }
            else if (vectors[i] instanceof Decimal128Vec) {
                blocks[i] = new Int128ArrayOmniBlock(positionCount, (Decimal128Vec) vectors[i]);
            }
            else if (vectors[i] instanceof ContainerVec) {
                ContainerVec containerVec = (ContainerVec) vectors[i];
                blocks[i] = buildRowOmniBlock(containerVec);
            }
            else if (vectors[i] instanceof DictionaryVec) {
                DictionaryVec dictionaryVec = (DictionaryVec) vectors[i];
                blocks[i] = new DictionaryOmniBlock(dictionaryVec, false, randomDictionaryId());
            }
            else {
                vecBatch.releaseAllVectors();
                vecBatch.close();
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Unsupported vector type " + vectors[i]);
            }
        }
        vecBatch.close();
        return new Page(positionCount, blocks);
    }
}
