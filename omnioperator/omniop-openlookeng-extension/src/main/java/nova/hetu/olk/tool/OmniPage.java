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
import io.prestosql.spi.block.Block;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.ArrayList;
import java.util.List;

public class OmniPage
        extends Page
{
    private VecBatch vecBatch;

    public OmniPage(int positionCount, VecBatch vecBatch, Block... blocks)
    {
        super(positionCount, blocks);
        this.vecBatch = vecBatch;
    }

    public OmniPage(Block... blocks)
    {
        super(blocks[0].getPositionCount(), blocks);
        List<Vec> vecs = new ArrayList<>();
        for (Block block : blocks) {
            vecs.add((Vec) block.getValues());
        }
        this.vecBatch = new VecBatch(vecs);
    }

    public VecBatch getVecBatch()
    {
        return vecBatch;
    }

    public void close()
    {
        if (vecBatch != null) {
            vecBatch.close();
            vecBatch = null;
        }
    }
}
