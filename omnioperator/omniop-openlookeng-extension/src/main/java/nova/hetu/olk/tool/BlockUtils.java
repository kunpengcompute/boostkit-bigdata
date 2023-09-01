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

/**
 * The type Block utils.
 *
 * @since 20210630
 */
public class BlockUtils
{
    private BlockUtils()
    {
    }

    public static void freePage(Page page)
    {
        // release native vector
        Block[] blocks = page.getBlocks();
        if (blocks != null) {
            for (Block block : blocks) {
                block.close();
            }
        }
        // only release vecBatch if page belong to OmniPage
        if (page instanceof OmniPage) {
            ((OmniPage) page).close();
        }
    }
}
