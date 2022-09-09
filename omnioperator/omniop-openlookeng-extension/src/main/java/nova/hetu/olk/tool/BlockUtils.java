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
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.ShortVec;
import nova.hetu.omniruntime.vector.VarcharVec;

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

    /**
     * Compact vec boolean vec.
     *
     * @param vec the vec
     * @param index the index
     * @param length the length
     * @return the boolean vec
     */
    public static BooleanVec compactVec(BooleanVec vec, int index, int length)
    {
        if (index == 0 && length == vec.getSize() && vec.getOffset() == 0) {
            return vec;
        }
        BooleanVec newValues = vec.copyRegion(index, length);
        // TODO: is there any other place to close? Use put(bytebuffer) for better
        // performance
        vec.close();
        return newValues;
    }

    /**
     * Compact vec int vec.
     *
     * @param vec the vec
     * @param index the index
     * @param length the length
     * @return the int vec
     */
    public static IntVec compactVec(IntVec vec, int index, int length)
    {
        if (index == 0 && length == vec.getSize() && vec.getOffset() == 0) {
            return vec;
        }
        IntVec newValues = vec.copyRegion(index, length);
        // TODO: is there any other place to close? Use put(bytebuffer) for better
        // performance
        vec.close();
        return newValues;
    }

    /**
     * Compact vec short vec.
     *
     * @param vec the vec
     * @param index the index
     * @param length the length
     * @return the short vec
     */
    public static ShortVec compactVec(ShortVec vec, int index, int length)
    {
        if (index == 0 && length == vec.getSize() && vec.getOffset() == 0) {
            return vec;
        }
        ShortVec newValues = vec.copyRegion(index, length);
        // TODO: is there any other place to close? Use put(bytebuffer) for better
        // performance
        vec.close();
        return newValues;
    }

    /**
     * Compact vec long vec.
     *
     * @param vec the vec
     * @param index the index
     * @param length the length
     * @return the long vec
     */
    public static LongVec compactVec(LongVec vec, int index, int length)
    {
        if (index == 0 && length == vec.getSize() && vec.getOffset() == 0) {
            return vec;
        }
        LongVec newValues = vec.copyRegion(index, length);
        // TODO: is there any other place to close? Use put(bytebuffer) for better
        // performance
        vec.close();
        return newValues;
    }

    /**
     * Compact vec double vec.
     *
     * @param vec the vec
     * @param index the index
     * @param length the length
     * @return the double vec
     */
    public static DoubleVec compactVec(DoubleVec vec, int index, int length)
    {
        if (index == 0 && length == vec.getSize() && vec.getOffset() == 0) {
            return vec;
        }
        DoubleVec newValues = vec.copyRegion(index, length);
        // TODO: is there any other place to close? Use put(bytebuffer) for better
        // performance
        vec.close();
        return newValues;
    }

    /**
     * Compact vec varchar vec.
     *
     * @param vec the vec
     * @param index the index
     * @param length the length
     * @return the varchar vec
     */
    public static VarcharVec compactVec(VarcharVec vec, int index, int length)
    {
        if (index == 0 && length == vec.getSize() && vec.getOffset() == 0) {
            return vec;
        }
        VarcharVec newValues = vec.copyRegion(index, length);
        // TODO: is there any other place to close original vec?
        vec.close();
        return newValues;
    }

    /**
     * Compact vec decimal 128 vec.
     *
     * @param vec the vec
     * @param index the index
     * @param length the length
     * @return the decimal 128 vec
     */
    public static Decimal128Vec compactVec(Decimal128Vec vec, int index, int length)
    {
        if (index == 0 && length == vec.getSize() && vec.getOffset() == 0) {
            return vec;
        }
        Decimal128Vec newValues = vec.copyRegion(index, length);
        // TODO: is there any other place to close original vec?
        vec.close();
        return newValues;
    }

    public static void freePage(Page page)
    {
        Block[] blocks = page.getBlocks();
        if (blocks != null) {
            for (Block block : blocks) {
                block.close();
            }
        }
    }
}
