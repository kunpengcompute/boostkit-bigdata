/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.collection.unsafe.sort;

import com.google.common.primitives.Ints;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;

public class RadixTwoColumnSort {

    /**
     * Sorts a given array of longs using least-significant-digit radix sort. This routine assumes
     * you have extra space at the end of the array at least equal to the number of records. The
     * sort is destructive and may relocate the data positioned within the array.
     *
     * @param array          array of long elements followed by at least that many empty slots.
     * @param numRecords     number of data records in the array.
     * @param startByteIndex the first byte (in range [0, 7]) to sort each long by, counting from the
     *                       least significant byte.
     * @param endByteIndex   the last byte (in range [0, 7]) to sort each long by, counting from the
     *                       least significant byte. Must be greater than startByteIndex.
     * @param desc           whether this is a descending (binary-order) sort.
     * @param signed         whether this is a signed (two's complement) sort.
     * @return The starting index of the sorted data within the given array. We return this instead
     * of always copying the data back to position zero for efficiency.
     */
    public static int sort(
            LongArray array, long numRecords, int startByteIndex, int endByteIndex,
            boolean desc, boolean signed) {
        assert startByteIndex >= 0 : "startByteIndex (" + startByteIndex + ") should >= 0";
        assert endByteIndex <= 7 : "endByteIndex (" + endByteIndex + ") should <= 7";
        assert endByteIndex > startByteIndex;
        assert numRecords * 2 <= array.size();
        long inIndex = 0;
        long outIndex = numRecords;
        if (numRecords > 0) {
            long[][] counts = getCounts(array, numRecords, startByteIndex, endByteIndex);
            for (int i = startByteIndex; i <= endByteIndex; i++) {
                if (counts[i] != null) {
                    sortAtByte(
                            array, numRecords, counts[i], i, inIndex, outIndex,
                            desc, signed && i == endByteIndex);
                    long tmp = inIndex;
                    inIndex = outIndex;
                    outIndex = tmp;
                }
            }
        }
        return Ints.checkedCast(inIndex);
    }

    /**
     * Performs a partial sort by copying data into destination offsets for each byte value at the
     * specified byte offset.
     *
     * @param array      array to partially sort.
     * @param numRecords number of data records in the array.
     * @param counts     counts for each byte value. This routine destructively modifies this array.
     * @param byteIdx    the byte in a long to sort at, counting from the least significant byte.
     * @param inIndex    the starting index in the array where input data is located.
     * @param outIndex   the starting index where sorted output data should be written.
     * @param desc       whether this is a descending (binary-order) sort.
     * @param signed     whether this is a signed (two's complement) sort (only applies to last byte).
     */
    private static void sortAtByte(
            LongArray array, long numRecords, long[] counts, int byteIdx, long inIndex, long outIndex,
            boolean desc, boolean signed) {
        assert counts.length == 256;
        long[] offsets = transformCountsToOffsets(
                counts, numRecords, array.getBaseOffset() + outIndex * 8L, 8, desc, signed);
        Object baseObject = array.getBaseObject();
        long baseOffset = array.getBaseOffset() + inIndex * 8L;
        long maxOffset = baseOffset + numRecords * 8L;
        for (long offset = baseOffset; offset < maxOffset; offset += 8) {
            long value = Platform.getLong(baseObject, offset);
            int bucket = (int) ((value >>> (byteIdx * 8)) & 0xff);
            Platform.putLong(baseObject, offsets[bucket], value);
            offsets[bucket] += 8;
        }
    }

    /**
     * Computes a value histogram for each byte in the given array.
     *
     * @param array          array to count records in.
     * @param numRecords     number of data records in the array.
     * @param startByteIndex the first byte to compute counts for (the prior are skipped).
     * @param endByteIndex   the last byte to compute counts for.
     * @return an array of eight 256-byte count arrays, one for each byte starting from the least
     * significant byte. If the byte does not need sorting the array will be null.
     */
    private static long[][] getCounts(
            LongArray array, long numRecords, int startByteIndex, int endByteIndex) {
        long[][] counts = new long[8][];
        // Optimization: do a fast pre-pass to determine which byte indices we can skip for sorting.
        // If all the byte values at a particular index are the same we don't need to count it.
        long bitwiseMax = 0;
        long bitwiseMin = -1L;
        long maxOffset = array.getBaseOffset() + numRecords * 8L;
        Object baseObject = array.getBaseObject();
        for (long offset = array.getBaseOffset(); offset < maxOffset; offset += 8) {
            long value = Platform.getLong(baseObject, offset);
            bitwiseMax |= value;
            bitwiseMin &= value;
        }
        long bitsChanged = bitwiseMin ^ bitwiseMax;
        // Compute counts for each byte index.
        for (int i = startByteIndex; i <= endByteIndex; i++) {
            if (((bitsChanged >>> (i * 8)) & 0xff) != 0) {
                counts[i] = new long[256];
                // TODO(ekl) consider computing all the counts in one pass.
                for (long offset = array.getBaseOffset(); offset < maxOffset; offset += 8) {
                    counts[i][(int) ((Platform.getLong(baseObject, offset) >>> (i * 8)) & 0xff)]++;
                }
            }
        }
        return counts;
    }

    /**
     * Transforms counts into the proper unsafe output offsets for the sort type.
     *
     * @param counts         counts for each byte value. This routine destructively modifies this array.
     * @param numRecords     number of data records in the original data array.
     * @param outputOffset   output offset in bytes from the base array object.
     * @param bytesPerRecord size of each record (8 for plain sort, 16 for key-prefix sort).
     * @param desc           whether this is a descending (binary-order) sort.
     * @param signed         whether this is a signed (two's complement) sort.
     * @return the input counts array.
     */
    private static long[] transformCountsToOffsets(
            long[] counts, long numRecords, long outputOffset, long bytesPerRecord,
            boolean desc, boolean signed) {
        assert counts.length == 256;
        // signed 的情况，bigint都是unsigned
        int start = signed ? 128 : 0;  // output the negative records first (values 129-255).
        if (desc) {
            long pos = numRecords;
            for (int i = start; i < start + 256; i++) {
                pos -= counts[i & 0xff];
                counts[i & 0xff] = outputOffset + pos * bytesPerRecord;
            }
        } else {
            long pos = 0;
            // 256个桶，遍历每个桶
            for (int i = start; i < start + 256; i++) {
                // 是否有记录落在这个桶里
                long tmp = counts[i & 0xff];
                // 更换counts里的值为落在这个桶里的数据要放到LongArray中那个位置
                // outputOffset是原始数据在LongArray中的结束位置
                // 如果counts[0]有3个，count[1]有1个；bytesPerRecord=16
                // 变成位置信息之后，counts[0]=0,counts[1]=16*3,counts[2]=16*4
                counts[i & 0xff] = outputOffset + pos * bytesPerRecord;
                pos += tmp;
            }
        }
        return counts;
    }

    /**
     * Specialization of sort() for key-prefix arrays. In this type of array, each record consists
     * of two longs, only the second of which is sorted on.
     *
     * @param startIndex starting index in the array to sort from. This parameter is not supported
     *                   in the plain sort() implementation.
     */
    public static int sortKeyPrefixArray(
            LongArray array,
            long startIndex,
            long numRecords,
            int startByteIndex,
            int endByteIndex,
            boolean desc,
            boolean signed,
            int prefixShiftOffset) {
        // radix 排的是long值，按字节比较，long总共8个字节，从低字节startByteIndex开始比。
        assert numRecords * 6 <= array.size();
        // 在LongArray中的第0个long
        long inIndex = startIndex;
        // 在LongArray中的最后1个long
        long outIndex = startIndex + numRecords * 3L;
        if (numRecords > 0) {
            // 修改3，下面是通过单个prefix进行排序的过程，进行两次，先后各排一次
            // long[8][256]
            // 按每一个字节位（最多8个字节位）比，落到每个桶（256个桶）里面的记录数
            long[][] counts = getKeyPrefixArrayCounts(
                    array, startIndex, numRecords, startByteIndex, endByteIndex, prefixShiftOffset);
            // 遍历每一个字节位，然后将counts转换成分配到这个桶里的记录要被放到LongArray中的那个位置（利用LongArray中剩余的位置来做为存放数据的桶）
            for (int i = startByteIndex; i <= 7; i++) {
                if (counts[i] != null) {
                    // 将counts转换成分配到这个桶里的记录要被放到LongArray中的那个位置（利用LongArray中剩余的位置来做为存放数据的桶）
                    sortKeyPrefixArrayAtByte2(
                            array, numRecords, counts[i], i, inIndex, outIndex,
                            desc, signed && i == endByteIndex);
                    long tmp = inIndex;
                    inIndex = outIndex;
                    outIndex = tmp;
                }
            }
            for (int i = 8; i <= endByteIndex; i++) {
                if (counts[i] != null) {
                    // 将counts转换成分配到这个桶里的记录要被放到LongArray中的那个位置（利用LongArray中剩余的位置来做为存放数据的桶）
                    sortKeyPrefixArrayAtByte(
                            array, numRecords, counts[i], i, inIndex, outIndex,
                            desc, signed && i == endByteIndex);
                    long tmp = inIndex;
                    inIndex = outIndex;
                    outIndex = tmp;
                }
            }
        }
        return Ints.checkedCast(inIndex);
    }

    /**
     * Specialization of getCounts() for key-prefix arrays. We could probably combine this with
     * getCounts with some added parameters but that seems to hurt in benchmarks.
     */
    private static long[][] getKeyPrefixArrayCounts(
            LongArray array, long startIndex, long numRecords, int startByteIndex, int endByteIndex, int prefixShiftOffset) {
        long[][] counts = new long[16][];
        long bitwiseMax1 = 0;
        long bitwiseMin1 = -1L;
        // 获取第0条记录的内存位置
        long baseOffset = array.getBaseOffset() + startIndex * 8L;
        // 最后一条记录的结束内存位置
        long limit = baseOffset + numRecords * 24L;
        Object baseObject = array.getBaseObject();
        // 遍历所有记录的prefix，得出每一位的不同
        for (long offset = baseOffset; offset < limit; offset += 24) {
            long value = Platform.getLong(baseObject, offset + 8);
            bitwiseMax1 |= value;
            bitwiseMin1 &= value;
        }
        long bitsChanged1 = bitwiseMin1 ^ bitwiseMax1;
        // 从第0个字节位开始，到第7个字节位。
        // 遍历所有的记录，统计在当前字节位，每个桶里会有几条记录
        for (int i = 0; i <= 7; i++) {
            if (((bitsChanged1 >>> (i * 8)) & 0xff) != 0) {
                counts[i+8] = new long[256];
                for (long offset = baseOffset; offset < limit; offset += 24) {
                    counts[i+8][(int) ((Platform.getLong(baseObject, offset + 8) >>> (i * 8)) & 0xff)]++;
                }
            }
        }


        long bitwiseMax2 = 0;
        long bitwiseMin2 = -1L;
        // 遍历所有记录的prefix，得出每一位的不同
        for (long offset = baseOffset; offset < limit; offset += 24) {
            long value = Platform.getLong(baseObject, offset + 16);
            bitwiseMax2 |= value;
            bitwiseMin2 &= value;
        }
        long bitsChanged2 = bitwiseMin2 ^ bitwiseMax2;
        // 从第0个字节位开始，到第7个字节位。
        // 遍历所有的记录，统计在当前字节位，每个桶里会有几条记录
        for (int i = 0; i <= 7; i++) {
            if (((bitsChanged2 >>> (i * 8)) & 0xff) != 0) {
                counts[i] = new long[256];
                for (long offset = baseOffset; offset < limit; offset += 24) {
                    counts[i][(int) ((Platform.getLong(baseObject, offset + 16) >>> (i * 8)) & 0xff)]++;
                }
            }
        }


        return counts;
    }

    /**
     * Specialization of sortAtByte() for key-prefix arrays.
     */
    private static void sortKeyPrefixArrayAtByte(
            LongArray array, long numRecords, long[] counts, int byteIdx, long inIndex, long outIndex,
            boolean desc, boolean signed) {
        assert counts.length == 256;
        // 将counts转换成分配到这个桶里的记录要被放到LongArray中的那个位置（利用LongArray中剩余的位置来做为存放数据的桶）
        long[] offsets = transformCountsToOffsets(
                counts, numRecords, array.getBaseOffset() + outIndex * 8L, 24, desc, signed);
        Object baseObject = array.getBaseObject();
        // 第一条记录的起始位置
        long baseOffset = array.getBaseOffset() + inIndex * 8L;
        // 最后一条记录的结束位置
        long maxOffset = baseOffset + numRecords * 24L;
        // 遍历每一条记录
        for (long offset = baseOffset; offset < maxOffset; offset += 24) {
            // 记录的指针
            long key = Platform.getLong(baseObject, offset);
            // 记录的前缀
            long prefix1 = Platform.getLong(baseObject, offset + 8);
            long prefix2 = Platform.getLong(baseObject, offset + 16);
            // 计算在当前字节位，记录应该落在哪个桶里
            int bucket = (int) ((prefix1 >>> (byteIdx * 8)) & 0xff);
            // 获取到记录应该放到哪个位置
            long dest = offsets[bucket];
            // 放记录指针
            Platform.putLong(baseObject, dest, key);
            // 放记录前缀
            Platform.putLong(baseObject, dest + 8, prefix1);
            Platform.putLong(baseObject, dest + 16, prefix2);
            // 落在这个桶里的可能还有其他记录，位置偏移2个long
            offsets[bucket] += 24;
        }
    }

    private static void sortKeyPrefixArrayAtByte2(
            LongArray array, long numRecords, long[] counts, int byteIdx, long inIndex, long outIndex,
            boolean desc, boolean signed) {
        assert counts.length == 256;
        // 将counts转换成分配到这个桶里的记录要被放到LongArray中的那个位置（利用LongArray中剩余的位置来做为存放数据的桶）
        long[] offsets = transformCountsToOffsets(
                counts, numRecords, array.getBaseOffset() + outIndex * 8L, 24, desc, signed);
        Object baseObject = array.getBaseObject();
        // 第一条记录的起始位置
        long baseOffset = array.getBaseOffset() + inIndex * 8L;
        // 最后一条记录的结束位置
        long maxOffset = baseOffset + numRecords * 24L;
        // 遍历每一条记录
        for (long offset = baseOffset; offset < maxOffset; offset += 24) {
            // 记录的指针
            long key = Platform.getLong(baseObject, offset);
            // 记录的前缀
            long prefix1 = Platform.getLong(baseObject, offset + 8);
            long prefix2 = Platform.getLong(baseObject, offset + 16);
            // 计算在当前字节位，记录应该落在哪个桶里
            int bucket = (int) ((prefix2 >>> (byteIdx * 8)) & 0xff);
            // 获取到记录应该放到哪个位置
            long dest = offsets[bucket];
            // 放记录指针
            Platform.putLong(baseObject, dest, key);
            // 放记录前缀
            Platform.putLong(baseObject, dest + 8, prefix1);
            Platform.putLong(baseObject, dest + 16, prefix2);
            // 落在这个桶里的可能还有其他记录，位置偏移2个long
            offsets[bucket] += 24;
        }
    }

}
