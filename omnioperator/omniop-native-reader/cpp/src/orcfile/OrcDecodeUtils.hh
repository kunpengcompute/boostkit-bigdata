/**
 * Copyright (C) 2025-2025. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ORC_DECODE_UTILS
#define ORC_DECODE_UTILS

#include "arm_neon.h"
namespace omniruntime::reader {

void inline UnZigZagBatch(uint64_t *data, uint64_t numValues) {
    for (uint64_t i = 0; i < numValues; i++) {
        data[i] = data[i] >> 1 ^ -(data[i] & 1);
    }
}

void inline UnZigZagBatchHEFs8p2(uint64_t *data, uint64_t numValues) {
    uint64_t vec_data_s0_p0;
    uint64_t vec_data_s1_p0;
    uint64_t vec_data_s2_p0;
    uint64_t vec_data_s3_p0;
    uint64_t vec_data_s4_p0;
    uint64_t vec_data_s5_p0;
    uint64_t vec_data_s6_p0;
    uint64_t vec_data_s7_p0;
    uint64_t vec_data_s0_p1;
    uint64_t vec_data_s1_p1;
    uint64_t vec_data_s2_p1;
    uint64_t vec_data_s3_p1;
    uint64_t vec_data_s4_p1;
    uint64_t vec_data_s5_p1;
    uint64_t vec_data_s6_p1;
    uint64_t vec_data_s7_p1;

    uint64_t neg_lsb_s0_p0;
    uint64_t neg_lsb_s1_p0;
    uint64_t neg_lsb_s2_p0;
    uint64_t neg_lsb_s3_p0;
    uint64_t neg_lsb_s4_p0;
    uint64_t neg_lsb_s5_p0;
    uint64_t neg_lsb_s6_p0;
    uint64_t neg_lsb_s7_p0;
    uint64_t neg_lsb_s0_p1;
    uint64_t neg_lsb_s1_p1;
    uint64_t neg_lsb_s2_p1;
    uint64_t neg_lsb_s3_p1;
    uint64_t neg_lsb_s4_p1;
    uint64_t neg_lsb_s5_p1;
    uint64_t neg_lsb_s6_p1;
    uint64_t neg_lsb_s7_p1;

    uint64_t shifted_data_s0_p0;
    uint64_t shifted_data_s1_p0;
    uint64_t shifted_data_s2_p0;
    uint64_t shifted_data_s3_p0;
    uint64_t shifted_data_s4_p0;
    uint64_t shifted_data_s5_p0;
    uint64_t shifted_data_s6_p0;
    uint64_t shifted_data_s7_p0;
    uint64_t shifted_data_s0_p1;
    uint64_t shifted_data_s1_p1;
    uint64_t shifted_data_s2_p1;
    uint64_t shifted_data_s3_p1;
    uint64_t shifted_data_s4_p1;
    uint64_t shifted_data_s5_p1;
    uint64_t shifted_data_s6_p1;
    uint64_t shifted_data_s7_p1;

    uint64_t lsb_mask_s0_p0;
    uint64_t lsb_mask_s1_p0;
    uint64_t lsb_mask_s2_p0;
    uint64_t lsb_mask_s3_p0;
    uint64_t lsb_mask_s4_p0;
    uint64_t lsb_mask_s5_p0;
    uint64_t lsb_mask_s6_p0;
    uint64_t lsb_mask_s7_p0;
    uint64_t lsb_mask_s0_p1;
    uint64_t lsb_mask_s1_p1;
    uint64_t lsb_mask_s2_p1;
    uint64_t lsb_mask_s3_p1;
    uint64_t lsb_mask_s4_p1;
    uint64_t lsb_mask_s5_p1;
    uint64_t lsb_mask_s6_p1;
    uint64_t lsb_mask_s7_p1;

    uint64_t i = 0;
    for (; i + 16 <= numValues; i += 16) {
        // load data[i]
        vec_data_s0_p0 = *(data + i);
        vec_data_s1_p0 = *(data + i + 1);
        vec_data_s2_p0 = *(data + i + 2);
        vec_data_s3_p0 = *(data + i + 3);
        vec_data_s4_p0 = *(data + i + 4);
        vec_data_s5_p0 = *(data + i + 5);
        vec_data_s6_p0 = *(data + i + 6);
        vec_data_s7_p0 = *(data + i + 7);
        vec_data_s0_p1 = *(data + i + 8);
        vec_data_s1_p1 = *(data + i + 9);
        vec_data_s2_p1 = *(data + i + 10);
        vec_data_s3_p1 = *(data + i + 11);
        vec_data_s4_p1 = *(data + i + 12);
        vec_data_s5_p1 = *(data + i + 13);
        vec_data_s6_p1 = *(data + i + 14);
        vec_data_s7_p1 = *(data + i + 15);

        // compute data[i] & 1
        lsb_mask_s0_p0 = vec_data_s0_p0 & 1;
        lsb_mask_s1_p0 = vec_data_s1_p0 & 1;
        lsb_mask_s2_p0 = vec_data_s2_p0 & 1;
        lsb_mask_s3_p0 = vec_data_s3_p0 & 1;
        lsb_mask_s4_p0 = vec_data_s4_p0 & 1;
        lsb_mask_s5_p0 = vec_data_s5_p0 & 1;
        lsb_mask_s6_p0 = vec_data_s6_p0 & 1;
        lsb_mask_s7_p0 = vec_data_s7_p0 & 1;
        lsb_mask_s0_p1 = vec_data_s0_p1 & 1;
        lsb_mask_s1_p1 = vec_data_s1_p1 & 1;
        lsb_mask_s2_p1 = vec_data_s2_p1 & 1;
        lsb_mask_s3_p1 = vec_data_s3_p1 & 1;
        lsb_mask_s4_p1 = vec_data_s4_p1 & 1;
        lsb_mask_s5_p1 = vec_data_s5_p1 & 1;
        lsb_mask_s6_p1 = vec_data_s6_p1 & 1;
        lsb_mask_s7_p1 = vec_data_s7_p1 & 1;

        // compute -(data[i] & 1)
        neg_lsb_s0_p0 = -lsb_mask_s0_p0;
        neg_lsb_s1_p0 = -lsb_mask_s1_p0;
        neg_lsb_s2_p0 = -lsb_mask_s2_p0;
        neg_lsb_s3_p0 = -lsb_mask_s3_p0;
        neg_lsb_s4_p0 = -lsb_mask_s4_p0;
        neg_lsb_s5_p0 = -lsb_mask_s5_p0;
        neg_lsb_s6_p0 = -lsb_mask_s6_p0;
        neg_lsb_s7_p0 = -lsb_mask_s7_p0;
        neg_lsb_s0_p1 = -lsb_mask_s0_p1;
        neg_lsb_s1_p1 = -lsb_mask_s1_p1;
        neg_lsb_s2_p1 = -lsb_mask_s2_p1;
        neg_lsb_s3_p1 = -lsb_mask_s3_p1;
        neg_lsb_s4_p1 = -lsb_mask_s4_p1;
        neg_lsb_s5_p1 = -lsb_mask_s5_p1;
        neg_lsb_s6_p1 = -lsb_mask_s6_p1;
        neg_lsb_s7_p1 = -lsb_mask_s7_p1;

        // compute data[i] >> 1
        shifted_data_s0_p0 = vec_data_s0_p0 >> 1;
        shifted_data_s1_p0 = vec_data_s1_p0 >> 1;
        shifted_data_s2_p0 = vec_data_s2_p0 >> 1;
        shifted_data_s3_p0 = vec_data_s3_p0 >> 1;
        shifted_data_s4_p0 = vec_data_s4_p0 >> 1;
        shifted_data_s5_p0 = vec_data_s5_p0 >> 1;
        shifted_data_s6_p0 = vec_data_s6_p0 >> 1;
        shifted_data_s7_p0 = vec_data_s7_p0 >> 1;
        shifted_data_s0_p1 = vec_data_s0_p1 >> 1;
        shifted_data_s1_p1 = vec_data_s1_p1 >> 1;
        shifted_data_s2_p1 = vec_data_s2_p1 >> 1;
        shifted_data_s3_p1 = vec_data_s3_p1 >> 1;
        shifted_data_s4_p1 = vec_data_s4_p1 >> 1;
        shifted_data_s5_p1 = vec_data_s5_p1 >> 1;
        shifted_data_s6_p1 = vec_data_s6_p1 >> 1;
        shifted_data_s7_p1 = vec_data_s7_p1 >> 1;

        // compute and store data[i]
        *(data + i) = shifted_data_s0_p0 ^ neg_lsb_s0_p0;
        *(data + i + 1) = shifted_data_s1_p0 ^ neg_lsb_s1_p0;
        *(data + i + 2) = shifted_data_s2_p0 ^ neg_lsb_s2_p0;
        *(data + i + 3) = shifted_data_s3_p0 ^ neg_lsb_s3_p0;
        *(data + i + 4) = shifted_data_s4_p0 ^ neg_lsb_s4_p0;
        *(data + i + 5) = shifted_data_s5_p0 ^ neg_lsb_s5_p0;
        *(data + i + 6) = shifted_data_s6_p0 ^ neg_lsb_s6_p0;
        *(data + i + 7) = shifted_data_s7_p0 ^ neg_lsb_s7_p0;
        *(data + i + 8) = shifted_data_s0_p1 ^ neg_lsb_s0_p1;
        *(data + i + 9) = shifted_data_s1_p1 ^ neg_lsb_s1_p1;
        *(data + i + 10) = shifted_data_s2_p1 ^ neg_lsb_s2_p1;
        *(data + i + 11) = shifted_data_s3_p1 ^ neg_lsb_s3_p1;
        *(data + i + 12) = shifted_data_s4_p1 ^ neg_lsb_s4_p1;
        *(data + i + 13) = shifted_data_s5_p1 ^ neg_lsb_s5_p1;
        *(data + i + 14) = shifted_data_s6_p1 ^ neg_lsb_s6_p1;
        *(data + i + 15) = shifted_data_s7_p1 ^ neg_lsb_s7_p1;
    }

    // handle left
    for (; i < numValues; i++) {
        data[i] = data[i] >> 1 ^ -(data[i] & 1);
    }
}

inline void BitsToBoolsBatch(bool *dest, uint8_t *source, uint64_t bitsLeft) {
    uint8_t* data = reinterpret_cast<uint8_t*>(dest);
    for (uint64_t i = 0; i < bitsLeft; i++) {
        uint64_t byteIndex = i / 8;
        uint64_t bitPosition = 7 - (i % 8);
        data[i] = !((source[byteIndex] >> bitPosition) & 0x1);
    }
}

inline void BitsToBoolsBatchHEFs0p1(bool *dest, uint8_t *source, uint64_t bitsLeft) {
    uint8_t* data = reinterpret_cast<uint8_t*>(dest);
    uint16_t *read = reinterpret_cast<uint16_t*>(source);

    // fill all zero in vector lane
    uint8x16_t zero = vdupq_n_u8(0);

    // Identify the bit in specified position
    uint8_t array[16] = {0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01,
                         0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01};
    uint8x16_t mask = vld1q_u8(array);

    uint64_t i = 0;
    // handle 16 elements in one batch
    for (; i + 16 <= bitsLeft; i += 16) {
        // load two bytes
        uint16_t value = read[i/16];
        uint8_t byte0 = (value >> 8) & 0xFF; // first byte
        uint8_t byte1 = value & 0xFF; // second byte

        uint8x8_t bits0 = vdup_n_u8(byte0);
        uint8x8_t bits1 = vdup_n_u8(byte1);
        uint8x16_t bits = vcombine_u8(bits1, bits0);
        uint8x16_t result = vmvnq_u8(bits);

        // compute masked result and compare with zero
        uint8x16_t expandedBytes = vcgtq_u8(vandq_u8(result, mask), zero);
        // move the most significant bit of each byte to the least significant bit
        uint8x16_t store = vshrq_n_u8(expandedBytes, 7);

        // store to data
        vst1q_u8(data + i, store);
    }

    // handle left
    for (; i < bitsLeft; i++) {
        uint64_t byteIndex = i / 8;
        uint64_t bitPosition = 7 - (i % 8);
        data[i] = !((source[byteIndex] >> bitPosition) & 0x1);
    }
}

struct BitNotFlip {
    BitNotFlip() {
        for (int i = 0; i < (1 << N); i++) {
            // 取反然后再翻转
            memo_[i] = flip(~i);
        }
    }

    static uint8_t flip(uint8_t byte) {
        return ((byte & 0x01) << 7) | ((byte & 0x02) << 5) | ((byte & 0x04) << 3) | ((byte & 0x08) << 1) |
               ((byte & 0x10) >> 1) |((byte & 0x20) >> 3) | ((byte & 0x40) >> 5) | ((byte & 0x80) >> 7);
    }

    uint8_t inline operator[](size_t i) const {
        return memo_[i];
    }

private:
    static constexpr int N = 8;
    uint8_t memo_[1 << N]{0};
};

const static BitNotFlip bitNotFlip;

inline void ReverseAndFlipBytes(uint8_t *bytes, int length) {
    for (int i = 0; i < length; i++) {
        bytes[i] = bitNotFlip[bytes[i]];
    }
}

struct NotNullBitMask {
    NotNullBitMask() {
        for (int i = 0; i < (1 << N); i++) {
            int32_t startIndex = i * (N + 1);
            int32_t index = startIndex;
            for (int bit = 0; bit < N; bit++) {
                if ((i & (1 << bit)) == 0) {
                    memo_[++index] = bit;
                }
            }
            memo_[startIndex] = index - startIndex;
        }
    }

    const inline uint8_t* operator[](size_t i) const {
        return memo_ + (i * (N + 1));
    }

private:
    static constexpr int N = 8;
    uint8_t memo_[(1 << N) * (N + 1)]{0};
};

const static NotNullBitMask notNullBitMask;

}

#endif // ORC_DECODE_UTILS