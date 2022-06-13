/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#include "Buffer.h"

// Return the number of bytes needed to fit the given number of bits
int64_t BytesForBits(int64_t bits)
{
    // This formula avoids integer overflow on very large `bits`
    return (bits >> 3) + ((bits & 7) != 0);
}