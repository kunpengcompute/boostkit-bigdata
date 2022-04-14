/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
 */
package com.huawei.boostkit.hbase.index;

/**
 * Value info
 *
 * @since 2021.09
 */
public class ValueInfo {
    public final long blockOffset;
    public final int blockLength;

    public ValueInfo(long blockOffset, int blockLength) {
        this.blockOffset = blockOffset;
        this.blockLength = blockLength;
    }
}
