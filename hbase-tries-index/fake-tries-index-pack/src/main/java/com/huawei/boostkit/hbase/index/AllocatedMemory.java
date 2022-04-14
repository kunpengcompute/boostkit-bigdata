/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
 */
package com.huawei.boostkit.hbase.index;

import java.nio.ByteBuffer;

/**
 * Allocated memory by unsafe
 *
 * @since 2021.09
 */
public class AllocatedMemory {
    public ByteBuffer getBuf() {
        return null;
    }

    public void retain() {
    }

    public void release() {
    }

    public int refCnt() {
        return 0;
    }

    public int size() {return 0;}
}
