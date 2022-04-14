/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
 */
package com.huawei.boostkit.hbase.index;

/**
 * Direct memory allocator with maximum memory
 * memory is free manually
 *
 * @since 2021.09
 */
public class NativeMaxLimitAllocator {


    public NativeMaxLimitAllocator(long maxMemorySize) {
    }

    public AllocatedMemory allocate(int size) throws InsufficientMemoryException, IllegalMemoryRequestException {
        return null;
    }

    public long getMaxMemory() {
        return 0;
    }

    public long getMemoryUsed() {
        return 0;
    }
}
