/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
 */
package com.huawei.boostkit.hbase.index;

/**
 * Insufficient memory exception
 *
 * @since 2021.09
 */
public class InsufficientMemoryException extends Exception {
    public InsufficientMemoryException(long memoryGap) {
    }
}
