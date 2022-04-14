/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
 */
package com.huawei.boostkit.hbase.index;

/**
 * Illegal Memory Request Exception
 *
 * @since 2021.09
 */
public class IllegalMemoryRequestException extends Exception {
    public IllegalMemoryRequestException(long requestMemorySize) {
    }
}
