/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
 */
package com.huawei.boostkit.hbase.index;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Louds Tries Map service
 *
 * @since 2021.09
 */
public class LoudsTriesService {

    public static void build(List<byte[]> keys, long[] offsets, int[] lengths, DataOutput out) throws IOException {
    }

    public static ValueInfo get(ByteBuffer buff, byte[] key, int offset, int length) {
        return null;
    }
}
