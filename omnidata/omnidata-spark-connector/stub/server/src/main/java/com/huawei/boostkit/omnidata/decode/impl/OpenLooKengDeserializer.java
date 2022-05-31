/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package com.huawei.boostkit.omnidata.decode.impl;

import com.huawei.boostkit.omnidata.decode.Deserializer;

import io.airlift.compress.Decompressor;
import io.airlift.slice.Slice;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.Page;

/**
 * Deserialize block
 *
 * @since 2021-03-30
 */
public class OpenLooKengDeserializer implements Deserializer<Page> {

    /**
     * Constructor of deserialize block
     */
    public OpenLooKengDeserializer() {
    }

    /**
     * Decompress serialized page
     *
     * @param page         page need decompress
     * @param decompressor decompressor
     * @return Slice decompressed
     */
    public static Slice decompressPage(SerializedPage page, Decompressor decompressor) {
        return null;
    }

    @Override
    public Page deserialize(SerializedPage page) {
        return null;
    }
}
