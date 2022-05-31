/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package com.huawei.boostkit.omnidata.decode.type;


import java.util.Optional;

/**
 * Decimal decode type
 *
 * @since 2021-07-31
 */
public class DecimalDecodeType implements DecodeType {

    public int getPrecision() {
        return 0;
    }

    public int getScale() {
        return 0;
    }

    public DecimalDecodeType(int v1, int v2) {
    }

    @Override
    public Optional<Class<?>> getJavaType() {
        return Optional.of(DecimalDecodeType.class);
    }
}

