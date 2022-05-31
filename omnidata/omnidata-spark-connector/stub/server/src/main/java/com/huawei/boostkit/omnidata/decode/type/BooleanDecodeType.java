/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package com.huawei.boostkit.omnidata.decode.type;


import java.util.Optional;

/**
 * Boolean decode type
 *
 * @since 2021-07-31
 */
public class BooleanDecodeType implements DecodeType {
    @Override
    public Optional<Class<?>> getJavaType() {
        return Optional.empty();
    }
}

