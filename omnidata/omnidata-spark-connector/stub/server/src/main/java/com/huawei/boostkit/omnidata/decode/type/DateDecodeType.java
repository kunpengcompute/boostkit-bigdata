/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package com.huawei.boostkit.omnidata.decode.type;


import java.util.Optional;

/**
 * Date Decode Type
 *
 * @since 2021-07-31
 */
public class DateDecodeType implements DecodeType {
    @Override
    public Optional<Class<?>> getJavaType() {
        return Optional.empty();
    }
}

