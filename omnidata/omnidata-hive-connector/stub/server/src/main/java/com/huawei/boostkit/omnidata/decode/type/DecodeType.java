/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package com.huawei.boostkit.omnidata.decode.type;


import java.util.Optional;

/**
 * Decode java type
 *
 * @since 2020-07-31
 */
public interface DecodeType {
    Optional<Class<?>> getJavaType();
}

