/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package com.huawei.boostkit.omnidata.decode;

import com.huawei.boostkit.omnidata.decode.type.DecodeType;

import io.airlift.slice.SliceInput;

import java.lang.reflect.Method;
import java.util.Optional;

/**
 * Abstract decoding
 *
 * @param <T> decoding type
 * @since 2021-07-31
 */
public abstract class AbstractDecoding<T> implements Decoding<T> {

    private Method getDecodeMethod(String decodeName) {
        return null;
    }

    private String getDecodeName(SliceInput input) {
        return null;
    }

    private Optional<String> typeToDecodeName(DecodeType type) {
        return null;
    }

    @Override
    public T decode(Optional<DecodeType> type, SliceInput sliceInput) {
        return null;
    }

    public Optional<boolean[]> decodeNullBits(SliceInput sliceInput, int positionCount) {
        return null;
    }

    private boolean[] getIsNullValue(byte value) {
        return null;
    }
}
