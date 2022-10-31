/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
