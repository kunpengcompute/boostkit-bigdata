/*
 * Copyright (C) 2022-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.huawei.boostkit.omnidata.exception.OmniDataException;
import io.airlift.slice.SliceInput;
import com.huawei.boostkit.omnidata.decode.type.DecodeType;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Abstract decoding
 *
 * @param <T> decoding type
 * @since 2022-07-18
 */
public abstract class AbstractDecoding<T>
        implements Decoding<T>
{
    private static final Map<String, Method> DECODE_METHODS;

    static {
        DECODE_METHODS = new HashMap<>();
        Method[] methods = Decoding.class.getDeclaredMethods();
        for (Method method : methods) {
            if (method.isAnnotationPresent(Decode.class)) {
                DECODE_METHODS.put(method.getAnnotation(Decode.class).value(), method);
            }
        }
    }

    private Method getDecodeMethod(String decodeName)
    {
        return DECODE_METHODS.get(decodeName);
    }

    private String getDecodeName(SliceInput input)
    {
        byte[] bytes = new byte[10];

        return new String(bytes, StandardCharsets.UTF_8);
    }

    private Optional<String> typeToDecodeName(DecodeType type)
    {
        return Optional.empty();
    }

    private boolean[] getIsNullValue(byte value)
    {
        return new boolean[8];
    }

    @Override
    public T decode(Optional<DecodeType> type, SliceInput sliceInput)
    {
        throw new OmniDataException("decode failed");
    }

    /**
     * decode Null Bits
     *
     * @param sliceInput sliceInput
     * @param positionCount positionCount
     * @return decode boolean[]
     * @since 2022-07-18
     */
    public Optional<boolean[]> decodeNullBits(SliceInput sliceInput, int positionCount)
    {
        return Optional.empty();
    }
}
