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

package io.prestosql.plugin.hive.omnidata.decode;

import com.huawei.boostkit.omnidata.exception.OmniDataException;
import io.airlift.slice.SliceInput;
import io.prestosql.plugin.hive.omnidata.decode.type.DecimalDecodeType;
import io.prestosql.plugin.hive.omnidata.decode.type.DecodeType;
import io.prestosql.plugin.hive.omnidata.decode.type.LongToByteDecodeType;
import io.prestosql.plugin.hive.omnidata.decode.type.LongToFloatDecodeType;
import io.prestosql.plugin.hive.omnidata.decode.type.LongToIntDecodeType;
import io.prestosql.plugin.hive.omnidata.decode.type.LongToShortDecodeType;
import io.prestosql.plugin.hive.omnidata.decode.type.TimestampDecodeType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.RowType;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

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
        int length = input.readInt();
        byte[] bytes = new byte[length];
        input.readBytes(bytes);

        return new String(bytes, StandardCharsets.UTF_8);
    }

    private Optional<String> typeToDecodeName(DecodeType type)
    {
        Class<?> javaType = null;
        if (type.getJavaType().isPresent()) {
            javaType = type.getJavaType().get();
        }
        if (javaType == double.class) {
            return Optional.of("DOUBLE_ARRAY");
        }
        else if (javaType == float.class) {
            return Optional.of("FLOAT_ARRAY");
        }
        else if (javaType == int.class) {
            return Optional.of("INT_ARRAY");
        }
        else if (javaType == long.class) {
            return Optional.of("LONG_ARRAY");
        }
        else if (javaType == byte.class) {
            return Optional.of("BYTE_ARRAY");
        }
        else if (javaType == boolean.class) {
            return Optional.of("BOOLEAN_ARRAY");
        }
        else if (javaType == short.class) {
            return Optional.of("SHORT_ARRAY");
        }
        else if (javaType == String.class) {
            return Optional.of("VARIABLE_WIDTH");
        }
        else if (javaType == RowType.class) {
            return Optional.of("ROW");
        }
        else if (javaType == DateType.class) {
            return Optional.of("DATE");
        }
        else if (javaType == LongToIntDecodeType.class) {
            return Optional.of("LONG_TO_INT");
        }
        else if (javaType == LongToShortDecodeType.class) {
            return Optional.of("LONG_TO_SHORT");
        }
        else if (javaType == LongToByteDecodeType.class) {
            return Optional.of("LONG_TO_BYTE");
        }
        else if (javaType == LongToFloatDecodeType.class) {
            return Optional.of("LONG_TO_FLOAT");
        }
        else if (javaType == DecimalDecodeType.class) {
            return Optional.of("DECIMAL");
        }
        else if (javaType == TimestampDecodeType.class) {
            return Optional.of("TIMESTAMP");
        }
        else {
            return Optional.empty();
        }
    }

    private boolean[] getIsNullValue(byte value)
    {
        boolean[] isNullValue = new boolean[8];
        isNullValue[0] = ((value & 0b1000_0000) != 0);
        isNullValue[1] = ((value & 0b0100_0000) != 0);
        isNullValue[2] = ((value & 0b0010_0000) != 0);
        isNullValue[3] = ((value & 0b0001_0000) != 0);
        isNullValue[4] = ((value & 0b0000_1000) != 0);
        isNullValue[5] = ((value & 0b0000_0100) != 0);
        isNullValue[6] = ((value & 0b0000_0010) != 0);
        isNullValue[7] = ((value & 0b0000_0001) != 0);

        return isNullValue;
    }

    @Override
    public T decode(Optional<DecodeType> type, SliceInput sliceInput)
    {
        try {
            String decodeName = getDecodeName(sliceInput);
            if (type.isPresent()) {
                Optional<String> decodeNameOpt = typeToDecodeName(type.get());
                if ("DECIMAL".equals(decodeNameOpt.orElse(decodeName)) && !"RLE".equals(decodeName)) {
                    Method method = getDecodeMethod("DECIMAL");
                    return (T) method.invoke(this, type, sliceInput, decodeName);
                }
                if (!"RLE".equals(decodeName)) {
                    decodeName = decodeNameOpt.orElse(decodeName);
                }
            }
            Method method = getDecodeMethod(decodeName);
            return (T) method.invoke(this, type, sliceInput);
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new OmniDataException("decode failed " + e.getMessage());
        }
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
        if (!sliceInput.readBoolean()) {
            return Optional.empty();
        }

        // read null bits 8 at a time
        boolean[] valueIsNull = new boolean[positionCount];
        for (int position = 0; position < (positionCount & ~0b111); position += 8) {
            boolean[] nextEightValue = getIsNullValue(sliceInput.readByte());
            int finalPosition = position;
            IntStream.range(0, 8).forEach(pos -> valueIsNull[finalPosition + pos] = nextEightValue[pos]);
        }

        // read last null bits
        if ((positionCount & 0b111) > 0) {
            byte value = sliceInput.readByte();
            int maskInt = 0b1000_0000;
            for (int pos = positionCount & ~0b111; pos < positionCount; pos++) {
                valueIsNull[pos] = ((value & maskInt) != 0);
                maskInt >>>= 1;
            }
        }

        return Optional.of(valueIsNull);
    }
}
