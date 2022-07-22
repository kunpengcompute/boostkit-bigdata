/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.prestosql.plugin.hive.omnidata.decode.type;

import io.prestosql.spi.type.RowType;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

/**
 * Test All OmniData DecodeTypes
 *
 * @since 2022-07-18
 */
public class TestOmniDataDecodeTypes
{
    @Test
    public void testOmnidataDecodeTypes()
    {
        // Test DecimalDecodeType
        DecodeType decimalDecodeType = new DecimalDecodeType(10, 5);
        assertEquals(decimalDecodeType.getJavaType().get(), DecimalDecodeType.class);

        // Test TimestampDecodeType
        DecodeType timestampDecodeType = new TimestampDecodeType();
        assertEquals(timestampDecodeType.getJavaType().get(), TimestampDecodeType.class);

        // Test VarcharDecodeType
        DecodeType varcharDecodeType = new VarcharDecodeType();
        assertEquals(varcharDecodeType.getJavaType().get(), String.class);

        // Test ShortDecodeType
        DecodeType shortDecodeType = new ShortDecodeType();
        assertEquals(shortDecodeType.getJavaType().get(), short.class);

        // Test RowDecodeType
        DecodeType rowDecodeType = new RowDecodeType();
        assertEquals(rowDecodeType.getJavaType().get(), RowType.class);

        // Test MapDecodeType
        DecodeType mapDecodeType = new MapDecodeType(new ShortDecodeType(), new ShortDecodeType());
        assertEquals(mapDecodeType.getJavaType(), Optional.empty());

        // Test IntDecodeType
        DecodeType intDecodeType = new IntDecodeType();
        assertEquals(intDecodeType.getJavaType().get(), int.class);

        // Test FloatDecodeType
        DecodeType floatDecodeType = new FloatDecodeType();
        assertEquals(floatDecodeType.getJavaType().get(), float.class);

        // Test DoubleDecodeType
        DecodeType doubleDecodeType = new DoubleDecodeType();
        assertEquals(doubleDecodeType.getJavaType().get(), double.class);

        // Test ByteDecodeType
        DecodeType byteDecodeType = new ByteDecodeType();
        assertEquals(byteDecodeType.getJavaType().get(), byte.class);

        // Test BooleanDecodeType
        DecodeType booleanDecodeType = new BooleanDecodeType();
        assertEquals(booleanDecodeType.getJavaType().get(), boolean.class);
    }
}
