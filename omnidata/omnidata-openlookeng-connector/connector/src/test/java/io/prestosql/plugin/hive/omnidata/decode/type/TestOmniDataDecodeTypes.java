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

import com.huawei.boostkit.omnidata.decode.type.DecimalDecodeType;
import com.huawei.boostkit.omnidata.decode.type.DecodeType;
import com.huawei.boostkit.omnidata.decode.type.TimestampDecodeType;
import org.testng.annotations.Test;

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
    }
}
