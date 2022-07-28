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

package org.apache.hadoop.hive.ql.omnidata.decode.type;

import java.util.Optional;

/**
 * MapDecode type
 *
 * @param <K> k
 * @param <V> v
 * @since 2022-07-28
 */
public class MapDecodeType<K extends DecodeType, V extends DecodeType>
        implements DecodeType
{
    private final K keyType;
    private final V valueType;

    public MapDecodeType(K keyType, V valueType)
    {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public Optional<Class<?>> getJavaType()
    {
        return Optional.empty();
    }
}
