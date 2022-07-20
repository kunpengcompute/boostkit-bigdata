/*
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

import java.util.Optional;

/**
 * Decimal decode type
 *
 * @since 2022-07-18
 */
public class DecimalDecodeType
        implements DecodeType
{
    private final int precision;
    private final int scale;

    public DecimalDecodeType(int precision, int scale)
    {
        this.precision = precision;
        this.scale = scale;
    }

    public int getPrecision()
    {
        return precision;
    }

    public int getScale()
    {
        return scale;
    }

    @Override
    public Optional<Class<?>> getJavaType()
    {
        return Optional.of(DecimalDecodeType.class);
    }
}
