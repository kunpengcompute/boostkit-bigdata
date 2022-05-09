/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package nova.hetu.olk.operator.filterandproject;

import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;
import nova.hetu.omniruntime.operator.project.OmniProjectOperatorFactory;

import java.util.List;

import static nova.hetu.olk.operator.filterandproject.OmniRowExpressionUtil.expressionStringify;
import static nova.hetu.olk.tool.OperatorUtils.toDataTypes;

/**
 * The type Omni projection.
 *
 * @since 20210630
 */
public class OmniProjection
{
    private final OmniProjectOperatorFactory omniProjectionFactory;

    private final int projectLength;
    private final boolean isSupported;

    /**
     * Instantiates a new Omni projection.
     *
     * @param expressions the expressions
     * @param inputTypes the input types
     */
    public OmniProjection(List<? extends RowExpression> expressions, List<Type> inputTypes,
                          OmniRowExpressionUtil.Format parseFormat)
    {
        this.projectLength = expressions.size();
        this.omniProjectionFactory = new OmniProjectOperatorFactory(
                expressions.stream().map(p -> expressionStringify(p, parseFormat)).toArray(String[]::new),
                toDataTypes(inputTypes), parseFormat.ordinal());
        this.isSupported = omniProjectionFactory.isSupported();
    }

    /**
     * Gets factory.
     *
     * @return the factory
     */
    public OmniProjectOperatorFactory getFactory()
    {
        return this.omniProjectionFactory;
    }

    /**
     * Is empty boolean.
     *
     * @return the boolean
     */
    public boolean isEmpty()
    {
        return this.projectLength == 0;
    }

    /**
     * Size int.
     *
     * @return the int
     */
    public int size()
    {
        return this.projectLength;
    }

    /**
     * Check if the projection is supported by OmniRuntime
     *
     * @return if the projection is supported
     */
    public boolean isSupported()
    {
        return isSupported;
    }
}
