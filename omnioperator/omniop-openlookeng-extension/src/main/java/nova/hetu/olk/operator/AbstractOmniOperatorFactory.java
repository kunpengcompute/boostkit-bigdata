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

package nova.hetu.olk.operator;

import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import nova.hetu.omniruntime.constants.FunctionType;

import java.util.List;

import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_AVG;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_COUNT_ALL;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_SUM;

public abstract class AbstractOmniOperatorFactory
        implements OperatorFactory
{
    protected int operatorId;

    protected List<Type> sourceTypes;

    protected PlanNodeId planNodeId;

    @Override
    public abstract Operator createOperator(DriverContext driverContext);

    @Override
    public abstract void noMoreOperators();

    @Override
    public abstract OperatorFactory duplicate();

    @Override
    public boolean isExtensionOperatorFactory()
    {
        return true;
    }

    @Override
    public List<Type> getSourceTypes()
    {
        return sourceTypes;
    }

    public int getOperatorId()
    {
        return operatorId;
    }

    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    public void checkDataTypes(List<? extends Type> types)
    {
        for (Type type : types) {
            checkDataType(type);
        }
    }

    public static void checkDataTypes(final List<Type> sourceTypes,
                                      final FunctionType[] aggregatorTypes,
                                      final int[] aggregationInputChannels,
                                      final AggregationNode.Step step)
    {
        int channelIndex = 0;
        for (FunctionType fType : aggregatorTypes) {
            if (fType == OMNI_AGGREGATION_TYPE_COUNT_ALL) {
                continue;
            }

            String base = sourceTypes.get(aggregationInputChannels[channelIndex++]).getTypeSignature().getBase();
            switch (base) {
                case StandardTypes.INTEGER:
                case StandardTypes.BIGINT:
                case StandardTypes.DOUBLE:
                case StandardTypes.BOOLEAN:
                case StandardTypes.SMALLINT:
                case StandardTypes.VARCHAR:
                case StandardTypes.CHAR:
                case StandardTypes.DECIMAL:
                case StandardTypes.DATE:
                    continue;
                case StandardTypes.VARBINARY:
                    if (step == AggregationNode.Step.FINAL
                            && (fType == OMNI_AGGREGATION_TYPE_AVG || fType == OMNI_AGGREGATION_TYPE_SUM)) {
                        continue;
                    }
                case StandardTypes.ROW: {
                    if (step == AggregationNode.Step.FINAL && fType == OMNI_AGGREGATION_TYPE_AVG) {
                        continue;
                    }
                }
                default:
                    throw new PrestoException(
                            StandardErrorCode.NOT_SUPPORTED,
                            "Not support data Type " + base + " for aggregation " + fType + " with step " + step);
            }
        }
    }

    public void checkDataType(Type type)
    {
        TypeSignature signature = type.getTypeSignature();
        String base = signature.getBase();

        switch (base) {
            case StandardTypes.INTEGER:
            case StandardTypes.SMALLINT:
            case StandardTypes.BIGINT:
            case StandardTypes.DOUBLE:
            case StandardTypes.BOOLEAN:
            case StandardTypes.VARCHAR:
            case StandardTypes.CHAR:
            case StandardTypes.DECIMAL:
            case StandardTypes.DATE:
                return;
            default:
                throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support data Type " + base);
        }
    }
}
