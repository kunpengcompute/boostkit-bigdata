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

package nova.hetu.olk.operator.localexchange;

import com.google.common.collect.ImmutableList;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.exchange.LocalExchange;
import io.prestosql.operator.exchange.LocalExchangeSource;
import io.prestosql.operator.exchange.LocalExchangeSourceOperator;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.operator.AbstractOmniOperatorFactory;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * The Local exchange source omni operator.
 */
public class LocalExchangeSourceOmniOperator
        extends LocalExchangeSourceOperator
{
    public LocalExchangeSourceOmniOperator(OperatorContext operatorContext, LocalExchangeSource source,
                                           int totalInputChannels)
    {
        super(operatorContext, source, totalInputChannels);
    }

    /**
     * The Local exchange source omni operator factory.
     */
    public static class LocalExchangeSourceOmniOperatorFactory
            extends AbstractOmniOperatorFactory
    {
        private final LocalExchange.LocalExchangeFactory localExchangeFactory;
        private final int totalInputChannels;
        private boolean closed;

        public LocalExchangeSourceOmniOperatorFactory(int operatorId, PlanNodeId planNodeId,
                                                      LocalExchange.LocalExchangeFactory localExchangeFactory, int totalInputChannels, List<Type> types)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.localExchangeFactory = requireNonNull(localExchangeFactory, "localExchangeFactory is null");
            this.totalInputChannels = totalInputChannels;
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(types, "sourceTypes is null"));
            checkDataTypes(this.sourceTypes);
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            LocalExchange inMemoryExchange = localExchangeFactory.getLocalExchange(driverContext.getLifespan(),
                    driverContext.getPipelineContext().getTaskContext());

            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId,
                    LocalExchangeSourceOmniOperator.class.getSimpleName());
            LocalExchangeSource localExchangeSource = inMemoryExchange.getNextSource();

            return new LocalExchangeSourceOmniOperator(context, localExchangeSource, totalInputChannels);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Source operator factories can not be duplicated");
        }

        public LocalExchange.LocalExchangeFactory getLocalExchangeFactory()
        {
            return localExchangeFactory;
        }
    }
}
