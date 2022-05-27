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
import io.prestosql.execution.Lifespan;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.LocalPlannerAware;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.exchange.LocalExchange;
import io.prestosql.operator.exchange.LocalExchangeSink;
import io.prestosql.operator.exchange.LocalExchangeSinkOperator;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.operator.AbstractOmniOperatorFactory;

import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * The Local exchange sink omni operator.
 */
public class LocalExchangeSinkOmniOperator
        extends LocalExchangeSinkOperator
{
    public LocalExchangeSinkOmniOperator(String id, OperatorContext operatorContext, LocalExchangeSink sink,
                                         Function<Page, Page> pagePreprocessor)
    {
        super(id, operatorContext, sink, pagePreprocessor);
    }

    /**
     * The Local exchange sink omni operator factory.
     */
    public static class LocalExchangeSinkOmniOperatorFactory
            extends AbstractOmniOperatorFactory implements LocalPlannerAware
    {
        private final LocalExchange.LocalExchangeFactory localExchangeFactory;

        // There will be a LocalExchangeSinkFactory per LocalExchangeSinkOperatorFactory
        // per Driver Group.
        // A LocalExchangeSinkOperatorFactory needs to have access to
        // LocalExchangeSinkFactories for each Driver Group.
        private final LocalExchange.LocalExchangeSinkFactoryId sinkFactoryId;
        private final Function<Page, Page> pagePreprocessor;
        private boolean closed;

        public LocalExchangeSinkOmniOperatorFactory(LocalExchange.LocalExchangeFactory localExchangeFactory,
                                                    int operatorId, PlanNodeId planNodeId, LocalExchange.LocalExchangeSinkFactoryId sinkFactoryId,
                                                    Function<Page, Page> pagePreprocessor, List<Type> types)
        {
            this.localExchangeFactory = requireNonNull(localExchangeFactory, "localExchangeFactory is null");

            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sinkFactoryId = requireNonNull(sinkFactoryId, "sinkFactoryId is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(types, "sourceTypes is null"));
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId,
                    LocalExchangeSinkOmniOperator.class.getSimpleName());

            LocalExchange.LocalExchangeSinkFactory localExchangeSinkFactory = localExchangeFactory
                    .getLocalExchange(driverContext.getLifespan(), driverContext.getPipelineContext().getTaskContext(),
                            planNodeId.toString(), context.isSnapshotEnabled())
                    .getSinkFactory(sinkFactoryId);

            String sinkId = context.getUniqueId();
            return new LocalExchangeSinkOmniOperator(sinkId, context, localExchangeSinkFactory.createSink(sinkId),
                    pagePreprocessor);
        }

        @Override
        public void noMoreOperators()
        {
            if (!closed) {
                closed = true;
                localExchangeFactory.closeSinks(sinkFactoryId);
            }
        }

        @Override
        public void noMoreOperators(Lifespan lifespan)
        {
            localExchangeFactory.getLocalExchange(lifespan).getSinkFactory(sinkFactoryId).close();
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new LocalExchangeSinkOmniOperatorFactory(localExchangeFactory, operatorId, planNodeId,
                    localExchangeFactory.newSinkFactoryId(), pagePreprocessor, sourceTypes);
        }

        @Override
        public void localPlannerComplete()
        {
            localExchangeFactory.noMoreSinkFactories();
        }
    }
}
