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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.exchange.LocalExchange;
import io.prestosql.operator.exchange.LocalExchangeSource;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.OrderingCompiler;
import nova.hetu.olk.operator.localexchange.OmniLocalExchange;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.omniruntime.vector.VecAllocator;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.operator.OrderByOmniOperator.OrderByOmniOperatorFactory.createOrderByOmniOperatorFactory;

/**
 * The type Local merge source omni operator.
 *
 * @since 20210630
 */
public class LocalMergeSourceOmniOperator
        implements Operator
{
    /**
     * The type Local merge source omni operator factory.
     *
     * @since 20210630
     */
    public static class LocalMergeSourceOmniOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;

        private final PlanNodeId planNodeId;

        private final OmniLocalExchange.LocalExchangeFactory localExchangeFactory;

        private final List<Type> sourceTypes;

        private boolean closed;

        private final OrderByOmniOperator.OrderByOmniOperatorFactory orderByOmniOperatorFactory;

        /**
         * Instantiates a new Local merge source omni operator factory.
         *
         * @param operatorId the operator id
         * @param orderByOmniId the order by omni id
         * @param planNodeId the plan node id
         * @param localExchangeFactory the local exchange factory
         * @param types the types
         * @param orderingCompiler the ordering compiler
         * @param sortChannels the sort channels
         * @param orderings the orderings
         * @param outputChannels the output channels
         */
        public LocalMergeSourceOmniOperatorFactory(int operatorId, int orderByOmniId, PlanNodeId planNodeId,
                                                   LocalExchange.LocalExchangeFactory localExchangeFactory, List<Type> types,
                                                   OrderingCompiler orderingCompiler, List<Integer> sortChannels, List<SortOrder> orderings,
                                                   List<Integer> outputChannels)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.localExchangeFactory = requireNonNull(localExchangeFactory, "exchange is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(types, "sourceTypes is null"));

            this.orderByOmniOperatorFactory = createOrderByOmniOperatorFactory(orderByOmniId, planNodeId, types,
                    outputChannels, sortChannels, orderings);
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            VecAllocator vecAllocator = VecAllocatorHelper.createOperatorLevelAllocator(driverContext,
                    VecAllocator.UNLIMIT, LocalMergeSourceOmniOperator.class);

            LocalExchange localExchange = localExchangeFactory.getLocalExchange(driverContext.getLifespan());

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    LocalMergeSourceOmniOperator.class.getSimpleName());

            List<LocalExchangeSource> sources = IntStream.range(0, localExchange.getBufferCount()).boxed()
                    .map(index -> localExchange.getNextSource()).collect(toImmutableList());
            return new LocalMergeSourceOmniOperator(operatorContext, sources,
                    orderByOmniOperatorFactory.createOperator(vecAllocator));
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
    }

    private final OperatorContext operatorContext;

    private final List<LocalExchangeSource> sources;

    private final OrderByOmniOperator orderByOmniOperator;

    private boolean isFinished;

    /**
     * Instantiates a new Local merge source omni operator.
     *
     * @param operatorContext the operator context
     * @param sources the sources
     * @param orderByOmniOperator the order by omni operator
     */
    public LocalMergeSourceOmniOperator(OperatorContext operatorContext, List<LocalExchangeSource> sources,
                                        Operator orderByOmniOperator)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sources = requireNonNull(sources, "sources is null");
        this.orderByOmniOperator = (OrderByOmniOperator) orderByOmniOperator;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        sources.forEach(LocalExchangeSource::finish);
    }

    @Override
    public boolean isFinished()
    {
        return orderByOmniOperator.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        // use the first exchange client as the signal if data are ready
        if (sources.size() > 0) {
            ListenableFuture<?> future = sources.get(0).waitForReading();
            if (!future.isDone()) {
                return future;
            }
        }
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        if (!isFinished && isSourceFinished()) {
            ImmutableList<List<Page>> pageProducers = sources.stream().map(LocalExchangeSource::getPages)
                    .collect(toImmutableList());
            for (List<Page> pageList : pageProducers) {
                for (Page page : pageList) {
                    this.orderByOmniOperator.addInput(page);
                }
            }
            orderByOmniOperator.finish();
            isFinished = true;
        }

        Page page = orderByOmniOperator.getOutput();
        if (page != null) {
            operatorContext.recordProcessedInput(page.getSizeInBytes(), page.getPositionCount());
        }
        return page;
    }

    @Override
    public void close() throws IOException
    {
        sources.forEach(LocalExchangeSource::close);
        orderByOmniOperator.close();
    }

    private boolean isSourceFinished()
    {
        for (LocalExchangeSource localExchangeSource : sources) {
            if (localExchangeSource.getPages() == null) {
                return false;
            }
        }
        return true;
    }
}
