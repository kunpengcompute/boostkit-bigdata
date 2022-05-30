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
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.PagesSerdeFactory;
import io.prestosql.metadata.Split;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.ExchangeClient;
import io.prestosql.operator.ExchangeClientSupplier;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.SourceOperator;
import io.prestosql.operator.SourceOperatorFactory;
import io.prestosql.operator.TaskLocation;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import io.prestosql.split.RemoteSplit;
import io.prestosql.sql.gen.OrderingCompiler;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.omniruntime.vector.VecAllocator;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.operator.OrderByOmniOperator.OrderByOmniOperatorFactory.createOrderByOmniOperatorFactory;

/**
 * The type Merge omni operator.
 *
 * @since 20210630
 */
public class MergeOmniOperator
        implements SourceOperator, Closeable
{
    /**
     * The type Merge omni operator factory.
     *
     * @since 20210630
     */
    public static class MergeOmniOperatorFactory
            extends AbstractOmniOperatorFactory
            implements SourceOperatorFactory
    {
        private final ExchangeClientSupplier exchangeClientSupplier;

        private final PagesSerdeFactory serdeFactory;

        private boolean closed;

        private final OrderByOmniOperator.OrderByOmniOperatorFactory orderByOmniOperatorFactory;

        /**
         * Instantiates a new Merge omni operator factory.
         *
         * @param operatorId the operator id
         * @param omniMergeId the omni merge id
         * @param sourceId the source id
         * @param exchangeClientSupplier the exchange client supplier
         * @param serdeFactory the serde factory
         * @param orderingCompiler the ordering compiler
         * @param types the types
         * @param outputChannels the output channels
         * @param sortChannels the sort channels
         * @param sortOrder the sort order
         */
        public MergeOmniOperatorFactory(int operatorId, int omniMergeId, PlanNodeId sourceId,
                                        ExchangeClientSupplier exchangeClientSupplier, PagesSerdeFactory serdeFactory,
                                        OrderingCompiler orderingCompiler, List<Type> types, List<Integer> outputChannels,
                                        List<Integer> sortChannels, List<SortOrder> sortOrder)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(sourceId, "sourceId is null");
            this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
            this.serdeFactory = requireNonNull(serdeFactory, "serdeFactory is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(types, "sourceTypes is null"));
            this.orderByOmniOperatorFactory = createOrderByOmniOperatorFactory(omniMergeId, sourceId, types,
                    outputChannels, sortChannels, sortOrder);
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return planNodeId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            VecAllocator vecAllocator = VecAllocatorHelper.createOperatorLevelAllocator(driverContext,
                    VecAllocator.UNLIMIT, MergeOmniOperator.class);

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    MergeOmniOperator.class.getSimpleName());

            return new MergeOmniOperator(operatorContext, planNodeId, exchangeClientSupplier,
                    serdeFactory.createPagesSerde(), orderByOmniOperatorFactory.createOperator(vecAllocator));
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
    }

    private final OperatorContext operatorContext;

    private final PlanNodeId sourceId;

    private final ExchangeClientSupplier exchangeClientSupplier;

    private final PagesSerde pagesSerde;

    private final SettableFuture<Void> blockedOnSplits = SettableFuture.create();

    private final List<ExchangeClient> pageProducers = new ArrayList<>();

    private final Closer closer = Closer.create();

    private boolean closed;

    private final String id;

    private final OrderByOmniOperator orderByOmniOperator;

    private boolean isFinished;

    /**
     * Instantiates a new Merge omni operator.
     *
     * @param operatorContext the operator context
     * @param sourceId the source id
     * @param exchangeClientSupplier the exchange client supplier
     * @param pagesSerde the pages serde
     * @param orderByOmniOperator the order by omni operator
     */
    public MergeOmniOperator(OperatorContext operatorContext, PlanNodeId sourceId,
                             ExchangeClientSupplier exchangeClientSupplier, PagesSerde pagesSerde, Operator orderByOmniOperator)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
        this.id = operatorContext.getUniqueId();
        this.orderByOmniOperator = (OrderByOmniOperator) orderByOmniOperator;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        checkArgument(split.getConnectorSplit() instanceof RemoteSplit, "split is not a remote split");
        checkState(!blockedOnSplits.isDone(), "noMoreSplits has been called already");

        URI location = ((RemoteSplit) split.getConnectorSplit()).getLocation();
        String instanceId = ((RemoteSplit) split.getConnectorSplit()).getInstanceId();
        ExchangeClient exchangeClient = closer
                .register(exchangeClientSupplier.get(operatorContext.localSystemMemoryContext()));
        exchangeClient.addTarget(id);
        exchangeClient.noMoreTargets();
        exchangeClient.addLocation(new TaskLocation(location, instanceId));
        exchangeClient.noMoreLocations();

        pageProducers.add(exchangeClient);
        return Optional::empty;
    }

    @Override
    public void noMoreSplits()
    {
        blockedOnSplits.set(null);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        close();
    }

    @Override
    public boolean isFinished()
    {
        return closed || orderByOmniOperator.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!blockedOnSplits.isDone()) {
            return blockedOnSplits;
        }

        // use the first exchange client as the signal if data are ready
        if (pageProducers.size() > 0) {
            ListenableFuture<?> future = pageProducers.get(0).isBlocked();
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
        throw new UnsupportedOperationException(getClass().getName() + " can not take input");
    }

    @Override
    public Page getOutput()
    {
        if (closed) {
            return null;
        }

        if (!isFinished && isSourceFinished()) {
            ImmutableList<List<Page>> pageCollections = pageProducers.stream().map(exchangeClient -> exchangeClient.getPages(id, pagesSerde))
                    .collect(toImmutableList());
            for (List<Page> pageList : pageCollections) {
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
    public void close()
    {
        try {
            closer.close();
            closed = true;
            orderByOmniOperator.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean isSourceFinished()
    {
        for (ExchangeClient exchangeClient : pageProducers) {
            if (exchangeClient.getPages(id, pagesSerde) == null) {
                return false;
            }
        }
        return true;
    }
}
