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
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.MoreExecutors;
import io.prestosql.execution.Lifespan;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.PipelineContext;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.TestingSession;
import io.prestosql.testing.TestingTaskContext;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.sort.OmniSortOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.transform;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static nova.hetu.olk.tool.OperatorUtils.buildVecBatch;
import static nova.hetu.olk.tool.OperatorUtils.createExpressions;

/**
 * The type Order by omni operator.
 *
 * @since 20210630
 */
public class OrderByOmniOperator
        implements Operator
{
    /**
     * The type Order by omni operator factory.
     *
     * @since 20210630
     */
    public static class OrderByOmniOperatorFactory
            extends AbstractOmniOperatorFactory
    {
        private final int[] outputChannels;

        private final int[] sortChannels;

        private final int[] sortAscendings;

        private final int[] sortNullFirsts;

        private final OmniSortOperatorFactory omniSortOperatorFactory;

        private boolean closed;

        /**
         * Create order by omni operator factory order by omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         * @param sourceTypes the source types
         * @param outputChannels the output channels
         * @param sortChannels the sort channels
         * @param sortOrder the sort order
         * @return the order by omni operator factory
         */
        public static OrderByOmniOperatorFactory createOrderByOmniOperatorFactory(int operatorId, PlanNodeId planNodeId,
                                                                                  List<? extends Type> sourceTypes, List<Integer> outputChannels, List<Integer> sortChannels,
                                                                                  List<SortOrder> sortOrder)
        {
            DataType[] types = OperatorUtils.toDataTypes(sourceTypes);

            int sortColSize = sortChannels.size();
            int[] ascendings = new int[sortColSize];
            int[] nullFirsts = new int[sortColSize];
            for (int i = 0; i < sortColSize; i++) {
                SortOrder order = sortOrder.get(i);
                ascendings[i] = order.isAscending() ? 1 : 0;
                nullFirsts[i] = order.isNullsFirst() ? 1 : 0;
            }

            OmniSortOperatorFactory omniSortOperatorFactory = new OmniSortOperatorFactory(types,
                    Ints.toArray(outputChannels), createExpressions(sortChannels), ascendings, nullFirsts);

            return new OrderByOmniOperatorFactory(operatorId, planNodeId, (List<Type>) sourceTypes,
                    Ints.toArray(outputChannels), Ints.toArray(sortChannels), ascendings, nullFirsts,
                    omniSortOperatorFactory);
        }

        /**
         * Instantiates a new Order by omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         * @param sourceTypes the source types
         * @param outputChannels the output channels
         * @param sortChannels the sort channels
         * @param sortAscendings the sort ascendings
         * @param sortNullFirsts the sort null firsts
         * @param omniSortOperatorFactory the omni sort operator factory
         */
        public OrderByOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Type> sourceTypes,
                                          int[] outputChannels, int[] sortChannels, int[] sortAscendings, int[] sortNullFirsts,
                                          OmniSortOperatorFactory omniSortOperatorFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.outputChannels = outputChannels;
            this.sortChannels = sortChannels;
            this.sortAscendings = sortAscendings;
            this.sortNullFirsts = sortNullFirsts;
            this.omniSortOperatorFactory = omniSortOperatorFactory;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            VecAllocator vecAllocator = VecAllocatorHelper.createOperatorLevelAllocator(driverContext,
                    VecAllocator.UNLIMIT, OrderByOmniOperator.class);

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    OrderByOmniOperator.class.getSimpleName());
            OmniOperator omniSortOperator = omniSortOperatorFactory.createOperator(vecAllocator);
            return new OrderByOmniOperator(operatorContext, omniSortOperator);
        }

        /**
         * Create operator operator.
         *
         * @return the operator
         */
        public Operator createOperator(VecAllocator vecAllocator)
        {
            // all this is prepared for a fake driverContext to avoid change the original
            // pipeline
            Executor mockExecutor = MoreExecutors.directExecutor();
            ScheduledExecutorService mockScheduledExecutorService = newSingleThreadScheduledExecutor();
            TaskContext mockTaskContext = TestingTaskContext.createTaskContext(mockExecutor,
                    mockScheduledExecutorService, TestingSession.testSessionBuilder().build());
            MemoryTrackingContext mockMemoryTrackingContext = new MemoryTrackingContext(
                    newSimpleAggregatedMemoryContext(), newSimpleAggregatedMemoryContext(),
                    newSimpleAggregatedMemoryContext());
            PipelineContext mockPipelineContext = new PipelineContext(1, mockTaskContext, mockExecutor,
                    mockScheduledExecutorService, mockMemoryTrackingContext, false, false, false);
            DriverContext mockDriverContext = new DriverContext(mockPipelineContext, mockExecutor,
                    mockScheduledExecutorService, mockMemoryTrackingContext, Lifespan.taskWide(), 0);
            OperatorContext mockOperatorContext = mockDriverContext.addOperatorContext(1,
                    new PlanNodeId("Fake node for creating the OrderByOmniOperator"), "OrderByOmniOperator type");

            OmniOperator omniSortOperator = omniSortOperatorFactory.createOperator(vecAllocator);
            return new OrderByOmniOperator(mockOperatorContext, omniSortOperator);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new OrderByOmniOperatorFactory(operatorId, planNodeId, sourceTypes, outputChannels, sortChannels,
                    sortAscendings, sortNullFirsts, omniSortOperatorFactory);
        }
    }

    private enum State
    {
        /**
         * Needs input state.
         */
        NEEDS_INPUT,
        /**
         * Has output state.
         */
        HAS_OUTPUT,
        /**
         * Finished state.
         */
        FINISHED
    }

    private final OperatorContext operatorContext;

    private final OmniOperator omniOperator;

    private Iterator<Optional<Page>> sortedPages;

    private State state = State.NEEDS_INPUT;

    /**
     * Instantiates a new Order by omni operator.
     *
     * @param operatorContext the operator context
     * @param omniOperator the omni operator
     */
    public OrderByOmniOperator(OperatorContext operatorContext, OmniOperator omniOperator)
    {
        this.operatorContext = operatorContext;
        this.omniOperator = omniOperator;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(state == State.NEEDS_INPUT, "Operator is already finishing");
        requireNonNull(page, "page is null");

        int positionCount = page.getPositionCount();
        if (positionCount == 0) {
            return;
        }

        VecBatch vecBatch = buildVecBatch(omniOperator.getVecAllocator(), page, this);
        omniOperator.addInput(vecBatch);
    }

    @Override
    public Page getOutput()
    {
        if (state != State.HAS_OUTPUT) {
            return null;
        }

        if (!sortedPages.hasNext()) {
            state = State.FINISHED;
            return null;
        }

        Optional<Page> next = sortedPages.next();
        if (!next.isPresent()) {
            return null;
        }
        Page nextPage = next.get();

        return nextPage;
    }

    @Override
    public void finish()
    {
        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;
            sortedPages = transform(new VecBatchToPageIterator(omniOperator.getOutput()), Optional::of);
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    @Override
    public void close()
    {
        omniOperator.close();
    }
}
