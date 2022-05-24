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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.prestosql.execution.Lifespan;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.AggregationNode.Step;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.tool.BlockUtils;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.aggregator.OmniHashAggregationOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.buildVecBatch;
import static nova.hetu.olk.tool.OperatorUtils.createExpressions;

/**
 * The type Hash aggregation omni operator.
 *
 * @since 20210630
 */
public class HashAggregationOmniOperator
        implements Operator
{
    private static final Logger log = Logger.get(HashAggregationOmniOperator.class);

    private final OmniOperator omniOperator;

    private final OperatorContext operatorContext;

    private final Step step;

    /**
     * The Pages.
     */
    Iterator<Page> pages;

    private boolean finishing;

    private boolean finished;

    /**
     * Instantiates a new Hash aggregation omni operator.
     *
     * @param operatorContext the operator context
     * @param omniOperator the omni operator
     */
    public HashAggregationOmniOperator(OperatorContext operatorContext, OmniOperator omniOperator, Step step)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null.");
        this.omniOperator = requireNonNull(omniOperator, "omniOperator is null.");
        this.step = step;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return this.operatorContext;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public void close()
    {
        // free pages if it has next
        if (pages != null) {
            while (pages.hasNext()) {
                Page next = pages.next();
                BlockUtils.freePage(next);
            }
        }
        omniOperator.close();
    }

    @Override
    public boolean needsInput()
    {
        return !finishing;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        VecBatch vecBatch = buildVecBatch(omniOperator.getVecAllocator(), page, this);
        omniOperator.addInput(vecBatch);
    }

    @Override
    public Page getOutput()
    {
        if (finished) {
            return null;
        }
        if (finishing) {
            if (pages == null) {
                pages = new VecBatchToPageIterator(omniOperator.getOutput());
            }
            else {
                if (pages.hasNext()) {
                    return pages.next();
                }
                else {
                    finished = true;
                    return null;
                }
            }
        }
        return null;
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        return null;
    }

    @Override
    public void finishMemoryRevoke()
    {
    }

    public Step getStep()
    {
        return step;
    }

    /**
     * The type Hash aggregation omni operator factory.
     *
     * @since 20210630
     */
    public static class HashAggregationOmniOperatorFactory
            extends AbstractOmniOperatorFactory
    {
        private static final int INVALID_MASK_CHANNEL = -1;
        private final OmniHashAggregationOperatorFactory omniFactory;

        private final Step step;

        private int[] groupByInputChannels;

        private DataType[] groupByInputTypes;

        private int[] aggregationInputChannels;

        private DataType[] aggregationInputTypes;

        private FunctionType[] aggregatorTypes;

        private List<Optional<Integer>> maskChannels;

        private DataType[] aggregationOutputTypes;

        /**
         * Instantiates a new Hash aggregation omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         * @param groupByInputChannels the group by input channels
         * @param groupByInputTypes the group by input types
         * @param aggregationInputChannels the aggregation input channels
         * @param aggregationInputTypes the aggregation input types
         * @param aggregatorTypes the aggregator types
         * @param maskChannelList mask channel list for aggregators
         * @param aggregationOutputTypes the aggregation output types
         * @param step the step
         */
        public HashAggregationOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Type> sourceTypes,
                                                  int[] groupByInputChannels, DataType[] groupByInputTypes, int[] aggregationInputChannels,
                                                  DataType[] aggregationInputTypes, FunctionType[] aggregatorTypes,
                                                  List<Optional<Integer>> maskChannelList, DataType[] aggregationOutputTypes, AggregationNode.Step step)
        {
            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.step = step;
            this.groupByInputChannels = Arrays.copyOf(
                    requireNonNull(groupByInputChannels, "groupByInputChannels is null."), groupByInputChannels.length);
            this.groupByInputTypes = Arrays.copyOf(
                    requireNonNull(groupByInputTypes, "groupByInputTypes is null."),
                    groupByInputTypes.length);
            this.aggregationInputChannels = Arrays.copyOf(
                    requireNonNull(aggregationInputChannels, "aggregationInputChannels is null."),
                    aggregationInputChannels.length);
            this.aggregationInputTypes = Arrays.copyOf(
                    requireNonNull(aggregationInputTypes, "aggregationInputTypes is null."),
                    aggregationInputTypes.length);
            this.aggregatorTypes = Arrays.copyOf(
                    requireNonNull(aggregatorTypes, "aggregatorTypes is null."),
                    aggregatorTypes.length);
            this.maskChannels = requireNonNull(maskChannelList, "mask channels is null");
            int[] maskChannelArray = new int[maskChannelList.size()];
            for (int i = 0; i < maskChannelList.size(); i++) {
                Optional<Integer> channel = maskChannelList.get(i);
                if (channel.isPresent()) {
                    maskChannelArray[i] = channel.get().intValue();
                }
                else {
                    maskChannelArray[i] = INVALID_MASK_CHANNEL;
                }
            }
            this.aggregationOutputTypes = Arrays.copyOf(
                    requireNonNull(aggregationOutputTypes, "aggregationOutputTypes is null."),
                    aggregationOutputTypes.length);

            this.omniFactory = new OmniHashAggregationOperatorFactory(createExpressions(this.groupByInputChannels),
                    this.groupByInputTypes, createExpressions(this.aggregationInputChannels),
                    this.aggregationInputTypes, this.aggregatorTypes, maskChannelArray, this.aggregationOutputTypes, step.isInputRaw(),
                    step.isOutputPartial());
        }

        /**
         * Instantiates a new Hash aggregation omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         * @param groupByInputChannels the group by input channels
         * @param groupByInputTypes the group by input types
         * @param aggregationInputChannels the aggregation input channels
         * @param aggregationInputTypes the aggregation input types
         * @param aggregatorTypes the aggregator types
         * @param inAndOutputTypes the in and output types
         * @param maskChannels mask channel list for aggregators
         */
        @VisibleForTesting
        public HashAggregationOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, int[] groupByInputChannels,
                                                  DataType[] groupByInputTypes, int[] aggregationInputChannels, DataType[] aggregationInputTypes,
                                                  FunctionType[] aggregatorTypes, List<DataType[]> inAndOutputTypes, int[] maskChannels)
        {
            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            int groupByLength = groupByInputChannels.length;
            int aggLength = aggregationInputChannels.length;
            int[] cppGroupByChannels = new int[groupByLength];
            int[] cppAggChannels = new int[aggLength];

            for (int i = 0; i < groupByLength; i++) {
                cppGroupByChannels[i] = i;
            }
            for (int i = 0; i < aggLength; i++) {
                cppAggChannels[i] = groupByLength + i;
            }
            this.step = Step.SINGLE;
            OmniHashAggregationOperatorFactory omniOperatorFactory = new OmniHashAggregationOperatorFactory(
                    createExpressions(cppGroupByChannels), groupByInputTypes, createExpressions(cppAggChannels),
                    aggregationInputTypes, aggregatorTypes, maskChannels, inAndOutputTypes.get(1), this.step.isInputRaw(),
                    this.step.isOutputPartial());
            this.omniFactory = omniOperatorFactory;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            VecAllocator vecAllocator = VecAllocatorHelper.createOperatorLevelAllocator(driverContext,
                    VecAllocator.UNLIMIT, HashAggregationOmniOperator.class);
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    HashAggregationOmniOperator.class.getSimpleName());
            OmniOperator omniOperator = omniFactory.createOperator(vecAllocator);
            return new HashAggregationOmniOperator(operatorContext, omniOperator, step);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public void noMoreOperators(Lifespan lifespan)
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new HashAggregationOmniOperatorFactory(operatorId, planNodeId, sourceTypes, groupByInputChannels,
                    groupByInputTypes, aggregationInputChannels, aggregationInputTypes, aggregatorTypes, maskChannels,
                    aggregationOutputTypes, step);
        }
    }
}
