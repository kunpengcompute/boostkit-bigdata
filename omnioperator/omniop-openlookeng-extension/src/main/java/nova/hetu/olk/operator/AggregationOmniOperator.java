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
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.aggregator.OmniAggregationOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.buildVecBatch;

/**
 * The type Aggregation omni operator.
 *
 * @since 20210630
 */
public class AggregationOmniOperator
        implements Operator
{
    private final OperatorContext operatorContext;

    private final OmniOperator omniOperator;

    private State state = State.NEEDS_INPUT;

    /**
     * Instantiates a new Aggregation omni operator.
     *
     * @param operatorContext the operator context
     * @param omniOperator the omni operator
     */
    public AggregationOmniOperator(OperatorContext operatorContext, OmniOperator omniOperator)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.omniOperator = requireNonNull(omniOperator, "omniOperator is null");
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
        checkState(needsInput(), "Operator is already finishing");
        requireNonNull(page, "page is null");

        VecBatch vecBatch = buildVecBatch(omniOperator.getVecAllocator(), page, this);
        omniOperator.addInput(vecBatch);
    }

    @Override
    public Page getOutput()
    {
        if (state != State.HAS_OUTPUT) {
            return null;
        }

        VecBatchToPageIterator pageIterator = new VecBatchToPageIterator(omniOperator.getOutput());
        if (pageIterator.hasNext()) {
            state = State.FINISHED;
            return pageIterator.next();
        }
        return null;
    }

    @Override
    public void finish()
    {
        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;
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

    /**
     * The type Aggregation omni operator factory.
     *
     * @since 20210630
     */
    public static class AggregationOmniOperatorFactory
            extends AbstractOmniOperatorFactory
    {
        private static final int INVALID_MASK_CHANNEL = -1;
        private final DataType[] sourceDataTypes;
        private final AggregationNode.Step step;
        private final FunctionType[] aggregatorTypes;
        private final int[] aggregationInputChannels;
        private final List<Optional<Integer>> maskChannels;
        private final DataType[] aggregationOutputTypes;
        private final OmniAggregationOperatorFactory omniFactory;

        /**
         * Instantiates a new Aggregation omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         * @param sourceTypes the source types
         * @param aggregatorTypes the aggregations
         * @param aggregationInputChannels the accumulator factories
         * @param maskChannelList mask channel list for aggregators
         * @param aggregationOutputTypes aggregation output types
         * @param step the step
         */
        public AggregationOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Type> sourceTypes,
                                              FunctionType[] aggregatorTypes, int[] aggregationInputChannels, List<Optional<Integer>> maskChannelList,
                                              DataType[] aggregationOutputTypes, AggregationNode.Step step)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = sourceTypes;
            checkDataTypes(this.sourceTypes);
            this.sourceDataTypes = requireNonNull(OperatorUtils.toDataTypes(sourceTypes), "sourceTypes is null");
            this.step = step;
            this.aggregatorTypes = aggregatorTypes;
            this.aggregationInputChannels = aggregationInputChannels;
            this.aggregationOutputTypes = aggregationOutputTypes;
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
            this.omniFactory = new OmniAggregationOperatorFactory(sourceDataTypes, aggregatorTypes,
                    aggregationInputChannels, maskChannelArray, aggregationOutputTypes, step.isInputRaw(),
                    step.isOutputPartial());
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            VecAllocator vecAllocator = VecAllocatorHelper.createOperatorLevelAllocator(driverContext,
                    VecAllocator.UNLIMIT, AggregationOmniOperator.class);
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    AggregationOmniOperator.class.getSimpleName());
            OmniOperator omniOperator = omniFactory.createOperator(vecAllocator);
            return new AggregationOmniOperator(operatorContext, omniOperator);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new AggregationOmniOperatorFactory(operatorId, planNodeId, sourceTypes, aggregatorTypes,
                    aggregationInputChannels, maskChannels, aggregationOutputTypes, step);
        }

        @Override
        public void checkDataType(Type type)
        {
            TypeSignature signature = type.getTypeSignature();
            String base = signature.getBase();

            switch (base) {
                case StandardTypes.INTEGER:
                case StandardTypes.BIGINT:
                case StandardTypes.DOUBLE:
                case StandardTypes.BOOLEAN:
                case StandardTypes.VARCHAR:
                case StandardTypes.CHAR:
                case StandardTypes.DECIMAL:
                case StandardTypes.DATE:
                    return;
                case StandardTypes.VARBINARY:
                case StandardTypes.ROW: {
                    if (this.step == AggregationNode.Step.FINAL) {
                        return;
                    }
                    else if (this.step == AggregationNode.Step.PARTIAL) {
                        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support data Type " + base);
                    }
                }
                default:
                    throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support data Type " + base);
            }
        }
    }
}
