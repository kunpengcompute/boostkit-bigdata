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

import io.airlift.units.DataSize;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.DynamicFilterSourceOperator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.omniruntime.vector.VecAllocator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * The Dynamic filter source omni operator. This extends the original
 * DynamicFilterSourceOperator
 */
public class DynamicFilterSourceOmniOperator
        extends DynamicFilterSourceOperator
{
    private VecAllocator vecAllocator;

    /**
     * Constructor for the Dynamic Filter Source Operator TODO: no need to collect
     * dynamic filter if it's cached
     */
    public DynamicFilterSourceOmniOperator(OperatorContext context,
                                           Consumer<Map<Channel, Set>> dynamicPredicateConsumer, List<Channel> channels, PlanNodeId planNodeId,
                                           int maxFilterPositionsCount, DataSize maxFilterSize, VecAllocator vecAllocator)
    {
        super(context, dynamicPredicateConsumer, channels, planNodeId, maxFilterPositionsCount, maxFilterSize);
        this.vecAllocator = vecAllocator;
    }

    @Override
    public void addInput(Page page)
    {
        super.addInput(page);
    }

    public static class DynamicFilterSourceOmniOperatorFactory
            extends AbstractOmniOperatorFactory
    {
        private final Consumer<Map<Channel, Set>> dynamicPredicateConsumer;
        private final List<Channel> channels;
        private final int maxFilterPositionsCount;
        private final DataSize maxFilterSize;
        private boolean closed;

        /**
         * Constructor for the Dynamic Filter Source Omni Operator Factory
         */
        public DynamicFilterSourceOmniOperatorFactory(int operatorId, PlanNodeId planNodeId,
                                                      Consumer<Map<Channel, Set>> dynamicPredicateConsumer, List<Channel> channels,
                                                      int maxFilterPositionsCount, DataSize maxFilterSize, List<Type> sourceTypes)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.dynamicPredicateConsumer = requireNonNull(dynamicPredicateConsumer,
                    "dynamicPredicateConsumer is null");
            this.channels = requireNonNull(channels, "channels is null");
            verify(channels.stream().map(Channel::getFilterId).collect(toSet()).size() == channels.size(),
                    "duplicate dynamic filters are not allowed");
            verify(channels.stream().map(Channel::getIndex).collect(toSet()).size() == channels.size(),
                    "duplicate channel indices are not allowed");
            this.maxFilterPositionsCount = maxFilterPositionsCount;
            this.maxFilterSize = maxFilterSize;
            this.sourceTypes = sourceTypes;
        }

        public DynamicFilterSourceOperator createOperator(DriverContext driverContext)
        {
            VecAllocator vecAllocator = VecAllocatorHelper.createOperatorLevelAllocator(driverContext,
                    VecAllocator.UNLIMIT, DynamicFilterSourceOmniOperator.class);
            return new DynamicFilterSourceOmniOperator(
                    driverContext.addOperatorContext(operatorId, planNodeId,
                            DynamicFilterSourceOmniOperator.class.getSimpleName()),
                    dynamicPredicateConsumer, channels, planNodeId, maxFilterPositionsCount, maxFilterSize,
                    vecAllocator);
        }

        @Override
        public void noMoreOperators()
        {
            checkState(!closed, "Factory is already closed");
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException(
                    "duplicate() is not supported for DynamicFilterSourceOmniOperatorFactory");
        }
    }
}
