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
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.tool.BlockUtils;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.limit.OmniDistinctLimitOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.buildVecBatch;

/**
 * The type distinct limit omni operator.
 *
 * @since 20210630
 */
public class DistinctLimitOmniOperator
        implements Operator
{
    /**
     * The type distinct limit omni operator factory.
     *
     * @since 20210630
     */
    public static class DistinctLimitOmniOperatorFactory
            extends AbstractOmniOperatorFactory
    {
        private final List<Integer> distinctChannels;

        private final Optional<Integer> hashChannel;

        private final long limit;

        private final OmniDistinctLimitOperatorFactory omniDistinctLimitOperatorFactory;

        private boolean closed;

        /**
         * Instantiates a new distinct limit omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         * @param sourceTypes the source types
         * @param distinctChannels the distinct channels
         * @param hashChannel the input hash channel
         * @param limit the limit value
         */
        public DistinctLimitOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Type> sourceTypes,
                                                List<Integer> distinctChannels, Optional<Integer> hashChannel, long limit)
        {
            checkArgument(limit >= 0, "limit must be at least zero");
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            checkDataTypes(this.sourceTypes);
            this.distinctChannels = distinctChannels;
            this.hashChannel = requireNonNull(hashChannel, "hash channel is null.");
            this.limit = limit;
            this.omniDistinctLimitOperatorFactory = getOmniDistinctLimitOperatorFactory(sourceTypes, distinctChannels,
                    hashChannel, limit);
        }

        private OmniDistinctLimitOperatorFactory getOmniDistinctLimitOperatorFactory(List<Type> sourceTypes,
                                                                                     List<Integer> distinctChannelLists, Optional<Integer> hashChannel, long limit)
        {
            DataType[] omniSourceTypes = OperatorUtils.toDataTypes(sourceTypes);
            int[] distinctChannel = distinctChannelLists.stream().mapToInt(Integer::intValue).toArray();
            int hashChannelVal = hashChannel.orElse(-1);

            return new OmniDistinctLimitOperatorFactory(omniSourceTypes, distinctChannel, hashChannelVal, limit);
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            VecAllocator vecAllocator = VecAllocatorHelper.createOperatorLevelAllocator(driverContext,
                    VecAllocator.UNLIMIT, DistinctLimitOmniOperator.class);
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    DistinctLimitOmniOperator.class.getSimpleName());
            OmniOperator omniOperator = omniDistinctLimitOperatorFactory.createOperator(vecAllocator);
            int[] distinctChannelArray = distinctChannels.stream().mapToInt(Integer::intValue).toArray();
            int hashChannelVal = this.hashChannel.orElse(-1);

            return new DistinctLimitOmniOperator(operatorContext, omniOperator, sourceTypes, distinctChannelArray,
                    hashChannelVal, limit);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new DistinctLimitOmniOperator.DistinctLimitOmniOperatorFactory(operatorId, planNodeId, sourceTypes,
                    distinctChannels, hashChannel, limit);
        }
    }

    private final List<Type> sourceTypes;

    private final int[] distinctChannels;

    private final int hashChannel;

    private final long limit;

    private long outputCount;

    private boolean finishing;

    private boolean finished;

    private final OperatorContext operatorContext;

    private final OmniOperator omniOperator;

    private Iterator<Page> pages; // The Pages

    /**
     * Instantiates a new distinct limit omni operator.
     *
     * @param operatorContext the operator context
     * @param omniOperator the omni operator
     * @param sourceTypes the source types
     * @param distinctChannels the distinct columns id
     * @param hashChannel input hash column id
     * @param limit the limit record count
     */
    public DistinctLimitOmniOperator(OperatorContext operatorContext, OmniOperator omniOperator, List<Type> sourceTypes,
                                     int[] distinctChannels, int hashChannel, long limit)
    {
        this.operatorContext = operatorContext;
        this.omniOperator = omniOperator;
        this.sourceTypes = sourceTypes;
        this.distinctChannels = distinctChannels;
        this.hashChannel = hashChannel;
        this.limit = limit;
        this.outputCount = 0;
        this.pages = null;
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
    public void close() throws Exception
    {
        // free page if it has next
        if (pages != null) {
            while (pages.hasNext()) {
                Page next = pages.next();
                BlockUtils.freePage(next);
            }
        }
        omniOperator.close();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        if (finishing) {
            return false;
        }

        return true;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");

        if ((outputCount >= limit) || page.getPositionCount() == 0) {
            BlockUtils.freePage(page);
            return;
        }

        VecBatch vecBatch = buildVecBatch(omniOperator.getVecAllocator(), page, getClass().getSimpleName());
        omniOperator.addInput(vecBatch);
        pages = new VecBatchToPageIterator(omniOperator.getOutput());
    }

    @Override
    public Page getOutput()
    {
        if (finishing) {
            finished = true;
        }
        if (pages == null) {
            return null;
        }

        Page page = null;
        if (pages.hasNext()) {
            page = pages.next();
            outputCount += page.getPositionCount();
        }
        pages = null;
        return page;
    }
}
