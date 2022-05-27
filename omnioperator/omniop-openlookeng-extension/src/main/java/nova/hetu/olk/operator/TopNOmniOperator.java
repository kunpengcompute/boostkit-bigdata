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
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.topn.OmniTopNOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.buildVecBatch;
import static nova.hetu.olk.tool.OperatorUtils.createExpressions;

/**
 * The type Top n omni operator.
 *
 * @since 20210630
 */
public class TopNOmniOperator
        implements Operator
{
    private final OperatorContext operatorContext;

    private final OmniOperator omniOperator;

    private final int topN;

    /**
     * The Pages.
     */
    Iterator<Page> pages;

    private boolean finishing;

    private boolean finished;

    /**
     * Instantiates a new Top n omni operator.
     *
     * @param operatorContext the operator context
     * @param omniOperator the omni operator
     * @param topN the n
     */
    public TopNOmniOperator(OperatorContext operatorContext, OmniOperator omniOperator, int topN)
    {
        this.operatorContext = operatorContext;
        this.omniOperator = omniOperator;
        this.topN = topN;
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
        if (topN == 0) {
            return false;
        }
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

    /**
     * The type Top n omni operator factory.
     *
     * @since 20210630
     */
    public static class TopNOmniOperatorFactory
            extends AbstractOmniOperatorFactory
    {
        private final int topN;

        private final ImmutableList<Integer> sortChannels;

        private final ImmutableList<SortOrder> sortOrders;

        private final OmniTopNOperatorFactory omniTopNOperatorFactory;

        /**
         * Instantiates a new Top n omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         * @param sourceTypes the source types
         * @param topN the n
         * @param sortChannels the sort channels
         * @param sortOrders the sort orders
         */
        public TopNOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, List<? extends Type> sourceTypes,
                                       int topN, List<Integer> sortChannels, List<SortOrder> sortOrders)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.topN = topN;
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));

            omniTopNOperatorFactory = getOmniTopNOperatorFactory(sourceTypes, topN, sortChannels, sortOrders);
        }

        private OmniTopNOperatorFactory getOmniTopNOperatorFactory(List<? extends Type> sourceTypes, int topN,
                                                                   List<Integer> sortChannels, List<SortOrder> sortOrders)
        {
            DataType[] omniSourceTypes = OperatorUtils.toDataTypes(sourceTypes);
            int[] omniSortChannels = sortChannels.stream().mapToInt(Integer::valueOf).toArray();
            int[] omniSortOrder = new int[sortOrders.size()];
            int[] omniSortNullFirst = new int[sortOrders.size()];
            for (int i = 0; i < sortOrders.size(); i++) {
                if (sortOrders.get(i).isAscending()) {
                    omniSortOrder[i] = 1;
                }
                else {
                    omniSortOrder[i] = 0;
                }
                if (sortOrders.get(i).isNullsFirst()) {
                    omniSortNullFirst[i] = 1;
                }
                else {
                    omniSortNullFirst[i] = 0;
                }
            }

            return new OmniTopNOperatorFactory(omniSourceTypes, topN, createExpressions(omniSortChannels),
                    omniSortOrder, omniSortNullFirst);
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            VecAllocator vecAllocator = VecAllocatorHelper.createOperatorLevelAllocator(driverContext,
                    VecAllocator.UNLIMIT, TopNOmniOperator.class);
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    TopNOmniOperator.class.getSimpleName());
            OmniOperator omniOperator = omniTopNOperatorFactory.createOperator(vecAllocator);
            return new TopNOmniOperator(operatorContext, omniOperator, topN);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TopNOmniOperatorFactory(operatorId, planNodeId, sourceTypes, topN, sortChannels, sortOrders);
        }
    }
}
