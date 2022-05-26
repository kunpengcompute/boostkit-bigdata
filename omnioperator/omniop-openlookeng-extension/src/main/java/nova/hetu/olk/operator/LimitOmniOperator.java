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
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.limit.OmniLimitOperatorFactory;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.buildVecBatch;

/**
 * The type limit omni operator.
 *
 * @since 20210630
 */
public class LimitOmniOperator
        implements Operator
{
    private long remainingLimit;

    private boolean finishing;

    private boolean finished;

    private final OperatorContext operatorContext;

    private final OmniOperator omniOperator;

    private Iterator<Page> pages; // The Pages

    /**
     * Instantiates a new Top n omni operator.
     *
     * @param operatorContext the operator context
     * @param omniOperator the omni operator
     * @param limit the limit record count
     */
    public LimitOmniOperator(OperatorContext operatorContext, OmniOperator omniOperator, long limit)
    {
        checkArgument(limit >= 0, "limit must be at least zero");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.omniOperator = omniOperator;
        this.remainingLimit = limit;
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

        return ((remainingLimit > 0) && (pages == null));
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finished, "Operator is already finishing");
        requireNonNull(page, "page is null");

        int rowCount = page.getPositionCount();
        if (rowCount == 0) {
            return;
        }

        remainingLimit = (remainingLimit >= rowCount) ? (remainingLimit - rowCount) : 0;

        VecBatch vecBatch = buildVecBatch(omniOperator.getVecAllocator(), page, getClass().getSimpleName());
        omniOperator.addInput(vecBatch);
        pages = new VecBatchToPageIterator(omniOperator.getOutput());
    }

    @Override
    public Page getOutput()
    {
        if ((finishing) || (remainingLimit == 0)) {
            finished = true;
        }

        if (pages == null) {
            return null;
        }

        Page page = null;
        if (pages.hasNext()) {
            page = pages.next();
        }
        pages = null;
        return page;
    }

    /**
     * The type limit omni operator factory.
     *
     * @since 20210630
     */
    public static class LimitOmniOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;

        private final PlanNodeId planNodeId;

        private final long limit;

        private final OmniLimitOperatorFactory omniLimitOperatorFactory;

        private List<Type> sourceTypes;

        /**
         * Instantiates a new Top n omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         * @param limit the limit record count
         */
        public LimitOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, long limit, List<Type> sourceTypes)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.limit = limit;
            this.sourceTypes = sourceTypes;
            omniLimitOperatorFactory = getOmniLimitOperatorFactory(limit);
        }

        private OmniLimitOperatorFactory getOmniLimitOperatorFactory(long limit)
        {
            return new OmniLimitOperatorFactory(limit);
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            VecAllocator vecAllocator = VecAllocatorHelper.createOperatorLevelAllocator(driverContext,
                    VecAllocator.UNLIMIT, LimitOmniOperator.class);
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    LimitOmniOperator.class.getSimpleName());
            OmniOperator omniOperator = omniLimitOperatorFactory.createOperator(vecAllocator);
            return new LimitOmniOperator(operatorContext, omniOperator, limit);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new LimitOmniOperatorFactory(operatorId, planNodeId, limit, sourceTypes);
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
}
