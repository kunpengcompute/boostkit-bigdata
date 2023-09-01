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
import io.prestosql.spi.block.Block;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.tool.BlockUtils;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * The type limit omni operator.
 *
 * @since 20210630
 */
public class LimitOmniOperator
        implements Operator
{
    private long remainingLimit;

    private final OperatorContext operatorContext;

    private Page nextPage;

    /**
     * Instantiates a new Top n omni operator.
     *
     * @param operatorContext the operator context
     * @param limit the limit record count
     */
    public LimitOmniOperator(OperatorContext operatorContext, long limit)
    {
        checkArgument(limit >= 0, "limit must be at least zero");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.remainingLimit = limit;
        this.nextPage = null;
    }

    @Override
    public void finish()
    {
        remainingLimit = 0;
    }

    @Override
    public boolean isFinished()
    {
        return remainingLimit == 0 && nextPage == null;
    }

    @Override
    public void close() throws Exception
    {
        // free page if it is not null
        if (nextPage != null) {
            BlockUtils.freePage(nextPage);
        }
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return remainingLimit > 0 && nextPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");

        int rowCount = page.getPositionCount();
        if (rowCount == 0 || !needsInput()) {
            BlockUtils.freePage(page);
            return;
        }

        if (rowCount <= remainingLimit) {
            remainingLimit -= rowCount;
            nextPage = page;
        }
        else {
            Block[] blocks = new Block[page.getChannelCount()];
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                blocks[channel] = block.getRegion(0, (int) remainingLimit);
            }
            nextPage = new Page((int) remainingLimit, blocks);
            remainingLimit = 0;
            BlockUtils.freePage(page);
        }
    }

    @Override
    public Page getOutput()
    {
        Page page = nextPage;
        nextPage = null;
        return page;
    }

    /**
     * The type limit omni operator factory.
     *
     * @since 20210630
     */
    public static class LimitOmniOperatorFactory
            extends AbstractOmniOperatorFactory
    {
        private final long limit;

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
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    LimitOmniOperator.class.getSimpleName());
            return new LimitOmniOperator(operatorContext, limit);
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
    }
}
