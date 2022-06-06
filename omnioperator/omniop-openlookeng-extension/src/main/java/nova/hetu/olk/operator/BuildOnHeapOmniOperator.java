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
import nova.hetu.olk.tool.BlockUtils;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.transferToOnHeapPage;

/**
 * The type BuildOnHeap Omni Operator.
 *
 * @since 20220110
 */
public class BuildOnHeapOmniOperator
        implements Operator
{
    private final OperatorContext operatorContext;

    private boolean finishing;

    private Page inputPage;

    /**
     * Instantiates a new BuildOnHeap Omni Operator.
     *
     * @param operatorContext the operator context
     */
    public BuildOnHeapOmniOperator(OperatorContext operatorContext)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && inputPage == null;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && inputPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        inputPage = page;
    }

    @Override
    public Page getOutput()
    {
        if (inputPage == null) {
            return null;
        }

        Page outputPage = processPage();
        freeNativeMemory(inputPage);
        inputPage = null;
        return outputPage;
    }

    private void freeNativeMemory(Page inputPage)
    {
        BlockUtils.freePage(inputPage);
    }

    private Page processPage()
    {
        return transferToOnHeapPage(inputPage);
    }

    @Override
    public void close() throws Exception
    {
        if (inputPage != null) {
            BlockUtils.freePage(inputPage);
        }
    }

    /**
     * The type buildOnHeapOmniOperator factory.
     *
     * @since 20220110
     */
    public static class BuildOnHeapOmniOperatorFactory
            extends AbstractOmniOperatorFactory
    {
        /**
         * Instantiates a new buildOnHeapOmniOperator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         */
        public BuildOnHeapOmniOperatorFactory(int operatorId, PlanNodeId planNodeId)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    BuildOnHeapOmniOperator.class.getSimpleName());
            return new BuildOnHeapOmniOperator(operatorContext);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new BuildOnHeapOmniOperatorFactory(operatorId, planNodeId);
        }
    }
}
