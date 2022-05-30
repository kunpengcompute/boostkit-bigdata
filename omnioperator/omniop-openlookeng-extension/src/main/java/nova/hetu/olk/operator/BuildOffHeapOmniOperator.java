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
import nova.hetu.omniruntime.vector.VecAllocator;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.transferToOffHeapPages;

/**
 * The type BuildOffHeap Omni Operator.
 *
 * @since 20220110
 */
public class BuildOffHeapOmniOperator
        implements Operator
{
    private final OperatorContext operatorContext;

    private final VecAllocator vecAllocator;

    private boolean finishing;

    private Page inputPage;

    private final List<Type> inputTypes;

    /**
     * Instantiates a new BuildOffHeap Omni Operator.
     *
     * @param operatorContext the operator context
     * @param vecAllocator the vecAllocator
     */
    public BuildOffHeapOmniOperator(OperatorContext operatorContext, VecAllocator vecAllocator, List<Type> inputTypes)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.vecAllocator = vecAllocator;
        this.inputTypes = inputTypes;
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
        inputPage = null;
        return outputPage;
    }

    private Page processPage()
    {
        return transferToOffHeapPages(vecAllocator, inputPage, inputTypes);
    }

    /**
     * The type buildOffHeapOmniOperator factory.
     *
     * @since 20220110
     */
    public static class BuildOffHeapOmniOperatorFactory
            extends AbstractOmniOperatorFactory
    {
        /**
         * Instantiates a new buildOffHeapOmniOperator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         */
        public BuildOffHeapOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Type> sourceTypes)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = sourceTypes;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            VecAllocator vecAllocator = VecAllocatorHelper.createOperatorLevelAllocator(driverContext,
                    VecAllocator.UNLIMIT, VecAllocatorHelper.DEFAULT_RESERVATION, BuildOffHeapOmniOperator.class);
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    BuildOffHeapOmniOperator.class.getSimpleName());
            return new BuildOffHeapOmniOperator(operatorContext, vecAllocator, sourceTypes);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new BuildOffHeapOmniOperatorFactory(operatorId, planNodeId, sourceTypes);
        }
    }
}
