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
import io.prestosql.operator.EnforceSingleRowOperator;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.omniruntime.vector.VecAllocator;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * The enforce single row omni operator.
 */
public class EnforceSingleRowOmniOperator
        extends EnforceSingleRowOperator
{
    private VecAllocator vecAllocator;

    public EnforceSingleRowOmniOperator(OperatorContext operatorContext, VecAllocator vecAllocator)
    {
        super(operatorContext);
        this.vecAllocator = vecAllocator;
    }

    @Override
    public void addInput(Page page)
    {
        super.addInput(page);
    }

    @Override
    public Page getOutput()
    {
        // Here we need build the page off-heap in case it is SINGLE_NULL_VALUE_PAGE.
        Page output = super.getOutput();
        if (output == null) {
            return output;
        }
        return OperatorUtils.transferToOffHeapPages(vecAllocator, output);
    }

    /**
     * The enforce single row omni operator factory.
     */
    public static class EnforceSingleRowOmniOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private boolean closed;
        private final List<Type> sourceTypes;

        /**
         * Instantiates a new Enforce single row omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         */
        public EnforceSingleRowOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Type> sourceTypes)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = sourceTypes;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    EnforceSingleRowOmniOperator.class.getSimpleName());
            VecAllocator vecAllocator = VecAllocatorHelper.createOperatorLevelAllocator(driverContext,
                    VecAllocator.UNLIMIT, EnforceSingleRowOmniOperator.class);
            return new EnforceSingleRowOmniOperator(operatorContext, vecAllocator);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new EnforceSingleRowOmniOperatorFactory(operatorId, planNodeId, sourceTypes);
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
