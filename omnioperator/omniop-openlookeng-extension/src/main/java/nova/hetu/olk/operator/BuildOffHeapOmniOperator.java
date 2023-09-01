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
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;

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

    private boolean finishing;

    private Page inputPage;

    private final List<Type> inputTypes;

    /**
     * Instantiates a new BuildOffHeap Omni Operator.
     *
     * @param operatorContext the operator context
     */
    public BuildOffHeapOmniOperator(OperatorContext operatorContext, List<Type> inputTypes)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
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
        return transferToOffHeapPages(inputPage, inputTypes);
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
            checkDataTypes(this.sourceTypes);
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    BuildOffHeapOmniOperator.class.getSimpleName());
            return new BuildOffHeapOmniOperator(operatorContext, sourceTypes);
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

        @Override
        public void checkDataType(Type type)
        {
            TypeSignature signature = type.getTypeSignature();
            String base = signature.getBase();

            switch (base) {
                case StandardTypes.INTEGER:
                case StandardTypes.SMALLINT:
                case StandardTypes.BIGINT:
                case StandardTypes.DOUBLE:
                case StandardTypes.BOOLEAN:
                case StandardTypes.VARBINARY:
                case StandardTypes.VARCHAR:
                case StandardTypes.CHAR:
                case StandardTypes.DECIMAL:
                case StandardTypes.DATE:
                case StandardTypes.ROW:
                    return;
                default:
                    throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support data Type " + base);
            }
        }
    }
}
