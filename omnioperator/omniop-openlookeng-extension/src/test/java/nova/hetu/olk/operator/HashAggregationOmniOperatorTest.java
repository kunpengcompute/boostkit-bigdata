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

import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.operator.HashAggregationOmniOperator.HashAggregationOmniOperatorFactory;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.aggregator.OmniHashAggregationOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static nova.hetu.olk.mock.MockUtil.mockNewVecWithAnyArguments;
import static nova.hetu.olk.mock.MockUtil.mockOmniOperator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@PrepareForTest({
        HashAggregationOmniOperator.class,
        OperatorUtils.class
})
public class HashAggregationOmniOperatorTest
        extends AbstractOperatorTest
{
    private final int operatorId = new Random().nextInt();
    private final PlanNodeId planNodeId = new PlanNodeId(UUID.randomUUID().toString());
    private final int[] groupByInputChannels = {0};
    private final DataType[] groupByInputTypes = {};
    private final int[] aggregationInputChannels = {};
    private final DataType[] aggregationInputTypes = {};
    private final FunctionType[] aggregatorTypes = {};
    private final AggregationNode.Step step = AggregationNode.Step.SINGLE;
    private final DataType[] aggregationOutputTypes = {};
    private final List<Type> sourceTypes = Arrays.asList(BooleanType.BOOLEAN);
    private final List<DataType[]> inAndOutputTypes = Arrays.asList(new DataType[]{}, new DataType[]{});
    private OmniHashAggregationOperatorFactory omniHashAggregationOperatorFactory;
    private OmniOperator omniOperator;

    @Override
    protected void setUpMock()
    {
        super.setUpMock();
        omniHashAggregationOperatorFactory = mockNewVecWithAnyArguments(OmniHashAggregationOperatorFactory.class);
        omniOperator = mockOmniOperator();
        when(omniHashAggregationOperatorFactory.createOperator(any())).thenReturn(omniOperator);
    }

    @Override
    protected OperatorFactory createOperatorFactory()
    {
        HashAggregationOmniOperatorFactory hashAggregationOmniOperatorFactory = spy(new HashAggregationOmniOperatorFactory(operatorId, planNodeId,
                groupByInputChannels, groupByInputTypes, aggregationInputChannels, aggregationInputTypes,
                aggregatorTypes, inAndOutputTypes, new int[0]));
        doAnswer(invocation -> new HashAggregationOmniOperatorFactory(operatorId, planNodeId, sourceTypes,
                groupByInputChannels, groupByInputTypes, aggregationInputChannels, aggregationInputTypes,
                aggregatorTypes, new ArrayList<>(), aggregationOutputTypes, step)).when(hashAggregationOmniOperatorFactory).duplicate();
        return hashAggregationOmniOperatorFactory.duplicate();
    }

    @Override
    protected void checkOperatorFactory(OperatorFactory operatorFactory)
    {
        super.checkOperatorFactory(operatorFactory);
        assertEquals(operatorFactory.duplicate().getSourceTypes(), sourceTypes);
    }

    @Override
    protected Operator createOperator(Operator originalOperator)
    {
        return new HashAggregationOmniOperator(originalOperator.getOperatorContext(), mockOmniOperator(), step);
    }

    @Override
    protected void checkOperator(Operator operator)
    {
        super.checkOperator(operator);
        assertNull(operator.startMemoryRevoke());
        assertEquals(((HashAggregationOmniOperator) operator).getStep(), step);
    }

    @Test(dataProvider = "pageProvider")
    public void testProcess(int i)
    {
        Operator operator = getOperator();
        Page page = getPageForTest(i);
        if (page == null) {
            assertThrows("page is null", NullPointerException.class, () -> operator.addInput(page));
        }
        else {
            operator.addInput(page);
            assertNull(operator.getOutput());
        }

        assertFalse(operator.isFinished());
        operator.finish();
        do {
            operator.getOutput();
        } while (!operator.isFinished());
        assertNull(operator.getOutput());
        assertFalse(operator.needsInput());
        operator.finishMemoryRevoke();
    }
}
