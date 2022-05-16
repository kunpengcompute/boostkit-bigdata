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
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.sort.OmniSortOperatorFactory;
import nova.hetu.omniruntime.vector.VecAllocator;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static nova.hetu.olk.mock.MockUtil.mockOmniOperator;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@PrepareForTest(OperatorUtils.class)
public class OrderByOperatorTest
        extends AbstractOperatorTest
{
    private final int operatorId = 0;
    private final PlanNodeId planNodeId = new PlanNodeId(UUID.randomUUID().toString());
    private final List<Type> sourceTypes = new ArrayList<>();
    private final int[] outputChannels = new int[]{};
    private final int[] sortChannels = new int[]{};
    private final int[] sortAscendings = new int[]{};
    private final int[] sortNullFirsts = new int[]{};
    private OmniSortOperatorFactory omniSortOperatorFactory;

    @Override
    protected void setUpMock()
    {
        super.setUpMock();
        omniSortOperatorFactory = createOmniSortOperatorFactory();
    }

    @Override
    protected OperatorFactory createOperatorFactory()
    {
        return new OrderByOmniOperator.OrderByOmniOperatorFactory(operatorId, planNodeId, sourceTypes, outputChannels,
                sortChannels, sortAscendings, sortNullFirsts, omniSortOperatorFactory).duplicate();
    }

    private OmniSortOperatorFactory createOmniSortOperatorFactory()
    {
        OmniSortOperatorFactory omniSortOperatorFactory = mock(OmniSortOperatorFactory.class);
        OmniOperator omniOperator = mockOmniOperator();
        when(omniSortOperatorFactory.createOperator(any(VecAllocator.class))).thenReturn(omniOperator);
        return omniSortOperatorFactory;
    }

    @Override
    protected Operator createOperator(Operator originalOperator)
    {
        return new OrderByOmniOperator(originalOperator.getOperatorContext(), mockOmniOperator());
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
        }

        assertFalse(operator.isFinished());
        operator.finish();
        operator.getOutput();

        //assertFalse(operator.needsInput());
    }
}
