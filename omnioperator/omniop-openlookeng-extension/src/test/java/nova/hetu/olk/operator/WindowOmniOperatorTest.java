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
import io.prestosql.operator.WindowFunctionDefinition;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.operator.WindowOmniOperator.WindowOmniOperatorFactory;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.operator.window.OmniWindowOperatorFactory;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static io.prestosql.spi.block.SortOrder.values;
import static java.util.stream.Collectors.toList;
import static nova.hetu.olk.mock.MockUtil.mockNewWithWithAnyArguments;
import static nova.hetu.olk.mock.MockUtil.mockOmniOperator;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

@PrepareForTest({
        OperatorUtils.class, WindowOmniOperator.class
})
public class WindowOmniOperatorTest
        extends AbstractOperatorTest
{
    private final PlanNodeId planNodeId = new PlanNodeId(UUID.randomUUID().toString());
    private final List<? extends Type> sourceTypes = new ArrayList<>();
    private final List<Integer> outputChannels = new ArrayList<>();
    private final List<WindowFunctionDefinition> windowFunctionDefinitions = new ArrayList<>();
    private final List<Integer> partitionChannels = new ArrayList<>();
    private final List<Integer> preGroupedChannels = new ArrayList<>();
    private final List<Integer> sortChannels = Arrays.stream(values()).map(SortOrder::ordinal).collect(toList());
    private final List<SortOrder> sortOrder = Arrays.asList(values());
    private final int preSortedChannelPrefix = 1;
    private final int expectedPositions = 0;

    @Override
    protected void setUpMock()
    {
        super.setUpMock();
        mockNewWithWithAnyArguments(OmniWindowOperatorFactory.class);
    }

    @Override
    protected OperatorFactory createOperatorFactory()
    {
        return new WindowOmniOperatorFactory(0, planNodeId, sourceTypes, outputChannels,
                windowFunctionDefinitions, partitionChannels, preGroupedChannels, sortChannels, sortOrder,
                preSortedChannelPrefix, expectedPositions).duplicate();
    }

    @Override
    protected Operator createOperator(Operator originalOperator)
    {
        return new WindowOmniOperator(originalOperator.getOperatorContext(), mockOmniOperator());
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
        do {
            operator.getOutput();
        } while (!operator.isFinished());
        assertNull(operator.getOutput());
        assertFalse(operator.needsInput());
    }
}
