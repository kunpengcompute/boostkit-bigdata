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

import io.airlift.units.DataSize;
import io.prestosql.execution.buffer.OutputBuffer;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.PartitionFunction;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.operator.PartitionedOutputOmniOperator.PartitionedOutputOmniFactory;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.operator.OmniOperator;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;

import static nova.hetu.olk.mock.MockUtil.block;
import static nova.hetu.olk.mock.MockUtil.fill;
import static nova.hetu.olk.mock.MockUtil.mockOmniOperator;
import static nova.hetu.olk.mock.MockUtil.mockPage;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

@PrepareForTest({
        OperatorUtils.class, PartitionedOutputOmniOperator.class
})
public class PartitionedOutputOmniOperatorTest
        extends AbstractOperatorTest
{
    private final int operatorId = 0;
    private final PlanNodeId planNodeId = new PlanNodeId(UUID.randomUUID().toString());
    private final ArrayList<Type> sourceTypes = new ArrayList<>();
    private final Function<Page, Page> pagePageFunction = page -> page;
    private final PartitionFunction partitionFunction = new PartitionFunction()
    {
        @Override
        public int getPartitionCount()
        {
            return 10;
        }

        @Override
        public int getPartition(Page page, int position)
        {
            return 0;
        }
    };
    private final List<Integer> partitionChannels = new ArrayList<>();
    private final List<Optional<NullableValue>> partitionConstants = new ArrayList<>();
    private final boolean replicatesAnyRow = false;
    private final OptionalInt channel = OptionalInt.empty();
    @Mock
    private OutputBuffer outputBuffer;
    private final DataSize maxMemory = new DataSize(1, DataSize.Unit.BYTE);

    @Override
    protected Page[] pageForTest()
    {
        return new Page[]{
                mockPage(),
                mockPage(block(false, false, fill(new Integer[3], integer -> new Random().nextInt())))
        };
    }

    protected OperatorFactory createOperatorFactory()
    {
        PartitionedOutputOmniFactory outputOperator = spy(new PartitionedOutputOmniFactory(partitionFunction, partitionChannels,
                partitionConstants, replicatesAnyRow, channel, outputBuffer, maxMemory, new int[]{},
                false, new ArrayList<>()));
        OperatorFactory operatorFactory = mock(OperatorFactory.class);
        OperatorContext operatorContext = mock(OperatorContext.class);
        doAnswer((invocation) -> operatorFactory).when(outputOperator).createOutputOperator(anyInt(), any(), any(), any(), any());
        doAnswer(invocation -> createOperator()).when(operatorFactory).createOperator(any());
        doAnswer((invocation) -> operatorFactory).when(operatorFactory).duplicate();
        return outputOperator.createOutputOperator(operatorId, planNodeId, new ArrayList<>(), pagePageFunction, null);
    }

    @Override
    protected Class<? extends Throwable> throwWhenDuplicate()
    {
        return null;
    }

    private Operator createOperator()
    {
        OmniOperator omniOperator = mockOmniOperator();

        return new PartitionedOutputOmniOperator(
                UUID.randomUUID().toString(), operatorContext, sourceTypes, pagePageFunction,
                partitionFunction, partitionChannels, partitionConstants, replicatesAnyRow, channel,
                outputBuffer, maxMemory, omniOperator);
    }

    @Override
    protected boolean isExtensionFactory()
    {
        return false;
    }

    @Test(dataProvider = "pageProvider")
    public void testProcess(int i)
    {
        Operator operator = getOperator();
        Page page = getPageForTest(i);

        if (page == null) {
            assertThrows(NullPointerException.class, () -> operator.addInput(page));
        }
        else {
            operator.addInput(page);
        }
        Page output = operator.getOutput();
        assertTrue(page != null || output == null);
    }
}
