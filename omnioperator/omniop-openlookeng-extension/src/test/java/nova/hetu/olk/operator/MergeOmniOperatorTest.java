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

import io.hetu.core.transport.execution.buffer.PagesSerdeFactory;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.ExchangeClientSupplier;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.OrderingCompiler;
import nova.hetu.olk.block.LazyOmniBlock;
import nova.hetu.olk.operator.MergeOmniOperator.MergeOmniOperatorFactory;
import nova.hetu.olk.tool.OperatorUtils;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static nova.hetu.olk.mock.MockUtil.mockNewVecWithAnyArguments;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@PrepareForTest({
        LocalMergeSourceOmniOperator.class,
        OrderByOmniOperator.class,
        OperatorUtils.class,
        LazyOmniBlock.class
})
public class MergeOmniOperatorTest
        extends AbstractOperatorTest
{
    private final int operatorId = 0;
    private final int omniMergeId = 0;
    private final PlanNodeId sourceId = new PlanNodeId(UUID.randomUUID().toString());
    private final ExchangeClientSupplier exchangeClientSupplier = systemMemoryContext -> null;
    private final PagesSerdeFactory serdeFactory = new PagesSerdeFactory(new BlockEncodingSerde()
    {
    }, false);
    private final OrderingCompiler orderingCompiler = new OrderingCompiler();
    private final List<Type> types = new ArrayList<>();
    private final List<Integer> outputChannels = new ArrayList<>();
    private final List<Integer> sortChannels = new ArrayList<>();
    private final List<SortOrder> sortOrder = new ArrayList<>();

    @Override
    protected void setUpMock()
    {
        super.setUpMock();
        OrderByOmniOperator orderByOmniOperator = mock(OrderByOmniOperator.class);
        OrderByOmniOperator.OrderByOmniOperatorFactory orderByOmniOperatorFactory = mockNewVecWithAnyArguments(OrderByOmniOperator.OrderByOmniOperatorFactory.class);
        mockStatic(OrderByOmniOperator.OrderByOmniOperatorFactory.class);
        when(OrderByOmniOperator.OrderByOmniOperatorFactory.createOrderByOmniOperatorFactory(
                anyInt(), any(PlanNodeId.class), anyList(), anyList(), anyList(), anyList()
        )).thenReturn(orderByOmniOperatorFactory);
        when(orderByOmniOperatorFactory.createOperator(any(DriverContext.class))).thenReturn(orderByOmniOperator);
    }

    @Override
    protected OperatorFactory createOperatorFactory()
    {
        return new MergeOmniOperatorFactory(operatorId, omniMergeId, sourceId, exchangeClientSupplier, serdeFactory,
                orderingCompiler, types, outputChannels, sortChannels, sortOrder);
    }

    @Override
    protected void checkOperatorFactory(OperatorFactory operatorFactory)
    {
        super.checkOperatorFactory(operatorFactory);
        assertEquals(operatorFactory.getSourceTypes(), types);
        assertEquals(((MergeOmniOperatorFactory) operatorFactory).getSourceId(), sourceId);
    }

    @Override
    protected Operator createOperator(Operator originalOperator)
    {
        OrderByOmniOperator orderByOmniOperator = mock(OrderByOmniOperator.class);
        doAnswer(invocationOnMock -> {
            throw new IOException("");
        }).when(orderByOmniOperator).close();
        return new MergeOmniOperator(originalOperator.getOperatorContext(), sourceId, exchangeClientSupplier,
                serdeFactory.createPagesSerde(), orderByOmniOperator);
    }

    @Override
    protected Class<? extends Throwable> throwWhenDuplicate()
    {
        return UnsupportedOperationException.class;
    }

    @Test(dataProvider = "pageProvider")
    public void testProcess(int i)
    {
        Operator operator = getOperator();
        Page page = getPageForTest(i);
        assertThrows(UnsupportedOperationException.class, () -> operator.addInput(page));
        assertFalse(operator.needsInput());
        Page output = operator.getOutput();
        assertTrue(page != null || output == null);
    }
}
