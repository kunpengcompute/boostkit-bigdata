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

import com.google.common.util.concurrent.AbstractFuture;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.exchange.LocalExchange;
import io.prestosql.operator.exchange.LocalExchange.LocalExchangeFactory;
import io.prestosql.operator.exchange.LocalExchangeSource;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.OrderingCompiler;
import nova.hetu.olk.block.LazyOmniBlock;
import nova.hetu.olk.operator.LocalMergeSourceOmniOperator.LocalMergeSourceOmniOperatorFactory;
import nova.hetu.olk.operator.localexchange.OmniLocalExchange.OmniLocalExchangeFactory;
import nova.hetu.olk.tool.OperatorUtils;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.prestosql.operator.Operator.NOT_BLOCKED;
import static java.util.Arrays.asList;
import static nova.hetu.olk.mock.MockUtil.mockNewWithWithAnyArguments;
import static nova.hetu.olk.mock.MockUtil.mockOmniOperator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

@PrepareForTest({
        LocalMergeSourceOmniOperator.class,
        OrderByOmniOperator.class,
        OperatorUtils.class,
        LazyOmniBlock.class
})
public class LocalMergeSourceOmniOperatorTest
        extends AbstractOperatorTest
{
    private final int operatorId = new Random().nextInt();
    private final int orderByOmniId = new Random().nextInt();
    private final PlanNodeId planNodeId = new PlanNodeId(UUID.randomUUID().toString());

    private final List<Type> types = new ArrayList<>();
    private final OrderingCompiler orderingCompiler = null;
    private final List<Integer> sortChannels = new ArrayList<>();
    private final List<SortOrder> orderings = new ArrayList<>();
    private final List<Integer> outputChannels = new ArrayList<>();

    private final AtomicBoolean firstSourceFinish = new AtomicBoolean(false);
    private final AtomicBoolean secondSourceFinish = new AtomicBoolean(false);
    private LocalExchangeFactory localExchangeFactory;
    private LocalExchange localExchange;
    private LocalExchangeSource firstLocalExchangeSource;
    private LocalExchangeSource secondLocalExchangeSource;
    private List<LocalExchangeSource> sources;

    @Override
    protected void setUpMock()
    {
        super.setUpMock();
        OrderByOmniOperator orderByOmniOperator = mock(OrderByOmniOperator.class);
        OrderByOmniOperator.OrderByOmniOperatorFactory orderByOmniOperatorFactory = mockNewWithWithAnyArguments(OrderByOmniOperator.OrderByOmniOperatorFactory.class);
        mockStatic(OrderByOmniOperator.OrderByOmniOperatorFactory.class);
        when(OrderByOmniOperator.OrderByOmniOperatorFactory.createOrderByOmniOperatorFactory(
                anyInt(), any(PlanNodeId.class), anyList(), anyList(), anyList(), anyList()
        )).thenReturn(orderByOmniOperatorFactory);
        when(orderByOmniOperatorFactory.createOperator(any(DriverContext.class))).thenReturn(orderByOmniOperator);
        localExchangeFactory = mock(OmniLocalExchangeFactory.class);
        localExchange = mock(LocalExchange.class);
        firstLocalExchangeSource = mock(LocalExchangeSource.class);
        secondLocalExchangeSource = mock(LocalExchangeSource.class);
        sources = new ArrayList<>(asList(firstLocalExchangeSource, secondLocalExchangeSource));
    }

    @Override
    protected OperatorFactory createOperatorFactory()
    {
        when(localExchangeFactory.getLocalExchange(any())).thenReturn(localExchange);
        when(localExchange.getBufferCount()).thenReturn(2);
        when(localExchange.getNextSource()).thenReturn(firstLocalExchangeSource, secondLocalExchangeSource);
        when(firstLocalExchangeSource.getPages()).thenAnswer((invocation -> {
            return firstSourceFinish.get() ? asList(new Page(new LazyBlock(10, block -> {}))) : null;
        }));
        when(secondLocalExchangeSource.getPages()).thenAnswer((invocation -> {
            return secondSourceFinish.get() ? asList(new Page(new LazyBlock(10, block -> {}))) : null;
        }));
        when(firstLocalExchangeSource.waitForReading()).thenReturn(new AbstractFuture()
        {
            @Override
            public boolean isDone()
            {
                return false;
            }
        });
        when(firstLocalExchangeSource.waitForReading()).thenReturn(new AbstractFuture()
        {
            @Override
            public boolean isDone()
            {
                return false;
            }
        });
        return new LocalMergeSourceOmniOperatorFactory(operatorId, orderByOmniId, planNodeId, localExchangeFactory,
                types, orderingCompiler, sortChannels, orderings, outputChannels);
    }

    @Override
    protected Class<? extends Throwable> throwWhenDuplicate()
    {
        return UnsupportedOperationException.class;
    }

    @Override
    protected void checkOperatorFactory(OperatorFactory operatorFactory)
    {
        super.checkOperatorFactory(operatorFactory);
        assertEquals(operatorFactory.getSourceTypes(), types);
    }

    @Override
    protected Operator createOperator(Operator originalOperator)
    {
        OrderByOmniOperator orderByOmniOperator = spy(new OrderByOmniOperator(originalOperator.getOperatorContext(),
                mockOmniOperator()));
        when(orderByOmniOperator.getOutput()).thenReturn(mock(Page.class));
        when(orderByOmniOperator.isFinished()).then(invocation -> firstSourceFinish.get() && secondSourceFinish.get());
        return new LocalMergeSourceOmniOperator(originalOperator.getOperatorContext(), sources, orderByOmniOperator);
    }

    @Test(dataProvider = "pageProvider")
    public void testProcess(int i)
    {
        Operator operator = getOperator();
        Page page = getPageForTest(i);
        firstSourceFinish.set(false);
        secondSourceFinish.set(false);
        assertThrows(UnsupportedOperationException.class, () -> operator.addInput(page));
        assertFalse(operator.needsInput());
        assertFalse(operator.isFinished());
        operator.finish();
        do {
            operator.getOutput();
            firstSourceFinish.set(true);
            secondSourceFinish.set(true);
        } while (!operator.isFinished());
        operator.getOutput();

        while (sources.size() > 0) {
            assertEquals(operator.isBlocked(), sources.get(0).waitForReading());
            sources.remove(sources.size() - 1);
        }

        assertEquals(operator.isBlocked(), NOT_BLOCKED);
    }
}
