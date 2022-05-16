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
import io.prestosql.operator.JoinBridgeManager;
import io.prestosql.operator.LookupJoinOperators;
import io.prestosql.operator.LookupSourceFactory;
import io.prestosql.operator.LookupSourceProvider;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.PipelineExecutionStrategy;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.operator.HashBuilderOmniOperator.HashBuilderOmniOperatorFactory;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.join.OmniLookupJoinOperatorFactory;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static nova.hetu.olk.mock.MockUtil.mockNewWithWithAnyArguments;
import static nova.hetu.olk.mock.MockUtil.mockOmniOperator;
import static nova.hetu.olk.operator.LookupJoinOmniOperators.JoinType;
import static nova.hetu.olk.operator.LookupJoinOmniOperators.JoinType.FULL_OUTER;
import static nova.hetu.olk.operator.LookupJoinOmniOperators.JoinType.INNER;
import static nova.hetu.olk.operator.LookupJoinOmniOperators.JoinType.LOOKUP_OUTER;
import static nova.hetu.olk.operator.LookupJoinOmniOperators.JoinType.PROBE_OUTER;
import static nova.hetu.olk.operator.LookupJoinOmniOperators.fullOuterJoin;
import static nova.hetu.olk.operator.LookupJoinOmniOperators.innerJoin;
import static nova.hetu.olk.operator.LookupJoinOmniOperators.lookupOuterJoin;
import static nova.hetu.olk.operator.LookupJoinOmniOperators.probeOuterJoin;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@PrepareForTest({
        LookupJoinOmniOperator.class,
        OrderByOmniOperator.class,
        OperatorUtils.class
})
@Test
public class LookupJoinOmniOperatorTest
        extends AbstractOperatorTest
{
    private final int operatorId = 0;
    private final PlanNodeId planNodeId = new PlanNodeId(UUID.randomUUID().toString());

    private final List<Type> probeTypes = new ArrayList<>();
    private final OptionalInt totalOperatorsCount = OptionalInt.empty();
    private final List<Integer> probeJoinChannels = new ArrayList<>();
    private final List<Integer> probeOutputChannels = new ArrayList<>();
    private final OptionalInt empty = OptionalInt.empty();
    @Mock
    private HashBuilderOmniOperatorFactory hashBuilderOmniOperatorFactory;
    @Mock
    private LookupSourceProvider lookupSourceProvider;
    @Mock
    private JoinBridgeManager<LookupSourceFactory> lookupSourceFactoryManager;
    @Mock
    private LookupSourceFactory lookupSourceFactory;
    @Mock
    private AbstractFuture<LookupSourceProvider> lookupSourceProviderFuture;
    private final AtomicBoolean providerFinished = new AtomicBoolean(false);
    protected JoinType joinType;

    @Override
    protected void setUpMock()
    {
        super.setUpMock();
        mockNewWithWithAnyArguments(OmniLookupJoinOperatorFactory.class);
        when((lookupSourceFactoryManager).getJoinBridge(any())).thenReturn(lookupSourceFactory);
        when(lookupSourceFactoryManager.getBuildExecutionStrategy()).thenReturn(PipelineExecutionStrategy.GROUPED_EXECUTION);
        when(lookupSourceProvider.withLease(any())).thenReturn(0L);
        when(lookupSourceFactory.createLookupSourceProvider()).thenReturn(new AbstractFuture<LookupSourceProvider>()
        {
            @Override
            public LookupSourceProvider get()
            {
                return lookupSourceProvider;
            }

            @Override
            public boolean isDone()
            {
                return providerFinished.get();
            }

            @Override
            public LookupSourceProvider get(long timeout, TimeUnit unit)
            {
                return get();
            }
        });
    }

    @Override
    protected OperatorFactory createOperatorFactory()
    {
        OperatorFactory operatorFactory = null;
        switch (joinType) {
            case INNER:
                operatorFactory = innerJoin(operatorId, planNodeId, lookupSourceFactoryManager, probeTypes,
                        probeJoinChannels, empty, Optional.of(probeOutputChannels), totalOperatorsCount,
                        hashBuilderOmniOperatorFactory);
                break;
            case PROBE_OUTER:
                operatorFactory = probeOuterJoin(operatorId, planNodeId, lookupSourceFactoryManager, probeTypes,
                        probeJoinChannels, empty, Optional.of(probeOutputChannels), totalOperatorsCount,
                        hashBuilderOmniOperatorFactory);
                break;
            case LOOKUP_OUTER:
                operatorFactory = lookupOuterJoin(operatorId, planNodeId, lookupSourceFactoryManager, probeTypes,
                        probeJoinChannels, empty, Optional.of(probeOutputChannels), totalOperatorsCount,
                        hashBuilderOmniOperatorFactory);
                break;
            case FULL_OUTER:
                operatorFactory = fullOuterJoin(operatorId, planNodeId, lookupSourceFactoryManager, probeTypes,
                        probeJoinChannels, empty, Optional.of(probeOutputChannels), totalOperatorsCount,
                        hashBuilderOmniOperatorFactory);
                break;
        }

        return operatorFactory;
    }

    @Override
    protected void checkOperatorFactory(OperatorFactory operatorFactory)
    {
        super.checkOperatorFactory(operatorFactory);
        assertEquals(operatorFactory.getSourceTypes(), probeTypes);
    }

    @Override
    protected Operator createOperator(Operator originalOperator)
    {
        OmniOperator omniOperator = mockOmniOperator();
        //when(omniOperator.getOutput()).thenReturn(mockOmiOutput);
        return new LookupJoinOmniOperator(originalOperator.getOperatorContext(), probeTypes,
                LookupJoinOperators.JoinType.INNER, lookupSourceFactory, () -> {}, omniOperator);
    }

    @Override
    protected void checkOperator(Operator operator)
    {
        super.checkOperator(operator);
    }

    protected void testProcess(int i)
    {
        Operator operator = getOperator();
        Page page = getPageForTest(i);
        if (page == null) {
            assertThrows("page is null", NullPointerException.class, () -> operator.addInput(page));
        }
        else {
            providerFinished.set(false);
            assertThrows("Not ready to handle input yet", IllegalStateException.class, () -> operator.addInput(page));
            providerFinished.set(true);
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

    public static class InnerJoinTest
            extends LookupJoinOmniOperatorTest
    {
        {
            joinType = INNER;
        }

        @Override
        @Test(dataProvider = "pageProvider")
        protected void testProcess(int i)
        {
            super.testProcess(i);
        }
    }

    public static class ProbeOuterTest
            extends LookupJoinOmniOperatorTest
    {
        {
            joinType = PROBE_OUTER;
        }

        @Override
        @Test(dataProvider = "pageProvider")
        protected void testProcess(int i)
        {
            super.testProcess(i);
        }
    }

    public static class LookupOuterJoinTest
            extends LookupJoinOmniOperatorTest
    {
        {
            joinType = LOOKUP_OUTER;
        }

        @Override
        @Test(dataProvider = "pageProvider")
        protected void testProcess(int i)
        {
            super.testProcess(i);
        }
    }

    public static class FullOuterRJoinTest
            extends LookupJoinOmniOperatorTest
    {
        {
            joinType = FULL_OUTER;
        }

        @Override
        @Test(dataProvider = "pageProvider")
        protected void testProcess(int i)
        {
            super.testProcess(i);
        }
    }
}
