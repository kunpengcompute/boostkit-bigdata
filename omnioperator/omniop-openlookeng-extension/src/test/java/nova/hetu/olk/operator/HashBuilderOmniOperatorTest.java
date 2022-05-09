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

import io.prestosql.operator.JoinBridgeManager;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.PartitionedLookupSourceFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.operator.HashBuilderOmniOperator.HashBuilderOmniOperatorFactory;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.operator.join.OmniHashBuilderOperatorFactory;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.UUID;

import static io.prestosql.operator.Operator.NOT_BLOCKED;
import static nova.hetu.olk.mock.MockUtil.mockNewWithWithAnyArguments;
import static nova.hetu.olk.mock.MockUtil.mockOmniOperator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@PrepareForTest({
        HashBuilderOmniOperator.class,
        HashBuilderOmniOperatorFactory.class,
        OperatorUtils.class
})
public class HashBuilderOmniOperatorTest
        extends AbstractOperatorTest
{
    private final int operatorId = new Random().nextInt();
    private final PlanNodeId planNodeId = new PlanNodeId(UUID.randomUUID().toString());

    private final ArrayList<Integer> hashChannels = new ArrayList<>();
    private final OptionalInt preComputedHashChannel = OptionalInt.empty();
    private final Optional<String> filterFunction = Optional.empty();
    private final Optional<Integer> sortChannel = Optional.empty();
    private final List<String> searchFunctions = new ArrayList<>();
    private final int operatorCount = 0;
    private PartitionedLookupSourceFactory lookupSourceFactory;
    private ArrayList<Type> buildTypes = new ArrayList<>();
    private ArrayList<Integer> outputChannels = new ArrayList<>();
    private JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager;

    @Override
    protected void setUpMock()
    {
        super.setUpMock();
        lookupSourceFactoryManager = mock(JoinBridgeManager.class);
        mockNewWithWithAnyArguments(OmniHashBuilderOperatorFactory.class);
    }

    @Override
    protected OperatorFactory createOperatorFactory()
    {
        PartitionedLookupSourceFactory lookupSourceFactory = new PartitionedLookupSourceFactory(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), 2, new HashMap<>(), false, false);
        when(lookupSourceFactoryManager.getJoinBridge(any())).thenReturn(lookupSourceFactory);
        return new HashBuilderOmniOperatorFactory(operatorId, planNodeId,
                lookupSourceFactoryManager, buildTypes, outputChannels, hashChannels, preComputedHashChannel,
                filterFunction, sortChannel, searchFunctions, operatorCount);
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
        assertEquals(operatorFactory.getSourceTypes(), buildTypes);
        assertEquals(((HashBuilderOmniOperatorFactory) operatorFactory).getOutputChannels(), outputChannels);
    }

    @Override
    protected Operator createOperator(Operator originalOperator)
    {
        lookupSourceFactory = this.lookupSourceFactoryManager.getJoinBridge(originalOperator.getOperatorContext()
                .getDriverContext().getLifespan());
        return new HashBuilderOmniOperator(originalOperator.getOperatorContext(), lookupSourceFactory, 1, mockOmniOperator());
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
        int operationCount = 0;
        assertEquals(operator.isBlocked(), NOT_BLOCKED);
        do {
            operator.finish();
            if (operationCount++ > 10) {
                lookupSourceFactory.destroy();
            }
            else {
                assertNotEquals(operator.isBlocked(), NOT_BLOCKED);
            }
        } while (!operator.isFinished());
        assertEquals(operator.isBlocked(), NOT_BLOCKED);
        operator.getOutput();
        assertFalse(operator.needsInput());
    }
}
