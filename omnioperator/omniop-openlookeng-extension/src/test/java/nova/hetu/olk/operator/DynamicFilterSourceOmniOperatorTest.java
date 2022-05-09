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
import io.prestosql.operator.DynamicFilterSourceOperator;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.operator.DynamicFilterSourceOmniOperator.DynamicFilterSourceOmniOperatorFactory;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DynamicFilterSourceOmniOperatorTest
        extends AbstractOperatorTest
{
    private final int operatorId = new Random().nextInt();
    private final PlanNodeId planNodeId = new PlanNodeId(UUID.randomUUID().toString());
    private final Consumer<Map<DynamicFilterSourceOperator.Channel, Set>> mapConsumer = channelSetMap -> {};
    private final List<DynamicFilterSourceOperator.Channel> channels = Arrays.asList();
    private final int maxFilterPositionsCount = new Random().nextInt();
    private final DataSize maxFilterSize = new DataSize(1, DataSize.Unit.BYTE);
    private final List<Type> sourceTypes = Arrays.asList();

    @Override
    protected OperatorFactory createOperatorFactory()
    {
        return new DynamicFilterSourceOmniOperatorFactory(operatorId, planNodeId, mapConsumer, channels,
                maxFilterPositionsCount, maxFilterSize, sourceTypes);
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
        assertEquals(operatorFactory.getSourceTypes(), sourceTypes);
    }

    @Override
    protected Operator createOperator(Operator originalOperator)
    {
        return new DynamicFilterSourceOmniOperator(originalOperator.getOperatorContext(), mapConsumer, channels,
                planNodeId, maxFilterPositionsCount, maxFilterSize, null);
    }

    @Test(dataProvider = "pageProvider")
    public void testProcess(int i)
    {
        Operator operator = getOperator();
        Page page = getPageForTest(i);
        operator.addInput(page);
        Page output = operator.getOutput();
        assertTrue(page != null || output == null);
    }
}
