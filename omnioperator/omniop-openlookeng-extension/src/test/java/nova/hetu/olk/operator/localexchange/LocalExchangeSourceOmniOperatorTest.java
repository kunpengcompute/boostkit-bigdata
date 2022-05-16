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

package nova.hetu.olk.operator.localexchange;

import io.airlift.units.DataSize;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.PipelineExecutionStrategy;
import io.prestosql.operator.exchange.LocalExchange.LocalExchangeFactory;
import io.prestosql.operator.exchange.LocalExchange.LocalExchangeSinkFactoryId;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import nova.hetu.olk.operator.AbstractOperatorTest;
import nova.hetu.olk.operator.localexchange.LocalExchangeSourceOmniOperator.LocalExchangeSourceOmniOperatorFactory;
import nova.hetu.olk.operator.localexchange.OmniLocalExchange.OmniLocalExchangeFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static nova.hetu.olk.mock.MockUtil.mockPage;
import static org.junit.Assert.assertEquals;

public class LocalExchangeSourceOmniOperatorTest
        extends AbstractOperatorTest
{
    private final PlanNodeId planNodeId = new PlanNodeId(UUID.randomUUID().toString());
    private final LocalExchangeFactory localExchangeFactory = createLocalExchangeFactory();
    private final int operatorId = 0;
    private final LocalExchangeSinkFactoryId sinkFactoryId = new LocalExchangeSinkFactoryId(1);
    private final Function<Page, Page> pagePreprocessor = page -> page;
    private final List<Type> types = new ArrayList<>();

    @Override
    protected Page[] pageForTest()
    {
        return new Page[]{mockPage()};
    }

    @Override
    protected OperatorFactory createOperatorFactory()
    {
        LocalExchangeSourceOmniOperatorFactory localExchangeSinkOmniOperatorFactory = new LocalExchangeSourceOmniOperatorFactory(operatorId, planNodeId, localExchangeFactory, 0, types);
        return localExchangeSinkOmniOperatorFactory;
    }

    @Override
    protected void checkOperatorFactory(OperatorFactory operatorFactory)
    {
        assertEquals(operatorFactory.getSourceTypes(), types);
        assertEquals(operatorFactory.isExtensionOperatorFactory(), isExtensionFactory());
    }

    @Override
    protected boolean isExtensionFactory()
    {
        return true;
    }

    private LocalExchangeFactory createLocalExchangeFactory()
    {
        OmniLocalExchangeFactory omniLocalExchangeFactory = new OmniLocalExchangeFactory(FIXED_HASH_DISTRIBUTION, 2, Arrays.asList(VarcharType.createVarcharType(10), VarcharType.createVarcharType(10), VarcharType.createVarcharType(10)), Arrays.asList(0, 1, 2),
                Optional.empty(), PipelineExecutionStrategy.UNGROUPED_EXECUTION, new DataSize(1, DataSize.Unit.BYTE));
        omniLocalExchangeFactory.newSinkFactoryId();
        omniLocalExchangeFactory.newSinkFactoryId();
        omniLocalExchangeFactory.noMoreSinkFactories();
        return omniLocalExchangeFactory;
    }

    @Test(dataProvider = "pageProvider")
    public void testProcess(int i)
    {
    }
}
