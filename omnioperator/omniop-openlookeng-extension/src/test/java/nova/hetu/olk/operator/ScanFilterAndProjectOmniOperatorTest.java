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
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.WorkProcessorSourceOperatorAdapter;
import io.prestosql.operator.project.CursorProcessor;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.dynamicfilter.DynamicFilterSupplier;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.split.PageSourceProvider;
import nova.hetu.olk.operator.ScanFilterAndProjectOmniOperator.ScanFilterAndProjectOmniOperatorFactory;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ScanFilterAndProjectOmniOperatorTest
        extends AbstractOperatorTest
{
    private final int operatorId = 0;
    private final PlanNodeId planNodeId = new PlanNodeId(UUID.randomUUID().toString());
    private final PlanNodeId sourceId = new PlanNodeId(UUID.randomUUID().toString());
    private final PageSourceProvider pageSourceProvider = (session, split, table, columns, dynamicFilter) -> null;
    private final Supplier<CursorProcessor> cursorProcessor = () -> mock(CursorProcessor.class);
    private final Supplier<PageProcessor> pageProcessor = () -> mock(PageProcessor.class);
    private final TableHandle table = createTableHandle();
    private final Iterable<ColumnHandle> columns = new ArrayList<>();
    private final Optional<DynamicFilterSupplier> dynamicFilter = Optional.empty();
    private final List<Type> types = new ArrayList<>();
    private final DataSize minOutputPageSize = new DataSize(1, DataSize.Unit.BYTE);
    private final int minOutputPageRowCount = 0;
    private final ReuseExchangeOperator.STRATEGY strategy = ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT;
    private final UUID reuseTableScanMappingId = null;
    private final boolean spillEnabled = false;
    private final Optional<SpillerFactory> spillerFactory = Optional.empty();
    private final Integer spillerThreshold = 0;
    private final Integer consumerTableScanNodeCount = 0;
    private final List<Type> inputTypes = new ArrayList<>();
    private ScanFilterAndProjectOmniOperatorFactory operatorFactoryCache;

    protected OperatorFactory createOperatorFactory()
    {
        operatorFactoryCache = new ScanFilterAndProjectOmniOperatorFactory(operatorId, planNodeId, sourceId, pageSourceProvider,
                cursorProcessor, pageProcessor, table, columns, dynamicFilter, types, minOutputPageSize,
                minOutputPageRowCount, strategy, reuseTableScanMappingId, spillEnabled, spillerFactory,
                spillerThreshold, consumerTableScanNodeCount, inputTypes);
        return operatorFactoryCache;
    }

    private TableHandle createTableHandle()
    {
        return new TableHandle(new CatalogName(UUID.randomUUID().toString()), new ConnectorTableHandle()
        {
        }, new ConnectorTransactionHandle()
        {
        }, Optional.empty());
    }

    @Override
    protected Operator createOperator(Operator originalOperator)
    {
        return new WorkProcessorSourceOperatorAdapter(originalOperator.getOperatorContext(), operatorFactoryCache, strategy, reuseTableScanMappingId,
                spillEnabled, types, spillerFactory, spillerThreshold, consumerTableScanNodeCount);
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
        Assert.assertThrows(operator.getClass().getName() + " can not take input", UnsupportedOperationException.class, () -> {
            operator.addInput(page);
        });

        Page output = operator.getOutput();
        assertTrue(page != null || output == null);
    }
}
