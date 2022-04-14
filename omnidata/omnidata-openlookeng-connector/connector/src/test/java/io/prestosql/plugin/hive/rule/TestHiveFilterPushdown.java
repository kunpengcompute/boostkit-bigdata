/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.plugin.hive.rule;

import io.prestosql.cost.ConnectorFilterStatsCalculatorService;
import io.prestosql.cost.FilterStatsCalculator;
import io.prestosql.cost.ScalarStatsCalculator;
import io.prestosql.cost.StatsNormalizer;
import io.prestosql.plugin.hive.HivePartitionManager;
import io.prestosql.plugin.hive.HiveTransactionManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.FilterStatsCalculatorService;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.sql.TestingRowExpressionTranslator;
import io.prestosql.sql.relational.ConnectorRowExpressionService;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;
import io.prestosql.sql.relational.RowExpressionDomainTranslator;
import org.testng.annotations.Test;

import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.COLUMN_INT;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.COLUMN_TYPE_MAP;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.ID_ALLOCATOR;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.OFFLOAD_METADATA;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.OFFLOAD_SESSION;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.SYMBOL_ALLOCATOR;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.buildFilterNode;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.buildTableScanNode;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.matchFilterOffload;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.simulationHiveTransactionManager;

public class TestHiveFilterPushdown
        extends TestHivePushdown
{
    private static final HiveFilterPushdown FILTER_OPTIMIZER = createOptimizer();
    private static final TestingRowExpressionTranslator TRANSLATOR = new TestingRowExpressionTranslator(OFFLOAD_METADATA);

    private static HiveFilterPushdown createOptimizer()
    {
        RowExpressionService expressionService = new ConnectorRowExpressionService(new RowExpressionDomainTranslator(OFFLOAD_METADATA), new RowExpressionDeterminismEvaluator(OFFLOAD_METADATA));
        HiveTransactionManager transactionManager = simulationHiveTransactionManager();
        StandardFunctionResolution resolution = new FunctionResolution(OFFLOAD_METADATA.getFunctionAndTypeManager());
        HivePartitionManager partitionManager =
                new HivePartitionManager(OFFLOAD_METADATA.getFunctionAndTypeManager(), 1, false, 1);
        ScalarStatsCalculator scalarStatsCalculator = new ScalarStatsCalculator(OFFLOAD_METADATA);
        StatsNormalizer normalizer = new StatsNormalizer();
        FilterStatsCalculator statsCalculator = new FilterStatsCalculator(OFFLOAD_METADATA, scalarStatsCalculator, normalizer);
        FilterStatsCalculatorService calculatorService = new ConnectorFilterStatsCalculatorService(statsCalculator);
        HiveFilterPushdown optimizer = new HiveFilterPushdown(transactionManager, expressionService,
                resolution, partitionManager, calculatorService, OFFLOAD_METADATA.getFunctionAndTypeManager());
        return optimizer;
    }

    @Test
    public void testFilterPushdown()
    {
        TableScanNode tableScanNode = buildTableScanNode(COLUMN_INT);
        String predicate = String.format("%s < 1", COLUMN_INT.getColumnName());
        RowExpression expression = TRANSLATOR.translate(predicate, SYMBOL_ALLOCATOR.getSymbols());
        FilterNode filterNode = buildFilterNode(tableScanNode, expression);

        PlanNode node = FILTER_OPTIMIZER.optimize(filterNode, OFFLOAD_SESSION, COLUMN_TYPE_MAP, SYMBOL_ALLOCATOR, ID_ALLOCATOR);
        matchFilterOffload(node, expression);
    }
}
