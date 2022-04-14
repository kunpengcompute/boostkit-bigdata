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
import io.prestosql.plugin.hive.HiveMetadata;
import io.prestosql.plugin.hive.HiveMetadataFactory;
import io.prestosql.plugin.hive.HivePartitionManager;
import io.prestosql.plugin.hive.HiveTransactionManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.plan.FilterStatsCalculatorService;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.sql.relational.ConnectorRowExpressionService;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;
import io.prestosql.sql.relational.RowExpressionDomainTranslator;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.OFFLOAD_METADATA;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.simulationHiveMetadata;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.simulationHiveTransactionManager;
import static org.testng.Assert.assertEquals;

public class TestHivePlanOptimizerProvider
        extends TestHivePushdown
{
    @Test
    public void testProvider()
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

        HiveMetadataFactory hiveMetadataFactory = Mockito.mock(HiveMetadataFactory.class);
        HiveMetadata hiveMetadata = simulationHiveMetadata();
        Mockito.when(hiveMetadataFactory.get()).thenReturn(hiveMetadata);

        HivePlanOptimizerProvider hivePlanOptimizerProvider = new HivePlanOptimizerProvider(transactionManager,
                expressionService, resolution, partitionManager, OFFLOAD_METADATA.getFunctionAndTypeManager(),
                calculatorService, hiveMetadataFactory);
        assertEquals(hivePlanOptimizerProvider.getLogicalPlanOptimizers().size(), 3);
        assertEquals(hivePlanOptimizerProvider.getPhysicalPlanOptimizers().size(), 3);
    }
}
