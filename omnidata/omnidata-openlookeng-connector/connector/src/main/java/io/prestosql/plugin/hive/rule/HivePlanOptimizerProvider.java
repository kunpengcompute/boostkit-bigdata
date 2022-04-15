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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.prestosql.plugin.hive.HivePartitionManager;
import io.prestosql.plugin.hive.HiveTransactionManager;
import io.prestosql.plugin.hive.TransactionalMetadata;
import io.prestosql.spi.ConnectorPlanOptimizer;
import io.prestosql.spi.connector.ConnectorPlanOptimizerProvider;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.plan.FilterStatsCalculatorService;
import io.prestosql.spi.relation.RowExpressionService;

import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class HivePlanOptimizerProvider
        implements ConnectorPlanOptimizerProvider
{
    private final Set<ConnectorPlanOptimizer> planOptimizers;

    @Inject
    public HivePlanOptimizerProvider(
            HiveTransactionManager transactionManager,
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution,
            HivePartitionManager partitionManager,
            FunctionMetadataManager functionMetadataManager,
            FilterStatsCalculatorService filterCalculatorService,
            Supplier<TransactionalMetadata> metadataFactory)
    {
        requireNonNull(transactionManager, "transactionManager is null");
        requireNonNull(rowExpressionService, "rowExpressionService is null");
        requireNonNull(functionResolution, "functionResolution is null");
        requireNonNull(partitionManager, "partitionManager is null");
        requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        requireNonNull(filterCalculatorService, "filterCalculatorService is null");
        requireNonNull(metadataFactory, "metadataFactory is null");
        this.planOptimizers = ImmutableSet.of(
                new HiveFilterPushdown(transactionManager, rowExpressionService, functionResolution, partitionManager, filterCalculatorService, functionMetadataManager),
                new HivePartialAggregationPushdown(transactionManager, functionMetadataManager, functionResolution, metadataFactory),
                new HiveLimitPushdown(transactionManager));
    }

    @Override
    public Set<ConnectorPlanOptimizer> getLogicalPlanOptimizers()
    {
        return planOptimizers;
    }

    @Override
    public Set<ConnectorPlanOptimizer> getPhysicalPlanOptimizers()
    {
        // New filters may be created in between logical optimization and physical optimization. Push those newly created filters as well.
        return planOptimizers;
    }
}
