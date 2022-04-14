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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.huawei.boostkit.omnidata.model.AggregationInfo;
import io.prestosql.plugin.hive.HiveMetadata;
import io.prestosql.plugin.hive.HiveMetadataFactory;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.sql.relational.FunctionResolution;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.COLUMN_INT;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.COLUMN_TYPE_MAP;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.ID_ALLOCATOR;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.OFFLOAD_METADATA;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.OFFLOAD_SESSION;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.SYMBOL_ALLOCATOR;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.buildAggregationNode;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.buildAssignments;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.buildProjectNode;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.buildTableScanNode;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.createAggregation;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.createOperationExpression;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.matchAggregatorOffload;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.matchProjection;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.simulationHiveMetadata;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.simulationHiveTransactionManager;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;

public class TestHivePartialAggregationPushdown
        extends TestHivePushdown
{
    private static final HivePartialAggregationPushdown AGGREGATION_OPTIMIZER = createOptimizer();

    private static HivePartialAggregationPushdown createOptimizer()
    {
        HiveMetadataFactory hiveMetadataFactory = Mockito.mock(HiveMetadataFactory.class);
        HiveMetadata hiveMetadata = simulationHiveMetadata();
        Mockito.when(hiveMetadataFactory.get()).thenReturn(hiveMetadata);

        StandardFunctionResolution resolution = new FunctionResolution(OFFLOAD_METADATA.getFunctionAndTypeManager());
        HivePartialAggregationPushdown optimizer = new HivePartialAggregationPushdown(simulationHiveTransactionManager(),
                OFFLOAD_METADATA.getFunctionAndTypeManager(), resolution, hiveMetadataFactory);
        return optimizer;
    }

    AggregationNode buildCountAggregationNode(PlanNode source)
    {
        // select count(x) from table group by x
        VariableReferenceExpression expression = new VariableReferenceExpression(COLUMN_INT.getName(), COLUMN_TYPE_MAP.get(COLUMN_INT.getName()));
        AggregationNode.Aggregation aggregation = createAggregation("count", BIGINT, ImmutableList.of(expression));
        Map<Symbol, AggregationNode.Aggregation> aggregations = ImmutableMap.of(new Symbol(COLUMN_INT.getName()), aggregation);
        AggregationNode.GroupingSetDescriptor groupingSets =
                new AggregationNode.GroupingSetDescriptor(ImmutableList.of(new Symbol(COLUMN_INT.getName())), 1, Collections.emptySet());
        return buildAggregationNode(source, aggregations, groupingSets);
    }

    @Test
    public void testPartialAggregationPushdown()
    {
        // select count(x) from table group by x
        TableScanNode tableScanNode = buildTableScanNode(COLUMN_INT);
        AggregationNode aggregationNode = buildCountAggregationNode(tableScanNode);

        ImmutableMap.Builder<String, AggregationInfo.AggregateFunction> aggregationsExpected = new ImmutableMap.Builder<>();
        for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : aggregationNode.getAggregations().entrySet()) {
            AggregationInfo.AggregateFunction aggregateFunction =
                    new AggregationInfo.AggregateFunction(entry.getValue().getFunctionCall(), entry.getValue().isDistinct());
            aggregationsExpected.put(entry.getKey().getName(), aggregateFunction);
        }
        List<RowExpression> groupingKeysExpected =
                ImmutableList.of(new VariableReferenceExpression(COLUMN_INT.getName(), INTEGER));
        AggregationInfo aggregationInfoExpected = new AggregationInfo(aggregationsExpected.build(), groupingKeysExpected);

        PlanNode outputNode = AGGREGATION_OPTIMIZER.optimize(aggregationNode, OFFLOAD_SESSION, COLUMN_TYPE_MAP, SYMBOL_ALLOCATOR, ID_ALLOCATOR);
        matchAggregatorOffload(outputNode, aggregationInfoExpected);
    }

    @Test
    public void testPartialAggregationAndProjectPushdown()
    {
        // select count(x + 5) from table group by x
        TableScanNode tableScanNode = buildTableScanNode(COLUMN_INT);

        CallExpression callExpression = createOperationExpression(OperatorType.ADD,
                new VariableReferenceExpression(COLUMN_INT.getName(), INTEGER), new ConstantExpression(5, INTEGER));
        List<Symbol> symbols = ImmutableList.of(new Symbol(COLUMN_INT.getName()));
        List<RowExpression> rowExpressions = ImmutableList.of(callExpression);
        ProjectNode projectNode = buildProjectNode(tableScanNode, symbols, rowExpressions);

        AggregationNode aggregationNode = buildCountAggregationNode(projectNode);

        PlanNode output = AGGREGATION_OPTIMIZER.optimize(aggregationNode, OFFLOAD_SESSION, COLUMN_TYPE_MAP, SYMBOL_ALLOCATOR, ID_ALLOCATOR);
        Assignments assignmentsExpected = buildAssignments(symbols, rowExpressions);
        matchProjection(output, assignmentsExpected.getMap());
    }
}
