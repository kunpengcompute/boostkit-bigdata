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
import io.prestosql.metadata.Metadata;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveMetadata;
import io.prestosql.plugin.hive.HiveOffloadExpression;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.HiveStorageFormat;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.plugin.hive.HiveTransactionManager;
import io.prestosql.plugin.hive.OrcFileWriterConfig;
import io.prestosql.plugin.hive.ParquetFileWriterConfig;
import io.prestosql.plugin.hive.omnidata.OmniDataNodeManager;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.DoubleRange;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.testing.TestingConnectorSession;
import io.prestosql.testing.TestingTransactionHandle;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.metadata.FunctionAndTypeManager.qualifyObjectName;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.plugin.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static io.prestosql.plugin.hive.HiveType.HIVE_BOOLEAN;
import static io.prestosql.plugin.hive.HiveType.HIVE_BYTE;
import static io.prestosql.plugin.hive.HiveType.HIVE_DATE;
import static io.prestosql.plugin.hive.HiveType.HIVE_DOUBLE;
import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.plugin.hive.HiveType.HIVE_LONG;
import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.plugin.hive.HiveType.HIVE_TIMESTAMP;
import static io.prestosql.plugin.hive.omnidata.OmniDataNodeManager.CONFIG_PROPERTY;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.relational.Expressions.call;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHivePushdownUtil
{
    private static final int OFFLOAD_COLUMN_NUM = 1000;
    private static final int DISTINICT_COLUMN_NUM = OFFLOAD_COLUMN_NUM / 10;
    private static final String SIMULATION_FLIE_NAME = "config.simluation";
    protected static final ConnectorSession OFFLOAD_SESSION = getHiveOffloadSession();
    protected static final Metadata OFFLOAD_METADATA = createTestMetadataManager();
    protected static final PlanNodeIdAllocator ID_ALLOCATOR = new PlanNodeIdAllocator();
    protected static final CatalogName CATALOG_NAME = new CatalogName("catalog");
    protected static final PlanBuilder PLAN_BUILDER = new PlanBuilder(ID_ALLOCATOR, OFFLOAD_METADATA);
    protected static final HiveTableHandle OFFLOAD_HIVE_TABLE_HANDLE =
            new HiveTableHandle("db", "test", ImmutableMap.of(), ImmutableList.of(), Optional.empty());
    protected static final TableHandle OFFLOAD_TABLE_HANDLE =
            new TableHandle(CATALOG_NAME, OFFLOAD_HIVE_TABLE_HANDLE, TestingTransactionHandle.create(), Optional.empty());

    protected static final HiveColumnHandle COLUMN_BOOLEAN =
            new HiveColumnHandle("_boolean", HIVE_BOOLEAN, HIVE_BOOLEAN.getTypeSignature(), 0, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
    protected static final HiveColumnHandle COLUMN_INT =
            new HiveColumnHandle("_int", HIVE_INT, HIVE_INT.getTypeSignature(), 1, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
    protected static final HiveColumnHandle COLUMN_LONG =
            new HiveColumnHandle("_long", HIVE_LONG, HIVE_LONG.getTypeSignature(), 2, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
    protected static final HiveColumnHandle COLUMN_DOUBLE =
            new HiveColumnHandle("_double", HIVE_DOUBLE, HIVE_DOUBLE.getTypeSignature(), 3, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
    protected static final HiveColumnHandle COLUMN_CHAR =
            new HiveColumnHandle("_btye", HIVE_BYTE, HIVE_BYTE.getTypeSignature(), 4, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
    protected static final HiveColumnHandle COLUMN_STRING =
            new HiveColumnHandle("_string", HIVE_STRING, HIVE_STRING.getTypeSignature(), 5, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
    protected static final HiveColumnHandle COLUMN_TIMESTAMP =
            new HiveColumnHandle("_timestamp", HIVE_TIMESTAMP, HIVE_TIMESTAMP.getTypeSignature(), 6, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
    protected static final HiveColumnHandle COLUMN_DATE =
            new HiveColumnHandle("_date", HIVE_DATE, HIVE_DATE.getTypeSignature(), 7, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());

    protected static final Map<String, Type> COLUMN_TYPE_MAP = getColumnTypeMap();
    protected static final PlanSymbolAllocator SYMBOL_ALLOCATOR = new PlanSymbolAllocator(toSymbolMap(COLUMN_TYPE_MAP));
    protected static final Map<String, ColumnHandle> COLUMN_HANDLE_MAP = getColumnHandlesMap();

    private TestHivePushdownUtil()
    {
        OmniDataNodeManager nodeManager = new OmniDataNodeManager();
        HivePushdownUtil.setOmniDataNodeManager(nodeManager);
    }

    private static Map<String, ColumnHandle> getColumnHandlesMap()
    {
        ImmutableMap.Builder<String, ColumnHandle> builder = new ImmutableMap.Builder<>();
        builder.put(COLUMN_BOOLEAN.getColumnName(), COLUMN_BOOLEAN)
                .put(COLUMN_INT.getColumnName(), COLUMN_INT)
                .put(COLUMN_LONG.getColumnName(), COLUMN_LONG)
                .put(COLUMN_DOUBLE.getColumnName(), COLUMN_DOUBLE)
                .put(COLUMN_STRING.getColumnName(), COLUMN_STRING)
                .put(COLUMN_TIMESTAMP.getColumnName(), COLUMN_TIMESTAMP)
                .put(COLUMN_DATE.getColumnName(), COLUMN_DATE);
        return builder.build();
    }

    protected static Map<String, Type> getColumnTypeMap()
    {
        ImmutableMap.Builder<String, Type> builder = new ImmutableMap.Builder();
        builder.put(COLUMN_BOOLEAN.getColumnName(), BOOLEAN)
                .put(COLUMN_INT.getColumnName(), INTEGER)
                .put(COLUMN_LONG.getColumnName(), BIGINT)
                .put(COLUMN_DOUBLE.getColumnName(), DOUBLE)
                .put(COLUMN_STRING.getColumnName(), VARCHAR)
                .put(COLUMN_TIMESTAMP.getColumnName(), TIMESTAMP)
                .put(COLUMN_DATE.getColumnName(), DATE);
        return builder.build();
    }

    protected static Map<Symbol, Type> toSymbolMap(Map<String, Type> map)
    {
        return map.entrySet().stream().collect(Collectors.toMap(entry -> new Symbol(entry.getKey()), entry -> entry.getValue()));
    }

    protected static TableScanNode generateTableScanNode(HiveTableHandle hiveTableHandle, HiveColumnHandle... columnHandles)
    {
        ImmutableMap.Builder<Symbol, ColumnHandle> assignmentsBuilder = ImmutableMap.builder();
        ImmutableList.Builder<Symbol> symbolsBuilder = ImmutableList.builder();
        for (HiveColumnHandle columnHandle : columnHandles) {
            String name = columnHandle.getName().toLowerCase(Locale.ENGLISH);
            Symbol symbol = new Symbol(name);
            symbolsBuilder.add(symbol);
            assignmentsBuilder.put(symbol, columnHandle);
        }
        return PLAN_BUILDER.tableScan(OFFLOAD_TABLE_HANDLE, symbolsBuilder.build(), assignmentsBuilder.build());
    }

    protected static TableScanNode buildTableScanNode(HiveColumnHandle... columnHandles)
    {
        return generateTableScanNode(OFFLOAD_HIVE_TABLE_HANDLE, columnHandles);
    }

    protected static AggregationNode buildAggregationNode(
            PlanNode source,
            Map<Symbol, AggregationNode.Aggregation> aggregations,
            AggregationNode.GroupingSetDescriptor groupingSets)
    {
        AggregationNode aggregationNode = new AggregationNode(ID_ALLOCATOR.getNextId(), source, aggregations,
                groupingSets, Collections.emptyList(), AggregationNode.Step.PARTIAL, Optional.empty(),
                Optional.empty(), AggregationNode.AggregationType.HASH, Optional.empty());
        return aggregationNode;
    }

    protected static TableScanNode buildTableScanNode()
    {
        return generateTableScanNode(OFFLOAD_HIVE_TABLE_HANDLE, COLUMN_INT, COLUMN_LONG);
    }

    protected static LimitNode buildPartialLimitNode(PlanNode source, long count)
    {
        return new LimitNode(ID_ALLOCATOR.getNextId(), source, count, true);
    }

    protected static FilterNode buildFilterNode(PlanNode source, RowExpression predicate)
    {
        return new FilterNode(ID_ALLOCATOR.getNextId(), source, predicate);
    }

    protected static LimitNode buildLimitNode(PlanNode source, long count)
    {
        return new LimitNode(ID_ALLOCATOR.getNextId(), source, count, false);
    }

    protected static Assignments buildAssignments(List<Symbol> symbols, List<RowExpression> rowExpressions)
    {
        assertEquals(symbols.size(), rowExpressions.size());
        ImmutableMap.Builder<Symbol, RowExpression> assignments = ImmutableMap.builder();
        for (int i = 0; i < symbols.size(); i++) {
            assignments.put(symbols.get(i), rowExpressions.get(i));
        }
        return Assignments.copyOf(assignments.build());
    }

    protected static ProjectNode buildProjectNode(PlanNode source, List<Symbol> symbols, List<RowExpression> rowExpressions)
    {
        Assignments assignments = buildAssignments(symbols, rowExpressions);
        return new ProjectNode(ID_ALLOCATOR.getNextId(), source, assignments);
    }

    private static HiveOffloadExpression getCheckedOffloadExpression(PlanNode node)
    {
        assertTrue(node instanceof TableScanNode);
        ConnectorTableHandle tableHandle = ((TableScanNode) node).getTable().getConnectorHandle();
        assertTrue(tableHandle instanceof HiveTableHandle);
        HiveOffloadExpression hiveOffloadExpression = ((HiveTableHandle) tableHandle).getOffloadExpression();
        return hiveOffloadExpression;
    }

    protected static void matchLimitOffload(PlanNode node, long count)
    {
        HiveOffloadExpression expression = getCheckedOffloadExpression(node);
        assertTrue(expression.isPresent());
        assertEquals(count, expression.getLimit().getAsLong());
    }

    protected static void matchFilterOffload(PlanNode node, RowExpression predicate)
    {
        HiveOffloadExpression expression = getCheckedOffloadExpression(node);
        assertTrue(expression.isPresent());
        assertEquals(predicate, expression.getFilterExpression());
    }

    protected static void matchAggregatorOffload(PlanNode node, AggregationInfo aggregationInfoExpected)
    {
        HiveOffloadExpression expression = getCheckedOffloadExpression(node);
        assertTrue(expression.isPresent());
        assertTrue(expression.getAggregations().isPresent());
        AggregationInfo aggregationInfo = expression.getAggregations().get();
        assertEquals(aggregationInfoExpected, aggregationInfo);
    }

    protected static void matchProjection(PlanNode node, Map<Symbol, RowExpression> projections)
    {
        HiveOffloadExpression expression = getCheckedOffloadExpression(node);
        assertTrue(expression.isPresent());
        assertTrue(!expression.getProjections().isEmpty());
        assertEquals(projections, expression.getProjections());
    }

    protected static ConnectorSession getHiveOffloadSession()
    {
        HiveConfig hiveConfig = new HiveConfig().setOmniDataEnabled(true)
                .setFilterOffloadEnabled(true)
                .setAggregatorOffloadEnabled(true)
                .setMinFilterOffloadFactor(1)
                .setMinAggregatorOffloadFactor(1)
                .setMinOffloadRowNumber(1)
                .setOmniDataSslEnabled(false);
        return new TestingConnectorSession(
                new HiveSessionProperties(hiveConfig, new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
    }

    public static void simulationOmniDataConfig()
    {
        String configFile = System.getProperty(CONFIG_PROPERTY);
        if (configFile != null) {
            return;
        }

        File file = new File(SIMULATION_FLIE_NAME);
        try {
            file.createNewFile();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        System.setProperty(CONFIG_PROPERTY, SIMULATION_FLIE_NAME);
    }

    public static void unsimulationOmniDataConfig()
    {
        String configFile = System.getProperty(CONFIG_PROPERTY);
        if (configFile == null) {
            return;
        }
        if (configFile.equals(SIMULATION_FLIE_NAME)) {
            System.clearProperty(CONFIG_PROPERTY);
            File file = new File(SIMULATION_FLIE_NAME);
            file.delete();
        }
    }

    protected static CallExpression createCallExpression(String functionName, Type returnType, List<RowExpression> arguments)
    {
        List<Type> inputTypes = arguments.stream().map(expression -> expression.getType()).collect(Collectors.toList());
        FunctionHandle functionHandle = OFFLOAD_METADATA.getFunctionAndTypeManager()
                .resolveFunction(Optional.empty(), qualifyObjectName(QualifiedName.of(functionName)), TypeSignatureProvider.fromTypes(inputTypes));
        return new CallExpression(functionName, functionHandle, returnType, arguments);
    }

    protected static CallExpression createOperationExpression(OperatorType operatorType, RowExpression left, RowExpression right)
    {
        FunctionHandle functionHandle = OFFLOAD_METADATA.getFunctionAndTypeManager().resolveOperatorFunctionHandle(operatorType, fromTypes(left.getType(), right.getType()));
        return call(operatorType.name(), functionHandle, left.getType(), left, right);
    }

    protected static AggregationNode.Aggregation createAggregation(String functionName, Type returnType, List<RowExpression> arguments)
    {
        CallExpression callExpression = createCallExpression(functionName, returnType, arguments);
        return new AggregationNode.Aggregation(callExpression, arguments, false, Optional.empty(), Optional.empty(), Optional.empty());
    }

    protected static HiveMetadata simulationHiveMetadata()
    {
        // simulation chain: HiveTransactionManager -> HiveMetadata -> ColumnMetadata + TableStatistics + ColumnStatistics
        ColumnMetadata columnMetadataInt = Mockito.mock(ColumnMetadata.class);
        Mockito.when(columnMetadataInt.getName()).thenReturn(COLUMN_INT.getName());
        Mockito.when(columnMetadataInt.getType()).thenReturn(INTEGER);

        HashMap<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(STORAGE_FORMAT_PROPERTY, HiveStorageFormat.ORC);
        ConnectorTableMetadata connectorTableMetadata = Mockito.mock(ConnectorTableMetadata.class);
        Mockito.when(connectorTableMetadata.getProperties()).thenReturn(propertyMap);

        Map<ColumnHandle, ColumnStatistics> columnStatistics = new HashMap<>();
        ColumnStatistics columnStatisInt =
                new ColumnStatistics(Estimate.zero(), Estimate.of(DISTINICT_COLUMN_NUM), Estimate.unknown(), Optional.of(new DoubleRange(1, 10)));
        columnStatistics.put(COLUMN_INT, columnStatisInt);
        TableStatistics statistics = new TableStatistics(Estimate.of(OFFLOAD_COLUMN_NUM), 5, 1024, columnStatistics);
        HiveMetadata metadata = Mockito.mock(HiveMetadata.class);
        Mockito.when(metadata.getTableMetadata(OFFLOAD_SESSION, OFFLOAD_HIVE_TABLE_HANDLE)).thenReturn(connectorTableMetadata);
        Mockito.when(metadata.getColumnMetadata(Matchers.eq(OFFLOAD_SESSION), Matchers.eq(OFFLOAD_HIVE_TABLE_HANDLE), Matchers.any(ColumnHandle.class))).thenReturn(columnMetadataInt);
        Map<String, ColumnHandle> columnHandleMap = ImmutableMap.of(COLUMN_INT.getName(), COLUMN_INT);
        Mockito.when(metadata.getColumnHandles(OFFLOAD_SESSION, OFFLOAD_HIVE_TABLE_HANDLE)).thenReturn(columnHandleMap);

        Mockito.when(metadata.getTableStatistics(Matchers.eq(OFFLOAD_SESSION), Matchers.eq(OFFLOAD_HIVE_TABLE_HANDLE), Matchers.any(Constraint.class), Matchers.eq(true)))
                .thenReturn(statistics);

        return metadata;
    }

    protected static HiveTransactionManager simulationHiveTransactionManager()
    {
        HiveMetadata metadata = simulationHiveMetadata();
        HiveTransactionManager transactionManager = Mockito.mock(HiveTransactionManager.class);
        Mockito.when(transactionManager.get(OFFLOAD_TABLE_HANDLE.getTransaction())).thenReturn(metadata);
        return transactionManager;
    }
}
