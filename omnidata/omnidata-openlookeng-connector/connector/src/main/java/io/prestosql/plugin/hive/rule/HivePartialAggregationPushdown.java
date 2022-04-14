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

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableMap;
import com.huawei.boostkit.omnidata.expression.OmniExpressionChecker;
import com.huawei.boostkit.omnidata.model.AggregationInfo;
import io.airlift.log.Logger;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveOffloadExpression;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.plugin.hive.HiveTableProperties;
import io.prestosql.plugin.hive.HiveTransactionManager;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.HiveTypeTranslator;
import io.prestosql.plugin.hive.TransactionalMetadata;
import io.prestosql.spi.ConnectorPlanOptimizer;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.SymbolAllocator;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.PlanVisitor;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableBiMap.toImmutableBiMap;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.expressions.RowExpressionNodeInliner.replaceExpression;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.DUMMY_OFFLOADED;
import static io.prestosql.plugin.hive.HiveColumnHandle.DUMMY_OFFLOADED_COLUMN_INDEX;
import static io.prestosql.plugin.hive.HiveColumnHandle.DUMMY_OFFLOADED_COLUMN_NAME;
import static io.prestosql.plugin.hive.rule.HivePushdownUtil.checkTableCanOffload;
import static io.prestosql.plugin.hive.rule.HivePushdownUtil.getDataSourceColumns;
import static io.prestosql.plugin.hive.rule.HivePushdownUtil.isColumnsCanOffload;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.connector.Constraint.alwaysTrue;
import static io.prestosql.spi.plan.AggregationNode.Step.PARTIAL;
import static java.util.Objects.requireNonNull;

public class HivePartialAggregationPushdown
        implements ConnectorPlanOptimizer
{
    private static final Logger log = Logger.get(HivePartialAggregationPushdown.class);
    private static final double AGGREGATION_FACTOR_MAX = 1.0;
    private static final double AGGREGATION_FACTOR_MIN = 0.0;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;
    private final Supplier<TransactionalMetadata> metadataFactory;
    private final HiveTransactionManager transactionManager;

    @Inject
    public HivePartialAggregationPushdown(
            HiveTransactionManager transactionManager,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution,
            Supplier<TransactionalMetadata> metadataFactory)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "function manager is null");
        this.standardFunctionResolution = requireNonNull(standardFunctionResolution, "standard function resolution is null");
        this.metadataFactory = requireNonNull(metadataFactory, "metadata factory is null");
    }

    private static Optional<HiveTableHandle> getHiveTableHandle(TableScanNode tableScanNode)
    {
        TableHandle table = tableScanNode.getTable();
        if (table != null) {
            ConnectorTableHandle connectorHandle = table.getConnectorHandle();
            if (connectorHandle instanceof HiveTableHandle) {
                return Optional.of((HiveTableHandle) connectorHandle);
            }
        }
        return Optional.empty();
    }

    private static PlanNode replaceChildren(PlanNode node, List<PlanNode> children)
    {
        return children.containsAll(node.getSources()) ? node : node.replaceChildren(children);
    }

    @Override
    public PlanNode optimize(
            PlanNode maxSubplan,
            ConnectorSession session,
            Map<String, Type> types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        if (!HiveSessionProperties.isOmniDataEnabled(session) || !HiveSessionProperties.isAggregatorOffloadEnabled(session)) {
            return maxSubplan;
        }
        return maxSubplan.accept(new Visitor(session, idAllocator, types, symbolAllocator), null);
    }

    private class Visitor
            extends PlanVisitor<PlanNode, Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final ConnectorSession session;
        private final Map<String, Type> types;
        private final SymbolAllocator symbolAllocator;

        public Visitor(
                ConnectorSession session,
                PlanNodeIdAllocator idAllocator,
                Map<String, Type> types,
                SymbolAllocator symbolAllocator)
        {
            this.session = session;
            this.idAllocator = idAllocator;
            this.symbolAllocator = symbolAllocator;
            this.types = types;
        }

        private boolean isAggregationPushdownSupported(AggregationNode partialAggregationNode)
        {
            if (!partialAggregationNode.getPreGroupedSymbols().isEmpty()
                    || partialAggregationNode.getHashSymbol().isPresent()
                    || partialAggregationNode.getGroupIdSymbol().isPresent()) {
                return false;
            }

            for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : partialAggregationNode.getAggregations().entrySet()) {
                AggregationNode.Aggregation aggregation = entry.getValue();
                if (aggregation.isDistinct() || aggregation.getFilter().isPresent()
                        || aggregation.getMask().isPresent() || aggregation.getOrderingScheme().isPresent()) {
                    return false;
                }
                if (!OmniExpressionChecker.checkAggregateFunction(aggregation.getFunctionCall())) {
                    return false;
                }
            }

            TableScanNode tableScanNode;
            if (partialAggregationNode.getSource() instanceof TableScanNode) {
                tableScanNode = (TableScanNode) partialAggregationNode.getSource();
            }
            else {
                if (!(partialAggregationNode.getSource() instanceof ProjectNode)
                        || !(((ProjectNode) partialAggregationNode.getSource()).getSource() instanceof TableScanNode)) {
                    return false;
                }
                tableScanNode = (TableScanNode) (((ProjectNode) partialAggregationNode.getSource()).getSource());
            }
            ConnectorTableMetadata connectorTableMetadata = metadataFactory.get().getTableMetadata(session, tableScanNode.getTable().getConnectorHandle());
            Optional<Object> rawFormat = Optional.ofNullable(connectorTableMetadata.getProperties().get(HiveTableProperties.STORAGE_FORMAT_PROPERTY));
            if (!rawFormat.isPresent()) {
                return false;
            }

            if (true != checkTableCanOffload(tableScanNode, connectorTableMetadata)) {
                return false;
            }

            TableHandle tableHandle = tableScanNode.getTable();
            checkArgument(tableHandle.getConnectorHandle() instanceof HiveTableHandle, "Only supports hive TableHandle");
            HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle.getConnectorHandle();
            if (!isColumnsCanOffload(hiveTableHandle, tableScanNode.getOutputSymbols(), types)) {
                return false;
            }

            return true;
        }

        private Optional<AggregationInfo> buildAggregationInfo(AggregationNode partialAggregationNode, TableScanNode tableScanNode)
        {
            BiMap<VariableReferenceExpression, VariableReferenceExpression> symbolToColumnMapping =
                    tableScanNode.getAssignments().entrySet().stream().collect(toImmutableBiMap(
                            entry -> new VariableReferenceExpression(entry.getKey().getName(), types.get(entry.getKey().getName())),
                            entry -> new VariableReferenceExpression(entry.getValue().getColumnName(), types.get(entry.getKey().getName()))));

            ImmutableMap.Builder<String, AggregationInfo.AggregateFunction> aggregationsBuilder = ImmutableMap.builder();
            for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : partialAggregationNode.getAggregations().entrySet()) {
                RowExpression expression = replaceExpression(entry.getValue().getFunctionCall(), symbolToColumnMapping);
                checkArgument(expression instanceof CallExpression, "Replace result is not callExpression");
                CallExpression callExpression = (CallExpression) expression;
                AggregationInfo.AggregateFunction function = new AggregationInfo.AggregateFunction(callExpression, entry.getValue().isDistinct());
                aggregationsBuilder.put(entry.getKey().getName(), function);
            }
            List<RowExpression> groupingKeys = partialAggregationNode.getGroupingKeys().stream()
                    .map(entry -> replaceExpression(new VariableReferenceExpression(entry.getName(),
                            types.get(entry.getName())), symbolToColumnMapping)).collect(toImmutableList());
            AggregationInfo aggregationInfo = new AggregationInfo(aggregationsBuilder.build(), groupingKeys);
            return Optional.of(aggregationInfo);
        }

        private Optional<TableScanNode> tryProjectPushdown(AggregationNode aggregationNode)
        {
            if (aggregationNode.getSource() instanceof TableScanNode) {
                return Optional.of((TableScanNode) aggregationNode.getSource());
            }

            if (!(aggregationNode.getSource() instanceof ProjectNode)) {
                return Optional.empty();
            }

            ProjectNode projectNode = (ProjectNode) aggregationNode.getSource();
            return HiveProjectPushdown.tryProjectPushdown(projectNode, types);
        }

        private Optional<PlanNode> tryPartialAggregationPushdown(PlanNode plan)
        {
            if (!(plan instanceof AggregationNode && ((AggregationNode) plan).getStep().equals(PARTIAL))) {
                return Optional.empty();
            }
            AggregationNode partialAggregationNode = (AggregationNode) plan;
            if (!((partialAggregationNode.getSource() instanceof TableScanNode) ||
                    (partialAggregationNode.getSource() instanceof ProjectNode && ((ProjectNode) partialAggregationNode.getSource()).getSource() instanceof TableScanNode))) {
                return Optional.empty();
            }

            if (!isAggregationPushdownSupported(partialAggregationNode) || !HivePushdownUtil.isOmniDataNodesNormal()) {
                return Optional.empty();
            }

            double aggregationFactor = getAggregationFactor(partialAggregationNode, session, transactionManager);
            if (aggregationFactor > HiveSessionProperties.getMinAggregatorOffloadFactor(session)) {
                return Optional.empty();
            }

            Optional<TableScanNode> oldTableScanNode = tryProjectPushdown(partialAggregationNode);
            if (!oldTableScanNode.isPresent()) {
                return Optional.empty();
            }
            TableHandle oldTableHandle = oldTableScanNode.get().getTable();
            HiveTableHandle hiveTableHandle = getHiveTableHandle(oldTableScanNode.get()).orElseThrow(() -> new PrestoException(NOT_FOUND, "Hive table handle not found"));

            HiveTypeTranslator hiveTypeTranslator = new HiveTypeTranslator();
            Map<Symbol, ColumnHandle> assignments = new HashMap<>();
            for (Map.Entry<Symbol, AggregationNode.Aggregation> aggregationEntry : partialAggregationNode.getAggregations().entrySet()) {
                CallExpression callExpression = aggregationEntry.getValue().getFunctionCall();
                ColumnHandle newColumnHandle;
                TypeSignature typeSignature = callExpression.getType().getTypeSignature();
                if (callExpression.getArguments().isEmpty()) {
                    HiveType hiveType = HiveType.toHiveType(hiveTypeTranslator, callExpression.getType());
                    newColumnHandle = new HiveColumnHandle(DUMMY_OFFLOADED_COLUMN_NAME, hiveType, typeSignature, DUMMY_OFFLOADED_COLUMN_INDEX,
                            DUMMY_OFFLOADED, Optional.of("partial aggregation pushed down " + aggregationEntry.getKey().getName()), false);
                }
                else {
                    RowExpression column = callExpression.getArguments().get(0);
                    if (!(column instanceof VariableReferenceExpression)) {
                        return Optional.empty();
                    }
                    String columnName = column.toString();
                    HiveColumnHandle columnHandle = (HiveColumnHandle) oldTableScanNode.get().getAssignments().get(new Symbol(columnName));
                    HiveType hiveType;
                    if (callExpression.getType() instanceof RowType) {
                        hiveType = columnHandle.getHiveType();
                    }
                    else {
                        hiveType = HiveType.toHiveType(hiveTypeTranslator, callExpression.getType());
                    }
                    newColumnHandle = new HiveColumnHandle(columnHandle.getName(), hiveType, typeSignature, columnHandle.getHiveColumnIndex(),
                            DUMMY_OFFLOADED, Optional.of("partial aggregation pushed down " + aggregationEntry.getKey().getName()), false);
                }
                assignments.put(aggregationEntry.getKey(), newColumnHandle);
            }

            for (Symbol symbol : partialAggregationNode.getGroupingKeys()) {
                HiveColumnHandle groupingColumn = (HiveColumnHandle) oldTableScanNode.get().getAssignments().get(symbol);
                groupingColumn = new HiveColumnHandle(groupingColumn.getName(), groupingColumn.getHiveType(), groupingColumn.getTypeSignature(), groupingColumn.getHiveColumnIndex(),
                        DUMMY_OFFLOADED, Optional.of("partial aggregation pushed down " + symbol.getName()), false);
                assignments.put(symbol, groupingColumn);
            }

            HiveOffloadExpression offloadExpression = hiveTableHandle.getOffloadExpression();
            HiveTableHandle newHiveTableHandle = hiveTableHandle.withOffloadExpression(
                    offloadExpression.updateAggregation(buildAggregationInfo(partialAggregationNode, oldTableScanNode.get()), getDataSourceColumns(oldTableScanNode.get())));
            TableHandle newTableHandle = new TableHandle(
                    oldTableHandle.getCatalogName(),
                    newHiveTableHandle,
                    oldTableHandle.getTransaction(),
                    oldTableHandle.getLayout());

            TableScanNode newTableScan =
                    new TableScanNode(
                            oldTableScanNode.get().getId(),
                            newTableHandle,
                            partialAggregationNode.getOutputSymbols(),
                            assignments,
                            oldTableScanNode.get().getEnforcedConstraint(),
                            oldTableScanNode.get().getPredicate(),
                            oldTableScanNode.get().getStrategy(),
                            oldTableScanNode.get().getReuseTableScanMappingId(),
                            oldTableScanNode.get().getConsumerTableScanNodeCount(),
                            oldTableScanNode.get().isForDelete());
            log.info("Offloading: table %s, aggregation factor[%.2f%%], aggregation[%s] .",
                    newTableHandle.getConnectorHandle().getTableName(), aggregationFactor * 100,
                    HiveOffloadExpression.aggregationInfoToString(newHiveTableHandle.getOffloadExpression().getAggregations().get()));
            return Optional.of(newTableScan);
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Void context)
        {
            Optional<PlanNode> pushedDownPlan = tryPartialAggregationPushdown(node);
            return pushedDownPlan.orElseGet(() -> replaceChildren(
                    node,
                    node.getSources().stream().map(source -> source.accept(this, null)).collect(toImmutableList())));
        }

        private double getAggregationFactor(
                AggregationNode aggregationNode,
                ConnectorSession connectorSession,
                HiveTransactionManager hiveTransactionManager)
        {
            TableScanNode tableScanNode;
            if (aggregationNode.getSource() instanceof TableScanNode) {
                tableScanNode = (TableScanNode) aggregationNode.getSource();
            }
            else if (aggregationNode.getSource() instanceof ProjectNode && ((ProjectNode) aggregationNode.getSource()).getSource() instanceof TableScanNode) {
                tableScanNode = (TableScanNode) ((ProjectNode) aggregationNode.getSource()).getSource();
            }
            else {
                return AGGREGATION_FACTOR_MAX;
            }

            ConnectorTableHandle connectorHandle = tableScanNode.getTable().getConnectorHandle();
            if (!(connectorHandle instanceof HiveTableHandle)) {
                return AGGREGATION_FACTOR_MAX;
            }
            HiveTableHandle tableHandle = (HiveTableHandle) connectorHandle;
            ConnectorMetadata metadata = hiveTransactionManager.get(tableScanNode.getTable().getTransaction());
            TableStatistics statistics = metadata.getTableStatistics(connectorSession, tableHandle, alwaysTrue(), true);
            if (statistics.getRowCount().isUnknown() || statistics.getRowCount().getValue() < HiveSessionProperties.getMinOffloadRowNumber(connectorSession)) {
                log.info("Aggregation:Table %s row number[%d], expect min row number[%d].",
                        tableHandle.getTableName(),
                        (long) statistics.getRowCount().getValue(),
                        HiveSessionProperties.getMinOffloadRowNumber(connectorSession));
                return AGGREGATION_FACTOR_MAX;
            }

            if (aggregationNode.getGroupingKeys().isEmpty()) {
                return AGGREGATION_FACTOR_MIN;
            }

            double rowsCount = 1;
            Map<String, ColumnStatistics> statisticsMap = statistics.getColumnStatistics()
                    .entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey().getColumnName(), entry -> entry.getValue()));
            for (Symbol symbol : aggregationNode.getGroupingKeys()) {
                ColumnHandle columnHandle = tableScanNode.getAssignments().getOrDefault(symbol, null);
                if (columnHandle == null) {
                    return AGGREGATION_FACTOR_MAX;
                }
                ColumnStatistics columnStatistics = statisticsMap.getOrDefault(columnHandle.getColumnName(), null);
                if (columnStatistics == null || columnStatistics.getDistinctValuesCount().isUnknown()) {
                    return AGGREGATION_FACTOR_MAX;
                }
                int nullRow = (columnStatistics.getNullsFraction().getValue() == 0.0) ? 0 : 1;
                rowsCount *= columnStatistics.getDistinctValuesCount().getValue() + nullRow;
            }
            return rowsCount / statistics.getRowCount().getValue();
        }
    }
}
