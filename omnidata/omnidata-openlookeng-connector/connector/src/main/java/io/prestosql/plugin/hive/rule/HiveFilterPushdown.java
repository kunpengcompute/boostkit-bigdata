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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.huawei.boostkit.omnidata.expression.OmniExpressionChecker;
import io.airlift.log.Logger;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveMetadata;
import io.prestosql.plugin.hive.HiveOffloadExpression;
import io.prestosql.plugin.hive.HivePartitionManager;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.plugin.hive.HiveTransactionManager;
import io.prestosql.spi.ConnectorPlanOptimizer;
import io.prestosql.spi.SymbolAllocator;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.FilterStatsCalculatorService;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.PlanVisitor;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.DomainTranslator;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.Type;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableBiMap.toImmutableBiMap;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static io.prestosql.expressions.LogicalRowExpressions.FALSE_CONSTANT;
import static io.prestosql.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static io.prestosql.expressions.LogicalRowExpressions.extractConjuncts;
import static io.prestosql.expressions.RowExpressionNodeInliner.replaceExpression;
import static io.prestosql.plugin.hive.rule.HivePushdownUtil.checkStorageFormat;
import static io.prestosql.plugin.hive.rule.HivePushdownUtil.isColumnsCanOffload;
import static io.prestosql.spi.relation.DomainTranslator.BASIC_COLUMN_EXTRACTOR;
import static java.util.Objects.requireNonNull;

public class HiveFilterPushdown
        implements ConnectorPlanOptimizer
{
    private static final String DYNAMIC_FILTER_FUNCTION_NAME = "$internal$dynamic_filter_function";
    private static final Logger log = Logger.get(HiveFilterPushdown.class);

    private final HiveTransactionManager transactionManager;
    private final RowExpressionService rowExpressionService;
    private final StandardFunctionResolution functionResolution;
    private final HivePartitionManager partitionManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final FilterStatsCalculatorService filterCalculatorService;

    public HiveFilterPushdown(
            HiveTransactionManager transactionManager,
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution,
            HivePartitionManager partitionManager,
            FilterStatsCalculatorService filterCalculatorService,
            FunctionMetadataManager functionMetadataManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.filterCalculatorService = filterCalculatorService;
    }

    @Override
    public PlanNode optimize(
            PlanNode maxSubPlan,
            ConnectorSession session,
            Map<String, Type> types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        if (!HiveSessionProperties.isOmniDataEnabled(session) || !HiveSessionProperties.isFilterOffloadEnabled(session)) {
            return maxSubPlan;
        }
        return maxSubPlan.accept(new Visitor(session, idAllocator, types, symbolAllocator), null);
    }

    private static ExpressionExtractResult extractOffloadExpression(
            RowExpression predicate,
            LogicalRowExpressions logicalRowExpressions,
            RowExpressionService rowExpressionService)
    {
        List<RowExpression> offloadList = new ArrayList<>();
        List<RowExpression> remainingList = new ArrayList<>();
        List<RowExpression> conjuncts = extractConjuncts(predicate);
        for (RowExpression expression : conjuncts) {
            // filter nondeterministic and dynamic filter expression
            if (!rowExpressionService.getDeterminismEvaluator().isDeterministic(expression)) {
                remainingList.add(expression);
                continue;
            }
            if (expression instanceof CallExpression) {
                CallExpression call = (CallExpression) expression;
                if (call.getDisplayName().equals(DYNAMIC_FILTER_FUNCTION_NAME)) {
                    remainingList.add(expression);
                    continue;
                }
            }

            if (!OmniExpressionChecker.checkExpression(expression)) {
                remainingList.add(expression);
                continue;
            }

            offloadList.add(expression);
        }

        RowExpression offloadExpression = offloadList.isEmpty() ? TRUE_CONSTANT : logicalRowExpressions.combineConjuncts(offloadList);
        RowExpression remainingExpression = remainingList.isEmpty() ? TRUE_CONSTANT : logicalRowExpressions.combineConjuncts(remainingList);
        return new ExpressionExtractResult(offloadExpression, remainingExpression);
    }

    private static boolean determineOffloadExpression(
            RowExpression offloadExpression,
            ConnectorTableHandle tableHandle,
            HiveMetadata metadata,
            ConnectorSession session,
            RowExpressionService rowExpressionService,
            Map<String, ColumnHandle> columnHandlesMap,
            FilterStatsCalculatorService filterCalculatorService,
            Map<String, Type> typesMap)
    {
        // decompose expression
        DomainTranslator translator = rowExpressionService.getDomainTranslator();
        DomainTranslator.ExtractionResult<VariableReferenceExpression> decomposedFilter =
                translator.fromPredicate(session, offloadExpression, BASIC_COLUMN_EXTRACTOR);
        TupleDomain<ColumnHandle> entireColumnDomain = decomposedFilter.getTupleDomain()
                .transform(variableName -> columnHandlesMap.get(variableName.getName()));
        Constraint constraint;
        if (TRUE_CONSTANT.equals(decomposedFilter.getRemainingExpression())) {
            constraint = new Constraint(entireColumnDomain);
        }
        else {
            // TODO: evaluator is needed? or getTupleDomain().isAll()?
            ConstraintEvaluator evaluator = new ConstraintEvaluator(rowExpressionService, session, columnHandlesMap, decomposedFilter.getRemainingExpression());
            constraint = new Constraint(entireColumnDomain, evaluator::isCandidate);
        }
        return evaluateFilterBenefit(tableHandle, columnHandlesMap, metadata, filterCalculatorService, offloadExpression, constraint, session, typesMap);
    }

    @VisibleForTesting
    public static ConnectorPushdownFilterResult pushdownFilter(
            HiveMetadata metadata,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            RowExpression predicate,
            Map<String, Type> typesMap,
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution,
            FunctionMetadataManager functionMetadataManager,
            FilterStatsCalculatorService filterCalculatorService)
    {
        checkArgument(!FALSE_CONSTANT.equals(predicate), "Cannot pushdown filter that is always false");
        checkArgument(tableHandle instanceof HiveTableHandle, "Only supports hive TableHandle");

        LogicalRowExpressions logicalRowExpressions =
                new LogicalRowExpressions(rowExpressionService.getDeterminismEvaluator(), functionResolution, functionMetadataManager);
        ExpressionExtractResult expressionExtractResult = extractOffloadExpression(predicate, logicalRowExpressions, rowExpressionService);
        if (TRUE_CONSTANT.equals(expressionExtractResult.getOffloadExpression())) {
            return new ConnectorPushdownFilterResult(Optional.empty(), TRUE_CONSTANT);
        }

        /// TODO: handle partition column? handle predicate in tableScan node?
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        Map<String, ColumnHandle> columnHandlesMap = metadata.getColumnHandles(session, tableHandle);
        HiveOffloadExpression oldOffloadExpression = hiveTableHandle.getOffloadExpression();
        RowExpression filterExpression = TRUE_CONSTANT.equals(oldOffloadExpression.getFilterExpression()) ?
                expressionExtractResult.getOffloadExpression() : logicalRowExpressions.combineConjuncts(oldOffloadExpression.getFilterExpression(), expressionExtractResult.getOffloadExpression());
        RowExpression optimizedExpression = filterExpression;
        if (true != determineOffloadExpression(optimizedExpression, tableHandle, metadata, session, rowExpressionService, columnHandlesMap, filterCalculatorService, typesMap)) {
            return new ConnectorPushdownFilterResult(Optional.empty(), TRUE_CONSTANT);
        }

        Set<HiveColumnHandle> offloadColumns = HivePushdownUtil.extractAll(optimizedExpression).stream()
                .map(entry -> (HiveColumnHandle) columnHandlesMap.get(entry.getName())).collect(Collectors.toSet());
        Optional<ConnectorTableHandle> newTableHandle =
                Optional.of(hiveTableHandle.withOffloadExpression(oldOffloadExpression.updateFilter(optimizedExpression, offloadColumns)));
        return new ConnectorPushdownFilterResult(newTableHandle, expressionExtractResult.getRemainingExpression());
    }

    private static Map<Integer, Symbol> formSymbolsLayout(Map<ColumnHandle, String> columnHandlesMap)
    {
        Map<Integer, Symbol> layout = new LinkedHashMap<>();
        int channel = 0;
        for (Map.Entry<ColumnHandle, String> entry : columnHandlesMap.entrySet()) {
            layout.put(channel++, new Symbol(entry.getValue()));
        }
        return layout;
    }

    private static boolean evaluateFilterBenefit(
            ConnectorTableHandle tableHandle,
            Map<String, ColumnHandle> columnHandlesMap,
            HiveMetadata metadata,
            FilterStatsCalculatorService filterCalculatorService,
            RowExpression predicate,
            Constraint constraint,
            ConnectorSession session,
            Map<String, Type> typesMap)
    {
        // TODO: total data size
        TableStatistics statistics = metadata.getTableStatistics(session, tableHandle, constraint, true);
        if (statistics.getRowCount().isUnknown() || statistics.getRowCount().getValue() < HiveSessionProperties.getMinOffloadRowNumber(session)) {
            log.info("Filter:Table %s row number[%d], expect min row number[%d], predicate[%s].",
                    tableHandle.getTableName(),
                    (long) statistics.getRowCount().getValue(),
                    HiveSessionProperties.getMinOffloadRowNumber(session),
                    predicate.toString());
            return false;
        }

        Map<ColumnHandle, String> allColumns = columnHandlesMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        Map<String, Type> allColumnTypes = allColumns.entrySet().stream().collect(toImmutableMap(
                entry -> entry.getValue(), entry -> metadata.getColumnMetadata(session, tableHandle, entry.getKey()).getType()));
        Map<Symbol, Type> symbolsMap = typesMap.entrySet().stream()
                .collect(toImmutableMap(entry -> new Symbol(entry.getKey()), entry -> entry.getValue()));
        TableStatistics filterStatistics = filterCalculatorService.filterStats(statistics, predicate, session,
                allColumns, allColumnTypes, symbolsMap, formSymbolsLayout(allColumns));
        Estimate filteredRowCount = filterStatistics.getRowCount().isUnknown() ? statistics.getRowCount() : filterStatistics.getRowCount();
        double filterFactor = filteredRowCount.getValue() / statistics.getRowCount().getValue();
        if (filterFactor <= HiveSessionProperties.getMinFilterOffloadFactor(session)) {
            log.info("Offloading: table %s, size[%d], predicate[%s], filter factor[%.2f%%].",
                    tableHandle.getTableName(), (long) statistics.getRowCount().getValue(),
                    predicate.toString(), filterFactor * 100);
            return true;
        }
        else {
            log.info("No need to offload: table %s, size[%d], predicate[%s], filter factor[%.2f%%].",
                    tableHandle.getTableName(), (long) statistics.getRowCount().getValue(),
                    predicate.toString(), filterFactor * 100);
        }
        return false;
    }

    private static class ConnectorPushdownFilterResult
    {
        private final Optional<ConnectorTableHandle> tableHandle;
        private final RowExpression remainingExpression;

        public ConnectorPushdownFilterResult(
                Optional<ConnectorTableHandle> tableHandle, RowExpression remainingExpression)
        {
            this.tableHandle = requireNonNull(tableHandle, "handle is null");
            this.remainingExpression = requireNonNull(remainingExpression, "remainingExpression is null");
        }

        public Optional<ConnectorTableHandle> getTableHandle()
        {
            return tableHandle;
        }

        public RowExpression getRemainingExpression()
        {
            return remainingExpression;
        }
    }

    private class Visitor
            extends PlanVisitor<PlanNode, Void>
    {
        private final ConnectorSession session;
        private final PlanNodeIdAllocator idAllocator;
        private final Map<String, Type> types;
        private final SymbolAllocator symbolAllocator;

        Visitor(
                ConnectorSession session,
                PlanNodeIdAllocator idAllocator,
                Map<String, Type> types,
                SymbolAllocator symbolAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.types = requireNonNull(types, "types is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Void context)
        {
            ImmutableList.Builder<PlanNode> children = ImmutableList.builder();
            boolean changed = false;
            for (PlanNode child : node.getSources()) {
                PlanNode newChild = child.accept(this, null);
                if (newChild != child) {
                    changed = true;
                }
                children.add(newChild);
            }

            if (!changed) {
                return node;
            }
            return node.replaceChildren(children.build());
        }

        private String getColumnName(
                ConnectorSession session,
                HiveMetadata metadata,
                ConnectorTableHandle tableHandle,
                ColumnHandle columnHandle)
        {
            return metadata.getColumnMetadata(session, tableHandle, columnHandle).getName();
        }

        @Override
        public PlanNode visitFilter(FilterNode filterNode, Void context)
        {
            if (!(filterNode.getSource() instanceof TableScanNode)) {
                return visitPlan(filterNode, context);
            }

            TableScanNode tableScan = (TableScanNode) filterNode.getSource();
            if (!isOperatorOffloadSupported(session, tableScan.getTable())) {
                return filterNode;
            }

            if (!HivePushdownUtil.isOmniDataNodesNormal() || !isColumnsCanOffload(tableScan.getTable().getConnectorHandle(), tableScan.getOutputSymbols(), types)) {
                return filterNode;
            }

            RowExpression expression = filterNode.getPredicate();
            TableHandle tableHandle = tableScan.getTable();
            HiveMetadata hiveMetadata = getMetadata(tableHandle);

            BiMap<VariableReferenceExpression, VariableReferenceExpression> symbolToColumnMapping =
                    tableScan.getAssignments().entrySet().stream().collect(toImmutableBiMap(
                            entry -> new VariableReferenceExpression(entry.getKey().getName(), types.get(entry.getKey().getName())),
                            entry -> new VariableReferenceExpression(getColumnName(session, hiveMetadata,
                                    tableHandle.getConnectorHandle(), entry.getValue()), types.get(entry.getKey().getName()))));
            RowExpression replacedExpression = replaceExpression(expression, symbolToColumnMapping);
            // replaceExpression() may further optimize the expression; if the resulting expression is always false,
            // then return empty Values node
            if (FALSE_CONSTANT.equals(replacedExpression)) {
                return new ValuesNode(idAllocator.getNextId(), tableScan.getOutputSymbols(), ImmutableList.of());
            }

            ConnectorPushdownFilterResult pushdownFilterResult = pushdownFilter(hiveMetadata, session, tableHandle.getConnectorHandle(),
                    replacedExpression, types, rowExpressionService, functionResolution, functionMetadataManager, filterCalculatorService);
            if (!pushdownFilterResult.getTableHandle().isPresent()) {
                return filterNode;
            }

            TableHandle newTableHandle =
                    new TableHandle(
                            tableHandle.getCatalogName(),
                            pushdownFilterResult.getTableHandle().get(),
                            tableHandle.getTransaction(),
                            tableHandle.getLayout());
            TableScanNode newTableScan =
                    new TableScanNode(
                            tableScan.getId(),
                            newTableHandle,
                            tableScan.getOutputSymbols(),
                            tableScan.getAssignments(),
                            tableScan.getEnforcedConstraint(),
                            tableScan.getPredicate(),
                            tableScan.getStrategy(),
                            tableScan.getReuseTableScanMappingId(),
                            tableScan.getConsumerTableScanNodeCount(),
                            tableScan.isForDelete());

            if (!TRUE_CONSTANT.equals(pushdownFilterResult.getRemainingExpression())) {
                return new FilterNode(
                        idAllocator.getNextId(), newTableScan,
                        replaceExpression(pushdownFilterResult.getRemainingExpression(), symbolToColumnMapping.inverse()));
            }
            return newTableScan;
        }
    }

    private static class ConstraintEvaluator
    {
        private final Map<String, ColumnHandle> assignments;
        private final RowExpressionService evaluator;
        private final ConnectorSession session;
        private final RowExpression expression;
        private final Set<ColumnHandle> arguments;

        public ConstraintEvaluator(RowExpressionService evaluator, ConnectorSession session, Map<String, ColumnHandle> assignments, RowExpression expression)
        {
            this.assignments = assignments;
            this.evaluator = evaluator;
            this.session = session;
            this.expression = expression;

            arguments = ImmutableSet.copyOf(HivePushdownUtil.extractAll(expression)).stream()
                    .map(VariableReferenceExpression::getName)
                    .map(assignments::get)
                    .collect(toImmutableSet());
        }

        private boolean isCandidate(Map<ColumnHandle, NullableValue> bindings)
        {
            if (intersection(bindings.keySet(), arguments).isEmpty()) {
                return true;
            }

            Function<VariableReferenceExpression, Object> variableResolver = variable -> {
                ColumnHandle column = assignments.get(variable.getName());
                checkArgument(column != null, "Missing column assignment for %s", variable);

                if (!bindings.containsKey(column)) {
                    return variable;
                }

                return bindings.get(column).getValue();
            };

            // If any conjuncts evaluate to FALSE or null, then the whole predicate will never be true and so the partition should be pruned
            return !Boolean.FALSE.equals(expression) && expression != null && (!(expression instanceof ConstantExpression) || !((ConstantExpression) expression).isNull());
        }
    }

    private HiveMetadata getMetadata(TableHandle tableHandle)
    {
        ConnectorMetadata metadata = transactionManager.get(tableHandle.getTransaction());
        checkState(metadata instanceof HiveMetadata, "metadata must be HiveMetadata");
        return (HiveMetadata) metadata;
    }

    protected boolean isOperatorOffloadSupported(ConnectorSession session, TableHandle tableHandle)
    {
        ConnectorTableMetadata metadata = getMetadata(tableHandle).getTableMetadata(session, tableHandle.getConnectorHandle());
        return checkStorageFormat(metadata);
    }

    public static class ExpressionExtractResult
    {
        private final RowExpression offloadExpression;
        private final RowExpression remainingExpression;

        public ExpressionExtractResult(RowExpression offloadExpression, RowExpression remainingExpression)
        {
            this.offloadExpression = requireNonNull(offloadExpression, "offloadExpression is null");
            this.remainingExpression = requireNonNull(remainingExpression, "remainingExpression is null");
        }

        public RowExpression getOffloadExpression()
        {
            return offloadExpression;
        }

        public RowExpression getRemainingExpression()
        {
            return remainingExpression;
        }
    }
}
