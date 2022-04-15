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
import io.prestosql.plugin.hive.HiveMetadata;
import io.prestosql.plugin.hive.HiveOffloadExpression;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.plugin.hive.HiveTransactionManager;
import io.prestosql.spi.ConnectorPlanOptimizer;
import io.prestosql.spi.SymbolAllocator;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.PlanVisitor;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.type.Type;

import java.util.Map;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.hive.rule.HivePushdownUtil.isColumnsCanOffload;
import static java.util.Objects.requireNonNull;

public class HiveLimitPushdown
        implements ConnectorPlanOptimizer
{
    private final HiveTransactionManager transactionManager;

    public HiveLimitPushdown(HiveTransactionManager transactionManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    public PlanNode optimize(
            PlanNode maxSubPlan,
            ConnectorSession session,
            Map<String, Type> types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(maxSubPlan, "maxSubPlan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        if (!HiveSessionProperties.isOmniDataEnabled(session)) {
            return maxSubPlan;
        }
        return maxSubPlan.accept(new Visitor(types, session), null);
    }

    private class Visitor
            extends PlanVisitor<PlanNode, Void>
    {
        private final Map<String, Type> types;
        private final ConnectorSession session;

        public Visitor(Map<String, Type> types, ConnectorSession session)
        {
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Void context)
        {
            requireNonNull(node, "plan node is null");
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

        @Override
        public PlanNode visitLimit(LimitNode limitNode, Void context)
        {
            requireNonNull(limitNode, "limit node is null");
            if (!(limitNode.getSource() instanceof TableScanNode && limitNode.isPartial())) {
                return visitPlan(limitNode, context);
            }
            checkArgument(limitNode.getCount() >= 0, "limit must be at least zero");
            TableScanNode tableScan = (TableScanNode) limitNode.getSource();
            TableHandle tableHandle = tableScan.getTable();

            ConnectorMetadata connectorMetadata = transactionManager.get(tableHandle.getTransaction());
            if (!(connectorMetadata instanceof HiveMetadata)) {
                return visitPlan(limitNode, context);
            }
            ConnectorTableMetadata metadata = connectorMetadata.getTableMetadata(session, tableHandle.getConnectorHandle());
            if (true != HivePushdownUtil.checkTableCanOffload(tableScan, metadata)) {
                return visitPlan(limitNode, context);
            }

            if (!HivePushdownUtil.isOmniDataNodesNormal() || !isColumnsCanOffload(tableHandle.getConnectorHandle(), tableScan.getOutputSymbols(), types)) {
                return visitPlan(limitNode, context);
            }

            HiveOffloadExpression offloadExpression = ((HiveTableHandle) tableHandle.getConnectorHandle()).getOffloadExpression();
            TableHandle newTableHandle =
                    new TableHandle(
                    tableHandle.getCatalogName(),
                    ((HiveTableHandle) tableHandle.getConnectorHandle()).withOffloadExpression(offloadExpression.updateLimit(OptionalLong.of(limitNode.getCount()))),
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
            return newTableScan;
        }
    }
}
