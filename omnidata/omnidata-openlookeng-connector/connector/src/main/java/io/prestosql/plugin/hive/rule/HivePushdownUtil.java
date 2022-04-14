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
import com.huawei.boostkit.omnidata.expression.OmniExpressionChecker;
import io.prestosql.expressions.DefaultRowExpressionTraversalVisitor;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveStorageFormat;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.plugin.hive.omnidata.OmniDataNodeManager;
import io.prestosql.plugin.hive.omnidata.OmniDataNodeStatus;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.DUMMY_OFFLOADED;
import static io.prestosql.plugin.hive.HiveTableProperties.getHiveStorageFormat;

public class HivePushdownUtil
{
    private static final double OMNIDATA_BUSY_PERCENT = 0.8;
    private static Optional<OmniDataNodeManager> omniDataNodeManager = Optional.empty();

    private HivePushdownUtil() {}

    public static Set<HiveColumnHandle> getDataSourceColumns(TableScanNode node)
    {
        ImmutableSet.Builder<HiveColumnHandle> builder = new ImmutableSet.Builder<>();
        for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
            HiveColumnHandle hiveColumnHandle = (HiveColumnHandle) entry.getValue();
            if (hiveColumnHandle.getColumnType().equals(DUMMY_OFFLOADED)) {
                continue;
            }
            builder.add(hiveColumnHandle);
        }

        return builder.build();
    }

    public static Set<VariableReferenceExpression> extractAll(RowExpression expression)
    {
        ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
        expression.accept(new VariableReferenceBuilderVisitor(), builder);
        return builder.build();
    }

    public static class VariableReferenceBuilderVisitor
            extends DefaultRowExpressionTraversalVisitor<ImmutableSet.Builder<VariableReferenceExpression>>
    {
        @Override
        public Void visitVariableReference(VariableReferenceExpression variable, ImmutableSet.Builder<VariableReferenceExpression> builder)
        {
            builder.add(variable);
            return null;
        }
    }

    public static boolean isColumnsCanOffload(ConnectorTableHandle tableHandle, List<Symbol> outputSymbols, Map<String, Type> typesMap)
    {
        // just for performance, avoid query types
        if (tableHandle instanceof HiveTableHandle) {
            HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
            if (hiveTableHandle.getOffloadExpression().isPresent()) {
                return true;
            }
        }

        for (Symbol symbol : outputSymbols) {
            Type type = typesMap.get(symbol.getName());
            if (!OmniExpressionChecker.checkType(type)) {
                return false;
            }
        }
        return true;
    }

    public static void setOmniDataNodeManager(OmniDataNodeManager manager)
    {
        omniDataNodeManager = Optional.of(manager);
    }

    /**
     * Check that all omniDatas nodes are in the normal state.
     *
     * @return true/false
     */
    public static boolean isOmniDataNodesNormal()
    {
        if (!omniDataNodeManager.isPresent()) {
            return false;
        }
        Map<String, OmniDataNodeStatus> nodeStatusMap = omniDataNodeManager.get().getAllNodes();
        // TODO: check online node number
        for (Map.Entry<String, OmniDataNodeStatus> entry : nodeStatusMap.entrySet()) {
            OmniDataNodeStatus omniDataNode = entry.getValue();
            if (omniDataNode.getRunningTaskNumber() > omniDataNode.getMaxTaskNumber() * OMNIDATA_BUSY_PERCENT) {
                return false;
            }
        }
        return true;
    }

    public static boolean checkStorageFormat(ConnectorTableMetadata metadata)
    {
        HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(metadata.getProperties());
        if (hiveStorageFormat == null) {
            return false;
        }
        if (hiveStorageFormat != HiveStorageFormat.ORC && hiveStorageFormat != HiveStorageFormat.PARQUET
                && hiveStorageFormat != HiveStorageFormat.CSV && hiveStorageFormat != HiveStorageFormat.TEXTFILE) {
            return false;
        }
        return true;
    }

    public static boolean checkTableCanOffload(TableScanNode tableScanNode, ConnectorTableMetadata metadata)
    {
        // if there are filter conditions added by other optimizers, then return false
        if (tableScanNode.getPredicate().isPresent() && !tableScanNode.getPredicate().get().equals(TRUE_CONSTANT)) {
            return false;
        }
        if (!tableScanNode.getEnforcedConstraint().isAll() && !tableScanNode.getEnforcedConstraint().isNone()) {
            return false;
        }
        checkArgument(tableScanNode.getTable().getConnectorHandle() instanceof HiveTableHandle);
        HiveTableHandle tableHandle = (HiveTableHandle) tableScanNode.getTable().getConnectorHandle();
        if (!tableHandle.getPredicateColumns().isEmpty() || tableHandle.getDisjunctCompactEffectivePredicate().isPresent()) {
            return false;
        }
        if (!tableHandle.getEnforcedConstraint().isNone() && !tableHandle.getEnforcedConstraint().isAll()) {
            return false;
        }
        if (!tableHandle.getCompactEffectivePredicate().isNone() && !tableHandle.getCompactEffectivePredicate().isAll()) {
            return false;
        }

        if (true != checkStorageFormat(metadata)) {
            return false;
        }

        return true;
    }
}
