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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.huawei.boostkit.omnidata.expression.OmniExpressionChecker;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveOffloadExpression;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.HiveTypeTranslator;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableBiMap.toImmutableBiMap;
import static io.prestosql.expressions.RowExpressionNodeInliner.replaceExpression;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.DUMMY_OFFLOADED;
import static io.prestosql.plugin.hive.HiveColumnHandle.DUMMY_OFFLOADED_COLUMN_INDEX;
import static io.prestosql.plugin.hive.rule.HivePushdownUtil.getDataSourceColumns;

public class HiveProjectPushdown
{
    private HiveProjectPushdown() {}

    public static Optional<TableScanNode> tryProjectPushdown(ProjectNode plan, Map<String, Type> types)
    {
        if (!(plan.getSource() instanceof TableScanNode)) {
            return Optional.empty();
        }
        TableScanNode tableScanNode = (TableScanNode) plan.getSource();
        ConnectorTableHandle tableHandle = tableScanNode.getTable().getConnectorHandle();
        if (!(tableHandle instanceof HiveTableHandle) ||
                !(((HiveTableHandle) tableHandle).getOffloadExpression().getProjections().isEmpty())) {
            return Optional.empty();
        }

        Map<Symbol, ColumnHandle> assignments = new HashMap<>();
        HiveTypeTranslator hiveTypeTranslator = new HiveTypeTranslator();
        for (Map.Entry<Symbol, RowExpression> entry : plan.getAssignments().entrySet()) {
            String name = entry.getKey().getName();
            HiveType hiveType = HiveType.toHiveType(hiveTypeTranslator, entry.getValue().getType());
            TypeSignature typeSignature = entry.getValue().getType().getTypeSignature();
            HiveColumnHandle columnHandle = new HiveColumnHandle(name, hiveType, typeSignature,
                    DUMMY_OFFLOADED_COLUMN_INDEX, DUMMY_OFFLOADED, Optional.of("projections pushed down " + name));
            assignments.put(entry.getKey(), columnHandle);
        }

        BiMap<VariableReferenceExpression, VariableReferenceExpression> variableToColumnMapping =
                tableScanNode.getAssignments().entrySet().stream().collect(toImmutableBiMap(
                        entry -> new VariableReferenceExpression(entry.getKey().getName(), types.get(entry.getKey().getName())),
                        entry -> new VariableReferenceExpression(entry.getValue().getColumnName(), types.get(entry.getKey().getName()))));
        ImmutableMap.Builder<Symbol, RowExpression> projections = new ImmutableMap.Builder<>();
        for (Map.Entry<Symbol, RowExpression> entry : plan.getAssignments().getMap().entrySet()) {
            RowExpression expression = replaceExpression(entry.getValue(), variableToColumnMapping);
            if (!OmniExpressionChecker.checkExpression(expression)) {
                return Optional.empty();
            }
            projections.put(entry.getKey(), expression);
        }

        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        HiveOffloadExpression offloadExpression = hiveTableHandle.getOffloadExpression().updateProjections(projections.build(), getDataSourceColumns(tableScanNode));
        HiveTableHandle newHiveTableHandle = hiveTableHandle.withOffloadExpression(offloadExpression);
        TableHandle newTableHandle = new TableHandle(
                tableScanNode.getTable().getCatalogName(),
                newHiveTableHandle,
                tableScanNode.getTable().getTransaction(),
                tableScanNode.getTable().getLayout());
        return Optional.of(new TableScanNode(tableScanNode.getId(), newTableHandle, ImmutableList.copyOf(assignments.keySet()), assignments,
                tableScanNode.getEnforcedConstraint(), tableScanNode.getPredicate(), tableScanNode.getStrategy(), tableScanNode.getReuseTableScanMappingId(),
                tableScanNode.getConsumerTableScanNodeCount(), tableScanNode.isForDelete()));
    }
}
