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
package io.prestosql.plugin.hive.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.huawei.boostkit.omnidata.model.AggregationInfo;
import com.huawei.boostkit.omnidata.model.Column;
import com.huawei.boostkit.omnidata.model.Predicate;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveOffloadExpression;
import io.prestosql.plugin.hive.HivePartitionKey;
import io.prestosql.plugin.hive.HiveUtil;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.relation.VariableToChannelTranslator;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.prestosql.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.DUMMY_OFFLOADED;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.prestosql.plugin.hive.HiveColumnHandle.DUMMY_OFFLOADED_COLUMN_INDEX;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class PageSourceUtil
{
    private PageSourceUtil() {}

    public static List<RowExpression> buildColumnsProjections(HiveOffloadExpression expression,
                                                              List<HiveColumnHandle> columns,
                                                              Map<VariableReferenceExpression, Integer> layoutMap,
                                                              Map<VariableReferenceExpression, Integer> projectionsLayout)
    {
        checkArgument(projectionsLayout.isEmpty(), "buildColumnsProjections: input reference error.");
        int channel = 0;
        ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();
        if (!expression.getProjections().isEmpty()) {
            for (Map.Entry<Symbol, RowExpression> entry : expression.getProjections().entrySet()) {
                RowExpression rowExpression = VariableToChannelTranslator.translate(entry.getValue(), layoutMap);
                builder.add(rowExpression);
                projectionsLayout.put(new VariableReferenceExpression(entry.getKey().getName(), rowExpression.getType()), channel++);
            }
            return builder.build();
        }

        Map<String, Integer> nameMap = layoutMap.entrySet().stream().collect(toMap(key -> key.getKey().getName(), val -> val.getValue()));
        Map<String, Type> typeMap = layoutMap.entrySet().stream().collect(
                toMap(key -> key.getKey().getName(), val -> val.getKey().getType()));
        Set<String> columnSet = new HashSet<>();
        for (HiveColumnHandle columnHandle : columns) {
            if (columnHandle.getHiveColumnIndex() == DUMMY_OFFLOADED_COLUMN_INDEX) {
                continue;
            }
            if (columnSet.add(columnHandle.getName())) {
                Type type = typeMap.get(columnHandle.getName());
                InputReferenceExpression inputReferenceExpression =
                        new InputReferenceExpression(nameMap.get(columnHandle.getName()), type);
                projectionsLayout.put(new VariableReferenceExpression(columnHandle.getName(), type), channel++);
                builder.add(inputReferenceExpression);
            }
        }
        return builder.build();
    }

    public static List<HiveColumnHandle> combineDatasourceColumns(List<HiveColumnHandle> columns, HiveOffloadExpression expression)
    {
        // May contain duplicate columns
        Set<String> nameSet = new HashSet<>();
        ImmutableList.Builder<HiveColumnHandle> builder = new ImmutableList.Builder<>();
        for (HiveColumnHandle columnHandle : columns) {
            if (columnHandle.getColumnType() == DUMMY_OFFLOADED) {
                continue;
            }
            if (nameSet.add(columnHandle.getName())) {
                builder.add(columnHandle);
            }
        }

        for (HiveColumnHandle offload : expression.getOffloadColumns()) {
            if (offload.getColumnType() == DUMMY_OFFLOADED) {
                continue;
            }
            if (nameSet.add(offload.getName())) {
                builder.add(offload);
            }
        }
        return builder.build();
    }

    public static Map<VariableReferenceExpression, Integer> getColumnsLayout(List<HiveColumnHandle> columnHandles, TypeManager typeManager)
    {
        ImmutableMap.Builder<VariableReferenceExpression, Integer> builder = new ImmutableMap.Builder<>();
        for (int channel = 0; channel < columnHandles.size(); channel++) {
            HiveColumnHandle columnHandle = columnHandles.get(channel);
            String name = columnHandle.getName();
            Type type = typeManager.getType(columnHandle.getTypeSignature());
            builder.put(new VariableReferenceExpression(name, type), channel);
        }
        return builder.build();
    }

    public static Optional<AggregationInfo> translateAggregationInfo(Optional<AggregationInfo> aggregationInfo, Map<VariableReferenceExpression, Integer> layOut)
    {
        if (!aggregationInfo.isPresent()) {
            return aggregationInfo;
        }

        ImmutableMap.Builder<String, AggregationInfo.AggregateFunction> functionBuilder = new ImmutableMap.Builder<>();
        for (Map.Entry<String, AggregationInfo.AggregateFunction> entry : aggregationInfo.get().getAggregations().entrySet()) {
            RowExpression callExpression = VariableToChannelTranslator.translate(entry.getValue().getCall(), layOut);
            checkArgument(callExpression instanceof CallExpression);
            AggregationInfo.AggregateFunction aggregateFunction = new AggregationInfo.AggregateFunction((CallExpression) callExpression, entry.getValue().isDistinct());
            functionBuilder.put(entry.getKey(), aggregateFunction);
        }
        ImmutableList.Builder<RowExpression> referenceBuilder = new ImmutableList.Builder<>();
        for (RowExpression variable : aggregationInfo.get().getGroupingKeys()) {
            RowExpression inputReference = VariableToChannelTranslator.translate(variable, layOut);
            referenceBuilder.add(inputReference);
        }
        return Optional.of(new AggregationInfo(functionBuilder.build(), referenceBuilder.build()));
    }

    private static Column buildColumn(HiveColumnHandle columnHandle,
                                      TypeManager typeManager,
                                      List<HivePartitionKey> partitionKeys,
                                      OptionalInt bucketNumber,
                                      Path path)
    {
        Type columnType = typeManager.getType(columnHandle.getTypeSignature());
        if (!columnHandle.getColumnType().equals(PARTITION_KEY)) {
            return new Column(columnHandle.getHiveColumnIndex(), columnHandle.getColumnName(), columnType);
        }

        Map<String, HivePartitionKey> partitionKeysMap = uniqueIndex(partitionKeys, HivePartitionKey::getName);
        String prefilledValue = HiveUtil.getPrefilledColumnValue(columnHandle, partitionKeysMap.get(columnHandle.getName()), path, bucketNumber);
        Object columnValue = HiveUtil.typedPartitionKey(prefilledValue, columnType, prefilledValue);
        return new Column(columnHandle.getHiveColumnIndex(), columnHandle.getColumnName(), columnType, true, columnValue);
    }

    public static Predicate buildPushdownContext(List<HiveColumnHandle> columns,
                                                 HiveOffloadExpression expression,
                                                 TypeManager typeManager,
                                                 TupleDomain<HiveColumnHandle> effectivePredicate,
                                                 List<HivePartitionKey> partitionKeys,
                                                 OptionalInt bucketNumber,
                                                 Path path)
    {
        // Translate variable reference to input reference because PageFunctionCompiler can only support input reference.
        List<HiveColumnHandle> datasourceColumns = combineDatasourceColumns(columns, expression);
        Map<VariableReferenceExpression, Integer> datasoureLayout = getColumnsLayout(datasourceColumns, typeManager);
        Optional<RowExpression> filter = TRUE_CONSTANT.equals(expression.getFilterExpression())
                ? Optional.empty() : Optional.of(VariableToChannelTranslator.translate(expression.getFilterExpression(), datasoureLayout));
        Map<VariableReferenceExpression, Integer> projectionsLayout = new HashMap<>();
        List<RowExpression> filterProjections = buildColumnsProjections(expression, columns, datasoureLayout, projectionsLayout);
        List<Type> types = filterProjections.stream().map(RowExpression::getType).collect(Collectors.toList());
        Optional<AggregationInfo> aggregationInfo = translateAggregationInfo(expression.getAggregations(), projectionsLayout);
        List<Column> pushDownColumns = new ArrayList<>();
        datasourceColumns.forEach(
                column -> {
                    pushDownColumns.add(buildColumn(
                            column,
                            typeManager,
                            partitionKeys,
                            bucketNumber,
                            path));
                });

        Map<String, Domain> domains = effectivePredicate.getDomains().get().entrySet()
                .stream().collect(toMap(e -> e.getKey().getName(), Map.Entry::getValue));

        return new Predicate(
                types,
                pushDownColumns,
                filter,
                filterProjections,
                domains,
                ImmutableMap.of(),
                aggregationInfo,
                expression.getLimit());
    }

    public static void closeWithSuppression(ConnectorPageSource pageSource, Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            pageSource.close();
        }
        catch (RuntimeException | IOException e) {
            // Self-suppression not permitted
            if (throwable != e) {
                throwable.addSuppressed(e);
            }
        }
    }
}
