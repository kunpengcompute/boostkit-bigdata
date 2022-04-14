/*
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
package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.huawei.boostkit.omnidata.model.AggregationInfo;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.expressions.LogicalRowExpressions.TRUE_CONSTANT;

public class HiveOffloadExpression
{
    private final Set<HiveColumnHandle> offloadColumns;
    private final RowExpression filterExpression; // The default value is TRUE_CONSTANT, indicating that no operator is pushed down.
    private final Optional<AggregationInfo> aggregations;
    private final OptionalLong limit;
    private final Map<Symbol, RowExpression> projections;

    public HiveOffloadExpression()
    {
        this(Collections.emptySet(), TRUE_CONSTANT, Optional.empty(), OptionalLong.empty(), Collections.emptyMap());
    }

    @JsonCreator
    public HiveOffloadExpression(
            @JsonProperty("offloadColumns") Set<HiveColumnHandle> offloadColumns,
            @JsonProperty("filterExpression") RowExpression filterExpression,
            @JsonProperty("aggregations") Optional<AggregationInfo> aggregations,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("projections") Map<Symbol, RowExpression> projections)
    {
        this.offloadColumns = offloadColumns;
        this.filterExpression = filterExpression;
        this.aggregations = aggregations;
        this.limit = limit;
        this.projections = projections;
    }

    @JsonProperty
    public Set<HiveColumnHandle> getOffloadColumns()
    {
        return offloadColumns;
    }

    @JsonProperty
    public RowExpression getFilterExpression()
    {
        return filterExpression;
    }

    @JsonProperty
    public Optional<AggregationInfo> getAggregations()
    {
        return aggregations;
    }

    @JsonProperty
    public Map<Symbol, RowExpression> getProjections()
    {
        return projections;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    public HiveOffloadExpression updateFilter(RowExpression filterExpression, Set<HiveColumnHandle> offloadColumns)
    {
        /// todo : handle exception
        checkArgument(!aggregations.isPresent() && !limit.isPresent() && projections.isEmpty(),
                "Aggregations, limit or projection expression is not empty.");
        Set<HiveColumnHandle> newOffloadColumns = new HashSet<>(this.offloadColumns);
        newOffloadColumns.addAll(offloadColumns);
        return new HiveOffloadExpression(newOffloadColumns, filterExpression, aggregations, limit, projections);
    }

    public HiveOffloadExpression updateAggregation(Optional<AggregationInfo> aggregations, Set<HiveColumnHandle> offloadColumns)
    {
        checkArgument(!limit.isPresent() && !this.aggregations.isPresent(),
                "Limit or aggregations expression is not empty.");
        Set<HiveColumnHandle> newOffloadColumns = new HashSet<>(this.offloadColumns);
        newOffloadColumns.addAll(offloadColumns);
        return new HiveOffloadExpression(newOffloadColumns, filterExpression, aggregations, limit, projections);
    }

    public HiveOffloadExpression updateLimit(OptionalLong limit)
    {
        return new HiveOffloadExpression(offloadColumns, filterExpression, aggregations, limit, projections);
    }

    public HiveOffloadExpression updateProjections(Map<Symbol, RowExpression> projections, Set<HiveColumnHandle> offloadColumns)
    {
        checkArgument(this.projections.isEmpty() && !aggregations.isPresent(),
                "Projections or aggregations expression is not empty.");
        Set<HiveColumnHandle> newOffloadColumns = new HashSet<>(this.offloadColumns);
        newOffloadColumns.addAll(offloadColumns);
        return new HiveOffloadExpression(newOffloadColumns, filterExpression, aggregations, limit, projections);
    }

    public boolean isPresent()
    {
        return !TRUE_CONSTANT.equals(filterExpression) || aggregations.isPresent() || limit.isPresent() || !projections.isEmpty();
    }

    public static String aggregationInfoToString(AggregationInfo aggregationInfo)
    {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, AggregationInfo.AggregateFunction> entry : aggregationInfo.getAggregations().entrySet()) {
            CallExpression call = entry.getValue().getCall();
            String argument = call.getArguments().isEmpty() ? "*" : call.getArguments().get(0).toString();
            builder.append(call.getDisplayName()).append("(").append(argument).append(") ");
        }

        if (aggregationInfo.getGroupingKeys().isEmpty()) {
            return builder.toString();
        }

        builder.append("group by:");
        for (RowExpression variable : aggregationInfo.getGroupingKeys()) {
            builder.append(variable.toString()).append(" ");
        }
        return builder.toString();
    }

    @Override
    public String toString()
    {
        if (!isPresent()) {
            return "";
        }

        StringBuilder builder = new StringBuilder();
        builder.append(" offload={");
        if (!TRUE_CONSTANT.equals(filterExpression)) {
            builder.append(" filter=[").append(filterExpression.toString()).append("]");
        }
        if (!projections.isEmpty()) {
            builder.append(" projections=[");
            for (Map.Entry<Symbol, RowExpression> entry : projections.entrySet()) {
                builder.append(entry.getKey().getName()).append(":").append(entry.getValue().toString()).append(" ");
            }
            builder.append("]");
        }
        aggregations.ifPresent(expression ->
                builder.append(" aggregation=[").append(aggregationInfoToString(expression)).append("]"));
        limit.ifPresent(expression ->
                builder.append(" limit=[").append(expression).append("]"));
        builder.append("} ");
        return builder.toString();
    }
}
