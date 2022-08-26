/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.omnidata.model;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * 1. DataIoServer read the data from data source, output {@link io.prestosql.spi.Page}
 * 2. Driver will use {@link Predicate} to process {@link io.prestosql.spi.Page}
 *
 * @since 2021-07-31
 */
public class Predicate {
    /**
     * types of return columns that after filter and projection
     */
    private final List<Type> types;

    /**
     * columns that need to be read from data source.
     */
    private List<Column> columns;

    /**
     * filter expression json, it from {@link RowExpression}, describe how to filter data
     */
    private Optional<RowExpression> filter;

    /**
     * projection expression, it from {@link RowExpression}, describe how to projection data
     */
    private List<RowExpression> projections;

    /**
     * aggregations expression, it from {@link RowExpression}.
     * Input:
     *      1.Should translate VariableReferenceExpression to InputReferenceExpression reference
     * Ouput:
     *      1.aggregations output columns and order by columns
     */
    private Optional<AggregationInfo> aggregations;

    /**
     * limit information, use for LimitOperator.
     */
    private OptionalLong limit;

    /**
     * tuple domains, contains the real data that use to filter data.
     */
    private Map<String, Domain> domains;

    /**
     * bloom filters, contains the bitmap use to filter data.
     */
    private Map<String, byte[]> bloomFilters;

    @JsonCreator
    public Predicate(
            @JsonProperty("types") List<Type> types,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("filter") Optional<RowExpression> filter,
            @JsonProperty("projections") List<RowExpression> projections,
            @JsonProperty("domains") Map<String, Domain> domains,
            @JsonProperty("bloomFilters") Map<String, byte[]> bloomFilters,
            @JsonProperty("aggregations") Optional<AggregationInfo> aggregations,
            @JsonProperty("limit") OptionalLong limit) {
        this.types = requireNonNull(types);
        this.columns = requireNonNull(columns);
        this.filter = requireNonNull(filter);
        this.projections = requireNonNull(projections);
        this.domains = requireNonNull(domains);
        this.bloomFilters = requireNonNull(bloomFilters);
        this.aggregations = requireNonNull(aggregations);
        this.limit = requireNonNull(limit);
    }

    @JsonProperty
    public List<Type> getTypes() {
        return types;
    }

    @JsonProperty
    public List<Column> getColumns() {
        return columns;
    }

    @JsonProperty
    public Optional<RowExpression> getFilter() {
        return filter;
    }

    @JsonProperty
    public List<RowExpression> getProjections() {
        return projections;
    }

    @JsonProperty
    public Map<String, Domain> getDomains() {
        return domains;
    }

    @JsonProperty
    public Map<String, byte[]> getBloomFilters() {
        return bloomFilters;
    }

    @JsonProperty
    public Optional<AggregationInfo> getAggregations() {
        return aggregations;
    }

    @JsonProperty
    public OptionalLong getLimit() {
        return limit;
    }
}