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

package org.apache.hadoop.hive.ql.omnidata.operator.predicate;

import com.huawei.boostkit.omnidata.model.Predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Ndp Predicate Info
 *
 * @since 2022-01-27
 */
public class NdpPredicateInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private boolean isPushDown = false;

    private boolean isPushDownAgg = false;

    private boolean isPushDownFilter = false;

    private boolean hasPartitionColumn = false;

    private Predicate predicate;

    private List<Integer> outputColumns;

    private List<String> decodeTypes;

    private List<Boolean> decodeTypesWithAgg;

    public NdpPredicateInfo() {
    }

    public NdpPredicateInfo(boolean isPushDown) {
        this.isPushDown = isPushDown;
    }

    @JsonCreator
    public NdpPredicateInfo(@JsonProperty("isPushDown") boolean isPushDown,
        @JsonProperty("isPushDownAgg") boolean isPushDownAgg,
        @JsonProperty("isPushDownFilter") boolean isPushDownFilter,
        @JsonProperty("hasPartitionColumn") boolean hasPartitionColumn, @JsonProperty("predicate") Predicate predicate,
        @JsonProperty("outputColumns") List<Integer> outputColumns,
        @JsonProperty("decodeTypes") List<String> decodeTypes,
        @JsonProperty("decodeTypesWithAgg") List<Boolean> decodeTypesWithAgg) {
        this.isPushDown = isPushDown;
        this.isPushDownAgg = isPushDownAgg;
        this.isPushDownFilter = isPushDownFilter;
        this.hasPartitionColumn = hasPartitionColumn;
        this.predicate = predicate;
        this.outputColumns = outputColumns;
        this.decodeTypes = decodeTypes;
        this.decodeTypesWithAgg = decodeTypesWithAgg;
    }

    @JsonProperty
    public boolean getIsPushDown() {
        return isPushDown;
    }

    @JsonProperty
    public boolean getIsPushDownAgg() {
        return isPushDownAgg;
    }

    @JsonProperty
    public boolean getIsPushDownFilter() {
        return isPushDownFilter;
    }

    @JsonProperty
    public boolean getHasPartitionColumn() {
        return hasPartitionColumn;
    }

    @JsonProperty
    public Predicate getPredicate() {
        return predicate;
    }

    @JsonProperty
    public List<Integer> getOutputColumns() {
        return outputColumns;
    }

    @JsonProperty
    public List<String> getDecodeTypes() {
        return decodeTypes;
    }

    @JsonProperty
    public List<Boolean> getDecodeTypesWithAgg() {
        return decodeTypesWithAgg;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NdpPredicateInfo that = (NdpPredicateInfo) o;
        return isPushDown == that.isPushDown && isPushDownAgg == that.isPushDownAgg
            && isPushDownFilter == that.isPushDownFilter && hasPartitionColumn == that.hasPartitionColumn
            && Objects.equals(predicate, that.predicate) && Objects.equals(outputColumns, that.outputColumns) && Objects
            .equals(decodeTypes, that.decodeTypes) && Objects.equals(decodeTypesWithAgg, that.decodeTypesWithAgg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isPushDown, isPushDownAgg, isPushDownFilter, hasPartitionColumn, predicate, outputColumns,
            decodeTypes, decodeTypesWithAgg);
    }
}
