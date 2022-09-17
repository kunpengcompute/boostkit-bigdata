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
package com.huawei.boostkit.omnidata.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;

import java.util.List;
import java.util.Map;

public class AggregationInfo {
    private final Map<String, AggregateFunction> aggregations;
    List<RowExpression> groupingKeys;
    @JsonCreator
    public AggregationInfo(
            @JsonProperty("aggregations") Map<String, AggregateFunction> aggregations,
            @JsonProperty("groupingKeys") List<RowExpression> groupingKeys) {
        this.aggregations = aggregations;
        this.groupingKeys = groupingKeys;
    }

    @JsonProperty
    public Map<String, AggregateFunction> getAggregations() {
        return aggregations;
    }

    @JsonProperty
    public List<RowExpression> getGroupingKeys() {
        return groupingKeys;
    }

    @Override
    public boolean equals(Object object) {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(aggregations, groupingKeys);
    }

    public static class AggregateFunction {
        private CallExpression callExpression;
        boolean isDistinct;
        @JsonCreator
        public AggregateFunction(
                @JsonProperty("callExpression") CallExpression callExpression,
                @JsonProperty("isDistinct") boolean isDistinct) {
            this.callExpression = callExpression;
            this.isDistinct = isDistinct;
        }
        @JsonProperty
        public CallExpression getCall() {
            return callExpression;
        }
        @JsonProperty
        public boolean isDistinct() {
            return isDistinct;
        }

        @Override
        public boolean equals(Object object) {
            return true;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(callExpression, isDistinct);
        }
    }
}
