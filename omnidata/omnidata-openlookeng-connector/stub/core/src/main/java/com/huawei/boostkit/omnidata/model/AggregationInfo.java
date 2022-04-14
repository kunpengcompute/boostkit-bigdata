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

import com.google.common.base.Objects;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AggregationInfo {
    private final Map<String, AggregateFunction> aggregations;
    List<RowExpression> groupingKeys;
    public AggregationInfo(Map<String, AggregateFunction> aggregations , List<RowExpression> groupingKeys) {
        this.aggregations = aggregations;
        this.groupingKeys = groupingKeys;
    }

    public Map<String, AggregateFunction> getAggregations() {
        return aggregations;
    }

    public List<RowExpression> getGroupingKeys() {
        return groupingKeys;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof AggregationInfo)) {
            return false;
        }
        AggregationInfo that = (AggregationInfo) object;
        return aggregations.equals(that.aggregations) && groupingKeys.equals(that.groupingKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(aggregations, groupingKeys);
    }

    public static class AggregateFunction {
        private CallExpression callExpression;
        boolean isDistinct;
        public AggregateFunction(CallExpression callExpression, boolean isDistinct) {
            this.callExpression = callExpression;
            this.isDistinct = isDistinct;
        }
        public CallExpression getCall() {
            return callExpression;
        }
        public boolean isDistinct() {
            return isDistinct;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof AggregateFunction)) {
                return false;
            }
            AggregateFunction that = (AggregateFunction) object;
            return callExpression.equals(that.callExpression) && isDistinct == that.isDistinct;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(callExpression, isDistinct);
        }
    }
}
