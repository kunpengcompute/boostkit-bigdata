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

import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

public class Predicate {
    public Predicate(List<Type> types, List<Column> columns, Optional<RowExpression> filter, List<RowExpression> projections,
                     Map<String, Domain> domainMap, Map<String, byte[]> bloomFilters, Optional<AggregationInfo> aggregations,
                     OptionalLong limit) {
    }

}

