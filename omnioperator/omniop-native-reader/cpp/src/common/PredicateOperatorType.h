/**
* Copyright (C) 2025-2025. Huawei Technologies Co., Ltd. All rights reserved.
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

#ifndef PREDICATEOPERATORTYPE_H
#define PREDICATEOPERATORTYPE_H

namespace common {
    enum PredicateOperatorType {
        TRUE = 0,
        EQUAL_TO = 1,
        GREATER_THAN = 2,
        GREATER_THAN_OR_EQUAL = 3,
        LESS_THAN = 4,
        LESS_THAN_OR_EQUAL = 5,
        IS_NOT_NULL = 6,
        IS_NULL = 7,
        OR = 8,
        AND = 9,
        NOT = 10,
        FALSE = 11
    };
}

#endif //PREDICATEOPERATORTYPE_H
