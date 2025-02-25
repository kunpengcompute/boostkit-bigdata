/*
 * Copyright (C) 2025-2025. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.spark.predicate;

public class NotPredicateCondition extends PredicateCondition {

    PredicateCondition child;

    public NotPredicateCondition(PredicateCondition child) {
        super(PredicateOperatorType.NOT);
        this.child = child;
    }

    @Override
    protected PredicateCondition reduce(boolean upExistsNotOp) {
        PredicateCondition childReduce = child.reduce(true);
        if (childReduce == LeafPredicateCondition.TRUE_PREDICATE_CONDITION) {
            return LeafPredicateCondition.TRUE_PREDICATE_CONDITION;
        }
        return new NotPredicateCondition(childReduce);
    }

    @Override
    public String toString() {
        return String.format("{\"op\":%d,\"child\":%s}", op.ordinal(), child.toString());
    }
}
