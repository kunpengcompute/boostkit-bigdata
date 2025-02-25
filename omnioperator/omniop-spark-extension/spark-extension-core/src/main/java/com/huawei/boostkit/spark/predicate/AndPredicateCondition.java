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

public class AndPredicateCondition extends BinaryPredicateCondition {

    public AndPredicateCondition(PredicateCondition left, PredicateCondition right) {
        super(PredicateOperatorType.AND, left, right);
    }

    @Override
    protected PredicateCondition reduce(boolean upExistsNotOp) {
        PredicateCondition leftReduce = left.reduce(upExistsNotOp);
        PredicateCondition rightReduce = right.reduce(upExistsNotOp);
        if (leftReduce == LeafPredicateCondition.TRUE_PREDICATE_CONDITION &&
                rightReduce == LeafPredicateCondition.TRUE_PREDICATE_CONDITION) {
            return LeafPredicateCondition.TRUE_PREDICATE_CONDITION;
        }
        if (upExistsNotOp && (leftReduce == LeafPredicateCondition.TRUE_PREDICATE_CONDITION ||
                rightReduce == LeafPredicateCondition.TRUE_PREDICATE_CONDITION)) {
            return LeafPredicateCondition.TRUE_PREDICATE_CONDITION;
        }
        if (leftReduce == LeafPredicateCondition.TRUE_PREDICATE_CONDITION) {
            return rightReduce;
        }
        if (rightReduce == LeafPredicateCondition.TRUE_PREDICATE_CONDITION) {
            return leftReduce;
        }
        return new AndPredicateCondition(leftReduce, rightReduce);
    }
}
