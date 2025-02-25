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

public abstract class BinaryPredicateCondition extends PredicateCondition {

    protected PredicateCondition left;

    protected PredicateCondition right;

    public BinaryPredicateCondition(PredicateOperatorType op, PredicateCondition left, PredicateCondition right) {
        super(op);
        this.left = left;
        this.right = right;
    }

    @Override
    public String toString() {
        return String.format("{\"op\":%d,\"left\":%s,\"right\":%s}",
                op.ordinal(), left.toString(), right.toString());
    }
}
