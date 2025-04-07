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

import nova.hetu.omniruntime.type.DataType.DataTypeId;

public class LeafPredicateCondition extends PredicateCondition {

    public static final PredicateCondition TRUE_PREDICATE_CONDITION =
            new LeafPredicateCondition(PredicateOperatorType.TRUE, -1,  DataTypeId.OMNI_INT, "-1");

    private int index;

    private DataTypeId dataType;

    private String value;

    public LeafPredicateCondition(PredicateOperatorType op, int index, DataTypeId dataType, String value) {
        super(op);
        this.index = index;
        this.dataType = dataType;
        this.value = value;
    }

    @Override
    protected PredicateCondition reduce(boolean upExistsNotOp) {
        return this;
    }

    @Override
    public String toString() {
        return String.format("{\"op\":%d,\"index\":%d,\"dataType\":%d,\"value\":\"%s\"}",
                op.ordinal(), index, dataType.toValue(), value);
    }
}
