/*
 * Copyright (C) 2023-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.hive.expression;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

public class ConditionExpression extends BaseExpression {
    private Integer width;
    private Integer precision;
    private Integer scale;
    private BaseExpression condition;
    @SerializedName("if_true")
    private BaseExpression ifTrue;
    @SerializedName("if_false")
    private BaseExpression ifFalse;

    public ConditionExpression(Integer returnType, Integer width, Integer precision, Integer scale) {
        super("IF", returnType, null);
        this.width = width;
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public void add(BaseExpression node) {
        if (condition == null) {
            condition = node;
        } else if (ifTrue == null) {
            ifTrue = node;
        } else if (ifFalse == null) {
            ifFalse = node;
        } else {
            ifFalse.add(node);
        }
    }

    @Override
    public boolean isFull() {
        return condition != null && ifTrue != null && ifFalse != null;
    }

    @Override
    public void setLocated(Located located) {

    }

    @Override
    public Located getLocated() {
        return null;
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public ConditionExpression copyBase() {
        return new ConditionExpression(this.getReturnType(), this.width, this.precision, this.scale);
    }
}
