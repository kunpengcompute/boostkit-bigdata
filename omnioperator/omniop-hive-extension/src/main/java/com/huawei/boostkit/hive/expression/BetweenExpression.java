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

import javax.annotation.Nullable;

public class BetweenExpression extends BaseExpression {
    @Nullable
    private BaseExpression lower_bound;
    @Nullable
    private BaseExpression upper_bound;
    @Nullable
    private BaseExpression value;

    public BetweenExpression(String exprType, Integer returnType, String operator) {
        super(exprType, returnType, operator);
    }

    public BetweenExpression(String exprType, Integer returnType, String operator, BaseExpression lower_bound,
                             BaseExpression upper_bound, BaseExpression value) {
        super(exprType, returnType, operator);
        this.lower_bound = lower_bound;
        this.upper_bound = upper_bound;
        this.value = value;
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    @Override
    public void add(BaseExpression node) {

    }

    @Override
    public boolean isFull() {
        return lower_bound != null && lower_bound.isFull() && upper_bound != null && upper_bound.isFull()
                && value != null && value.isFull();
    }

    @Override
    public void setLocated(Located located) {

    }

    @Override
    public Located getLocated() {
        return null;
    }
}
