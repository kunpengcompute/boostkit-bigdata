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

import java.util.ArrayList;
import java.util.List;

public class UnaryExpression extends BaseExpression {

    private List<BaseExpression> arguments;

    private final transient int size;

    public UnaryExpression(String exprType, Integer returnType, int size) {
        super(exprType, returnType, null);
        this.size = size;
        this.arguments = new ArrayList<>(size);
    }

    public UnaryExpression(String exprType, Integer returnType, int size, String operator) {
        super(exprType, returnType, operator);
        this.size = size;
        this.arguments = new ArrayList<>(size);
    }

    @Override
    public void add(BaseExpression node) {
        if (isFull()) {
            return;
        }
        arguments.add(node);
    }

    protected List<BaseExpression> getArguments() {
        return arguments;
    }

    @Override
    public boolean isFull() {
        return arguments.size() == size;
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
}