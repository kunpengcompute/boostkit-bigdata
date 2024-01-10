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

public class CoalesceExpression extends BaseExpression {
    private Integer width;
    private Integer precision;
    private Integer scale;
    private BaseExpression value1;
    private BaseExpression value2;

    public CoalesceExpression(Integer returnType, Integer width, Integer precision, Integer scale) {
        super("COALESCE", returnType, null);
        this.width = width;
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public void add(BaseExpression node) {
        if (value1 == null) {
            value1 = node;
            return;
        }
        if (value2 == null) {
            value2 = node;
        }
    }

    @Override
    public boolean isFull() {
        return value1.isFull() && value2.isFull();
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
