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

public class LiteralFactor<T> extends BaseExpression {
    private Boolean isNull;

    private T value;

    @Nullable
    private Integer width;

    private Integer dataType;

    public LiteralFactor(String exprType, Integer returnType, String operator,
                         T value, @Nullable Integer width, Integer dataType) {
        super(exprType, returnType, operator);
        this.isNull = value == null;
        this.value = value;
        this.width = width;
        this.dataType = dataType;
    }

    @Override
    public void add(BaseExpression node) {

    }

    @Override
    public Integer getReturnType() {
        return dataType;
    }

    @Override
    public boolean isFull() {
        return true;
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

    public void setDataType(Integer dataType) {
        this.dataType= dataType;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }
}
