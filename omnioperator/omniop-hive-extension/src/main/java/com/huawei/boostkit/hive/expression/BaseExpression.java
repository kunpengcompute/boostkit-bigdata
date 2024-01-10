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

public abstract class BaseExpression {
    public enum Located {
        ROOT,
        LEFT,
        RIGHT
    }

    private String exprType;
    private Integer returnType;
    private String operator;
    private transient int level = 0;
    protected transient BaseExpression parent;

    public BaseExpression(String exprType, Integer returnType, String operator) {
        this.exprType = exprType;
        this.returnType = returnType;
        this.operator = operator;
    }

    public String getExprType() {
        return exprType;
    }

    public Integer getReturnType() {
        return returnType;
    }

    public String getOperator() {
        return operator;
    }

    public void setParent(BaseExpression parent) {
        this.parent = parent;
    }

    public BaseExpression getParent() {
        return parent;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public int getLevel() {
        return level;
    }

    public abstract void add(BaseExpression node);

    public abstract boolean isFull();

    public abstract void setLocated(Located located);

    public abstract Located getLocated();
}