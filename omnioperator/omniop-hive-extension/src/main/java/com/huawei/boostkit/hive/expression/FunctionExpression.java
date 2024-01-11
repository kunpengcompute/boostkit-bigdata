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

import com.google.gson.annotations.SerializedName;

public class FunctionExpression extends UnaryExpression {
    @SerializedName("function_name")
    private String functionName;

    private Integer precision;

    private Integer scale;

    private Integer width;

    public FunctionExpression(Integer returnType, String functionName, int size, Integer width) {
        super("FUNCTION", returnType, size);
        this.functionName = functionName;
        this.width = width;
    }

    public FunctionExpression(Integer returnType, String functionName, int size, Integer width, Integer precision, Integer scale) {
        this(returnType, functionName, size, width);
        this.precision = precision;
        this.scale = scale;
    }

    public Integer getPrecision() {
        return precision;
    }

    public Integer getScale() {
        return scale;
    }
}
