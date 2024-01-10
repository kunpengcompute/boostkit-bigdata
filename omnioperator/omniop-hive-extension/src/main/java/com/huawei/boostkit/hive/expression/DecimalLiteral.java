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

public class DecimalLiteral extends LiteralFactor<Object> {
    private Integer precision;
    private Integer scale;

    public DecimalLiteral(Object value, Integer dataType, Integer precision, Integer scale) {
        super("LITERAL", null, null, value, null, dataType);
        this.precision = precision;
        this.scale = scale;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public Integer getScale() {
        return scale;
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
