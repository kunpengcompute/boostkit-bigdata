/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
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

package org.apache.spark.sql;

/**
 * udf enum
 */
public enum NdpUdfEnum {
    // Supported push-down Udf
    SUBSTRING("substr","substr"),
    LENGTH("length","length"),
    UPPER("upper","upper"),
    LOWER("lower","lower"),
    CAST("cast","$operator$cast"),
    REPLACE("replace","replace"),
    INSTR("instr","instr"),
    SUBSCRIPT("SUBSCRIPT","$operator$subscript"),
    SPLIT("split","split"),
    STRINGINSTR("instr","instr");

    private String signatureName;
    private String operatorName;

    NdpUdfEnum(String signatureName, String operatorName) {
        this.signatureName = signatureName;
        this.operatorName = operatorName;
    }

    public String getSignatureName() {
        return signatureName;
    }

    public void setSignatureName(String signatureName) {
        this.signatureName = signatureName;
    }

    public String getOperatorName() {
        return operatorName;
    }

    public void setOperatorName(String operatorName) {
        this.operatorName = operatorName;
    }
}
