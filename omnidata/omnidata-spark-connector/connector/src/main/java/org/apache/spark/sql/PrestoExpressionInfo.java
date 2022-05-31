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

import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;

import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * PrestoExpressionInfo
 *
 * @date 2021/6/18 15:49
 */
public class PrestoExpressionInfo {

    private int projectionId;

    private Type returnType;

    private Type fieldDataType;

    private Expression childExpression;

    private RowExpression prestoRowExpression;

    private boolean isUDF = false;

    public boolean isUDF() {
        return isUDF;
    }

    public void setUDF(boolean UDF) {
        isUDF = UDF;
    }

    public PrestoExpressionInfo() {
    }

    public int getProjectionId() {
        return projectionId;
    }

    public void setProjectionId(int projectionId) {
        this.projectionId = projectionId;
    }

    public RowExpression getPrestoRowExpression() {
        return prestoRowExpression;
    }

    public void setPrestoRowExpression(RowExpression prestoRowExpression) {
        this.prestoRowExpression = prestoRowExpression;
    }

    public Type getReturnType() {
        return returnType;
    }

    public void setReturnType(Type returnType) {
        this.returnType = returnType;
    }

    public Type getFieldDataType() {
        return fieldDataType;
    }

    public void setFieldDataType(Type fieldDataType) {
        this.fieldDataType = fieldDataType;
    }

    public Expression getChildExpression() {
        return childExpression;
    }

    public void setChildExpression(Expression childExpression) {
        this.childExpression = childExpression;
    }
}
