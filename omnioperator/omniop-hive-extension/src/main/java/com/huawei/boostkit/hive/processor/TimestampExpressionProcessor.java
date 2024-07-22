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

package com.huawei.boostkit.hive.processor;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.CastFunctionExpression;
import com.huawei.boostkit.hive.expression.DivideExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.LiteralFactor;
import com.huawei.boostkit.hive.expression.TypeUtils;

import nova.hetu.omniruntime.type.DoubleDataType;
import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.type.Decimal64DataType;
import nova.hetu.omniruntime.type.LongDataType;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class TimestampExpressionProcessor implements ExpressionProcessor {
    @Override
    public BaseExpression process(ExprNodeGenericFuncDesc node, String operator, ObjectInspector inspector) {
        ExprNodeDesc exprNodeDesc = node.getChildren().get(0);
        BaseExpression baseExpression;
        int dataType = TypeUtils.convertHiveTypeToOmniType(exprNodeDesc.getTypeInfo());
        if (exprNodeDesc instanceof ExprNodeGenericFuncDesc) {
            baseExpression = ExpressionUtils.build((ExprNodeGenericFuncDesc) exprNodeDesc, inspector);
        } else {
                baseExpression = ExpressionUtils.createNode(exprNodeDesc, inspector);
        }
        LiteralFactor<Long> longLiteralFactorDay = new LiteralFactor<>("LITERAL", null, null,
                86400000L, null, LongDataType.LONG.getId().toValue());
        DivideExpression divideExpression = new DivideExpression(LongDataType.LONG.getId().toValue(),
                "MULTIPLY", null, null);
        if (dataType == Decimal128DataType.DECIMAL128.getId().toValue()
                || dataType == DoubleDataType.DOUBLE.getId().toValue()
                || dataType == Decimal64DataType.DECIMAL64.getId().toValue()) {
            DivideExpression secondExpression = new DivideExpression(dataType, "MULTIPLY", null, null);
            secondExpression.add(baseExpression);
            LiteralFactor<Long> longLiteralFactorSecond = new LiteralFactor<>("LITERAL", null, null,
                    1L, null, LongDataType.LONG.getId().toValue());
            LiteralFactor<Double> doubleLiteralFactorSecond = new LiteralFactor<>("LITERAL", null, null,
                    1000.0, null, DoubleDataType.DOUBLE.getId().toValue());
            secondExpression.add(doubleLiteralFactorSecond);
            CastFunctionExpression cast = new CastFunctionExpression(LongDataType.LONG.getId().toValue(),
                    null, null, null);
            cast.add(secondExpression);
            divideExpression.add(cast);
            divideExpression.add(longLiteralFactorSecond);
            return divideExpression;
        } else if (dataType != LongDataType.LONG.getId().toValue()) {
            CastFunctionExpression cast = new CastFunctionExpression(LongDataType.LONG.getId().toValue(),
                        null, null, null);
            cast.add(baseExpression);
            divideExpression.add(cast);
            divideExpression.add(longLiteralFactorDay);
            return divideExpression;
        }
        divideExpression.add(baseExpression);
        divideExpression.add(longLiteralFactorDay);
        return divideExpression;
    }
}
