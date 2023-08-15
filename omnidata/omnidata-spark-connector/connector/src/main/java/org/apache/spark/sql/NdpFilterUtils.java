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

import static io.prestosql.spi.relation.SpecialForm.Form.IN;
import static io.prestosql.spi.relation.SpecialForm.Form.IS_NULL;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;

import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;

import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * NdpFilterUtils
 */
public class NdpFilterUtils {

    public static int getFilterProjectionId(Expression expression, Map<String, Integer> fieldMap) {
        String filterColumnName = expression.toString().split("#")[0].toLowerCase(Locale.ENGLISH);
        if (fieldMap.containsKey(filterColumnName)) {
            return fieldMap.get(filterColumnName);
        } else {
            return fieldMap.size();
        }
    }

    public static  RowExpression generateRowExpression(
            String operatorName,
            List<RowExpression> arguments,
            TypeSignature typeSignature) {
        RowExpression rowExpression;
        switch (operatorName.toLowerCase(Locale.ENGLISH)) {
            case "is_null":
                rowExpression = new SpecialForm(IS_NULL, BOOLEAN, arguments);
                break;
            case "in":
                rowExpression = new SpecialForm(IN, BOOLEAN, arguments);
                break;
            case "not":
                Signature notSignature = new Signature(QualifiedObjectName.valueOfDefaultFunction("not"),
                        FunctionKind.SCALAR, new TypeSignature("boolean"), typeSignature);
                rowExpression =  new CallExpression(operatorName.toLowerCase(Locale.ENGLISH),
                        new BuiltInFunctionHandle(notSignature), BOOLEAN, arguments);
                break;
            case "isempty":
            case "isdeviceidlegal":
            case "ismessycode":
            case "dateudf":
                rowExpression = arguments.get(0);
                break;
            default:
                Signature signature = new Signature(
                        QualifiedObjectName.valueOfDefaultFunction("$operator$" +
                                operatorName.toLowerCase(Locale.ENGLISH)),
                        FunctionKind.SCALAR, new TypeSignature("boolean"), typeSignature, typeSignature);
                // To adapt to the omniOperator, use uppercase operatorName
                rowExpression = new CallExpression(operatorName,
                        new BuiltInFunctionHandle(signature), BOOLEAN, arguments);
                break;
        }
        return rowExpression;
    }

    /**
     * create RowExpression for column
     *
     * @param columnInfo column info
     * @return RowExpression produced by column info
     */
    public static RowExpression createRowExpressionForColumn(ColumnInfo columnInfo) {
        if (columnInfo.getExpressionInfo().getReturnType() != null) {
            return columnInfo.getExpressionInfo().getPrestoRowExpression();
        } else {
            return new InputReferenceExpression(columnInfo.getFilterProjectionId(), columnInfo.getPrestoType());
        }
    }
}
