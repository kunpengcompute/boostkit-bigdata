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

import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.*;
import org.apache.spark.sql.catalyst.expressions.*;
import scala.collection.JavaConverters;

import org.apache.spark.sql.hive.HiveSimpleUDF;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static io.prestosql.spi.type.VarcharType.createVarcharType;

/**
 * Used to process Spark`s UDF, which is converted to presto.
 *
 * @since 2021-06-24
 */
public class NdpUdfExpressions {

    private void checkAttributeReference(Expression childExpression,
                                         PrestoExpressionInfo prestoExpressionInfo,
                                         Map<String, Integer> fieldMap, Type childType,
                                         List<RowExpression> rowArguments) {
        if ((childExpression instanceof AttributeReference)) {
            int lengthProjectId = NdpFilterUtils.getFilterProjectionId(childExpression, fieldMap);
            rowArguments.add(new InputReferenceExpression(lengthProjectId, childType));
            prestoExpressionInfo.setProjectionId(lengthProjectId);
            prestoExpressionInfo.setFieldDataType(
                    NdpUtils.transOlkDataType(childExpression.dataType(), childExpression, false));
            prestoExpressionInfo.setChildExpression(childExpression);
        } else if (childExpression instanceof Literal) {
            rowArguments.add(NdpUtils.transConstantExpression(((Literal) childExpression).value().toString(),
                    childType));
        } else {
            createNdpUdf(childExpression, prestoExpressionInfo, fieldMap);
            rowArguments.add(prestoExpressionInfo.getPrestoRowExpression());
        }
    }

    /**
     * create Udf
     */
    public void createNdpUdf(Expression udfExpression, PrestoExpressionInfo prestoExpressionInfo,
                             Map<String, Integer> fieldMap) {
        if (udfExpression instanceof Length) {
            createNdpLength((Length) udfExpression, prestoExpressionInfo, fieldMap);
        } else if (udfExpression instanceof Upper) {
            createNdpUpper((Upper) udfExpression, prestoExpressionInfo, fieldMap);
        } else if (udfExpression instanceof Lower) {
            createNdpLower((Lower) udfExpression, prestoExpressionInfo, fieldMap);
        } else if (udfExpression instanceof Cast) {
            createNdpCast((Cast) udfExpression, prestoExpressionInfo, fieldMap);
        } else if (udfExpression instanceof Substring) {
            createNdpSubstring((Substring) udfExpression, prestoExpressionInfo, fieldMap);
        } else if (udfExpression instanceof StringReplace) {
            createNdpReplace((StringReplace) udfExpression, prestoExpressionInfo, fieldMap);
        } else if (udfExpression instanceof StringInstr) {
            createNdpInstr((StringInstr) udfExpression, prestoExpressionInfo, fieldMap);
        } else if (udfExpression instanceof StringSplit) {
            createNdpSplit((StringSplit) udfExpression, prestoExpressionInfo, fieldMap);
        } else if (udfExpression instanceof GetArrayItem) {
            createNdpSubscript((GetArrayItem) udfExpression, prestoExpressionInfo, fieldMap);
        } else if (udfExpression instanceof HiveSimpleUDF) {
            createHiveSimpleUdf(udfExpression, prestoExpressionInfo, fieldMap);
        } else {
            throw new RuntimeException("unsupported this UDF:" + udfExpression.toString());
        }
    }

    /**
     * Used to create UDF with only a single parameter
     */
    private void createNdpSingleParameter(NdpUdfEnum udfEnum,
                                          Expression expression, Expression childExpression,
                                          PrestoExpressionInfo prestoExpressionInfo,
                                          Map<String, Integer> fieldMap) {
        String signatureName = udfEnum.getSignatureName();
        Type childType = NdpUtils.transOlkDataType(childExpression.dataType(), childExpression, true);
        if (childType instanceof CharType) {
            childType = createVarcharType(((CharType) childType).getLength());
        }
        Type returnType = NdpUtils.transOlkDataType(expression.dataType(), true);
        List<RowExpression> rowArguments = new ArrayList<>();
        checkAttributeReference(childExpression,
                prestoExpressionInfo, fieldMap, childType, rowArguments);
        Signature signature = new Signature(
                QualifiedObjectName.valueOfDefaultFunction(udfEnum.getOperatorName()),
                FunctionKind.SCALAR, returnType.getTypeSignature(), childType.getTypeSignature());
        RowExpression resExpression = new CallExpression(
                signatureName, new BuiltInFunctionHandle(signature),
                returnType, rowArguments);
        prestoExpressionInfo.setReturnType(returnType);
        prestoExpressionInfo.setPrestoRowExpression(resExpression);
    }

    private void createNdpLength(Length expression, PrestoExpressionInfo prestoExpressionInfo,
                                 Map<String, Integer> fieldMap) {
        createNdpSingleParameter(NdpUdfEnum.LENGTH,
                expression, expression.child(), prestoExpressionInfo, fieldMap);
    }

    private void createNdpUpper(Upper expression, PrestoExpressionInfo prestoExpressionInfo,
                                Map<String, Integer> fieldMap) {
        createNdpSingleParameter(NdpUdfEnum.UPPER,
                expression, expression.child(), prestoExpressionInfo, fieldMap);
    }

    private void createNdpLower(Lower expression, PrestoExpressionInfo prestoExpressionInfo,
                                Map<String, Integer> fieldMap) {
        createNdpSingleParameter(NdpUdfEnum.LOWER,
                expression, expression.child(), prestoExpressionInfo, fieldMap);
    }

    private void createNdpCast(Cast expression, PrestoExpressionInfo prestoExpressionInfo,
                               Map<String, Integer> fieldMap) {
        createNdpSingleParameter(NdpUdfEnum.CAST,
                expression, expression.child(), prestoExpressionInfo, fieldMap);
    }

    private void createHiveSimpleUdf(Expression hiveSimpleUDFExpression,
                                     PrestoExpressionInfo prestoExpressionInfo,
                                     Map<String, Integer> fieldMap) {
        String signatureName = ((HiveSimpleUDF) hiveSimpleUDFExpression).name();
        List<Expression> hiveSimpleUdf = JavaConverters.seqAsJavaList(
                hiveSimpleUDFExpression.children());
        Type returnType = NdpUtils.transOlkDataType(
                hiveSimpleUDFExpression.dataType(), false);
        List<RowExpression> rowArguments = new ArrayList<>();
        Type strTypeCandidate = returnType;
        Signature signature;
        for (Expression hiveUdf : hiveSimpleUdf) {
            strTypeCandidate = NdpUtils.transOlkDataType(hiveUdf.dataType(), false);
            checkAttributeReference(hiveUdf, prestoExpressionInfo,
                    fieldMap, strTypeCandidate, rowArguments);
        }
        if (hiveSimpleUdf.size() > 0) {
            TypeSignature[] inputTypeSignatures = new TypeSignature[hiveSimpleUdf.size()];
            for (int i = 0; i < hiveSimpleUdf.size(); i++) {
                Type type = NdpUtils.transOlkDataType(hiveSimpleUdf.get(i).dataType(), false);
                inputTypeSignatures[i] = type.getTypeSignature();
            }
            signature = new Signature(
                    QualifiedObjectName.valueOf("hive", "default", signatureName),
                    FunctionKind.SCALAR, returnType.getTypeSignature(),
                    inputTypeSignatures);
        } else {
            throw new UnsupportedOperationException("The number of UDF parameters is invalid.");
        }
        signatureName = "hive.default." + signatureName.toLowerCase(Locale.ENGLISH);
        RowExpression resExpression = new CallExpression(signatureName.toLowerCase(Locale.ENGLISH),
                new BuiltInFunctionHandle(signature), returnType, rowArguments);
        prestoExpressionInfo.setReturnType(returnType);
        prestoExpressionInfo.setUDF(true);
        prestoExpressionInfo.setPrestoRowExpression(resExpression);
    }

    private void createNdpSubstring(Substring expression, PrestoExpressionInfo prestoExpressionInfo,
                                    Map<String, Integer> fieldMap) {
        String signatureName = NdpUdfEnum.SUBSTRING.getSignatureName();
        Type strType = NdpUtils.transOlkDataType(expression.str().dataType(), expression.str(),
                true);
        if (strType instanceof CharType) {
            strType = createVarcharType(((CharType) strType).getLength());
        }
        Type lenType = NdpUtils.transOlkDataType(expression.len().dataType(), true);
        Type posType = NdpUtils.transOlkDataType(expression.pos().dataType(), true);
        Type returnType = NdpUtils.transOlkDataType(expression.dataType(), true);

        List<RowExpression> rowArguments = new ArrayList<>();
        checkAttributeReference(expression.str(),
                prestoExpressionInfo, fieldMap, strType, rowArguments);
        String startIndex = "0".equals(expression.pos().toString()) ? "1" : expression.pos().toString();
        rowArguments.add(NdpUtils.transConstantExpression(
                startIndex, posType));
        rowArguments.add(NdpUtils.transConstantExpression(
                expression.len().toString(), lenType));
        Signature signature = new Signature(
                QualifiedObjectName.valueOfDefaultFunction(
                        NdpUdfEnum.SUBSTRING.getOperatorName()), FunctionKind.SCALAR,
                returnType.getTypeSignature(), strType.getTypeSignature(),
                posType.getTypeSignature(), lenType.getTypeSignature());
        RowExpression resExpression = new CallExpression(
                signatureName, new BuiltInFunctionHandle(signature),
                returnType, rowArguments);
        prestoExpressionInfo.setPrestoRowExpression(resExpression);
        prestoExpressionInfo.setReturnType(returnType);
    }

    private void createNdpReplace(StringReplace expression,
                                  PrestoExpressionInfo prestoExpressionInfo,
                                  Map<String, Integer> fieldMap) {
        String signatureName = NdpUdfEnum.REPLACE.getSignatureName();
        Type srcType = NdpUtils.transOlkDataType(expression.srcExpr().dataType(), expression.srcExpr(),
                true);
        if (srcType instanceof CharType) {
            srcType = createVarcharType(((CharType) srcType).getLength());
        }
        Type searchType = NdpUtils.transOlkDataType(expression.searchExpr().dataType(), true);
        Type replaceType = NdpUtils.transOlkDataType(expression.replaceExpr().dataType(), true);
        Type returnType = NdpUtils.transOlkDataType(expression.dataType(), true);

        List<RowExpression> rowArguments = new ArrayList<>();
        checkAttributeReference(expression.srcExpr(),
                prestoExpressionInfo, fieldMap, srcType, rowArguments);
        rowArguments.add(NdpUtils.transConstantExpression(
                expression.searchExpr().toString(), searchType));
        rowArguments.add(NdpUtils.transConstantExpression(
                expression.replaceExpr().toString(), replaceType));
        Signature signature = new Signature(
                QualifiedObjectName.valueOfDefaultFunction(
                        NdpUdfEnum.REPLACE.getOperatorName()), FunctionKind.SCALAR,
                returnType.getTypeSignature(), srcType.getTypeSignature(),
                searchType.getTypeSignature(), replaceType.getTypeSignature());
        RowExpression resExpression = new CallExpression(
                signatureName, new BuiltInFunctionHandle(signature),
                returnType, rowArguments);
        prestoExpressionInfo.setReturnType(returnType);
        prestoExpressionInfo.setPrestoRowExpression(resExpression);
    }

    private void createNdpInstr(StringInstr expression, PrestoExpressionInfo prestoExpressionInfo,
                                Map<String, Integer> fieldMap) {
        String signatureName = NdpUdfEnum.INSTR.getSignatureName();
        Type strType = NdpUtils.transOlkDataType(expression.str().dataType(), expression.str(),
                true);
        if (strType instanceof CharType) {
            strType = createVarcharType(((CharType) strType).getLength());
        }
        Type substrType = NdpUtils.transOlkDataType(expression.substr().dataType(), true);
        Type returnType = NdpUtils.transOlkDataType(expression.dataType(), true);

        List<RowExpression> rowArguments = new ArrayList<>();
        checkAttributeReference(expression.str(),
                prestoExpressionInfo, fieldMap, strType, rowArguments);
        rowArguments.add(NdpUtils.transConstantExpression(
                expression.substr().toString(), substrType));
        Signature signature = new Signature(
                QualifiedObjectName.valueOfDefaultFunction(
                        NdpUdfEnum.INSTR.getOperatorName()), FunctionKind.SCALAR,
                returnType.getTypeSignature(), strType.getTypeSignature(),
                substrType.getTypeSignature());
        RowExpression resExpression = new CallExpression(
                signatureName, new BuiltInFunctionHandle(signature),
                returnType, rowArguments);
        prestoExpressionInfo.setReturnType(returnType);
        prestoExpressionInfo.setPrestoRowExpression(resExpression);
    }

    private void createNdpSplit(StringSplit expression, PrestoExpressionInfo prestoExpressionInfo,
                                Map<String, Integer> fieldMap) {
        String signatureName = NdpUdfEnum.SPLIT.getSignatureName();
        Type strType = NdpUtils.transOlkDataType(expression.str().dataType(), expression.str(),
                true);
        if (strType instanceof CharType) {
            strType = createVarcharType(((CharType) strType).getLength());
        }
        Type regexType = NdpUtils.transOlkDataType(expression.regex().dataType(), true);
        Type returnType = NdpUtils.transOlkDataType(expression.dataType(), true);

        List<RowExpression> rowArguments = new ArrayList<>();
        checkAttributeReference(expression.str(),
                prestoExpressionInfo, fieldMap, strType, rowArguments);
        rowArguments.add(NdpUtils.transConstantExpression(
                expression.regex().toString(), regexType));
        Signature signature = new Signature(
                QualifiedObjectName.valueOfDefaultFunction(
                        NdpUdfEnum.SPLIT.getOperatorName()), FunctionKind.SCALAR,
                returnType.getTypeSignature(), strType.getTypeSignature(),
                regexType.getTypeSignature());
        RowExpression resExpression = new CallExpression(
                signatureName, new BuiltInFunctionHandle(signature),
                returnType, rowArguments);
        prestoExpressionInfo.setReturnType(returnType);
        prestoExpressionInfo.setPrestoRowExpression(resExpression);
    }

    private void createNdpSubscript(GetArrayItem expression,
                                    PrestoExpressionInfo prestoExpressionInfo,
                                    Map<String, Integer> fieldMap) {
        String signatureName = NdpUdfEnum.SUBSCRIPT.getSignatureName();
        Type strType = NdpUtils.transOlkDataType(expression.child().dataType(), expression.child(),
                true);
        if (strType instanceof CharType) {
            strType = createVarcharType(((CharType) strType).getLength());
        }
        Type ordinalType = NdpUtils.transOlkDataType(expression.ordinal().dataType(), true);
        Type returnType = NdpUtils.transOlkDataType(expression.dataType(), true);

        List<RowExpression> rowArguments = new ArrayList<>();
        checkAttributeReference(expression.child(),
                prestoExpressionInfo, fieldMap, strType, rowArguments);
        // The presto`s array subscript is initially 1.
        int argumentValue = Integer.parseInt(
                ((Literal) expression.ordinal()).value().toString()) + 1;
        rowArguments.add(NdpUtils.transConstantExpression(
                Integer.toString(argumentValue), ordinalType));
        Signature signature = new Signature(
                QualifiedObjectName.valueOfDefaultFunction(
                        NdpUdfEnum.SUBSCRIPT.getOperatorName()), FunctionKind.SCALAR,
                returnType.getTypeSignature(), strType.getTypeSignature(),
                ordinalType.getTypeSignature());
        RowExpression resExpression = new CallExpression(
                signatureName, new BuiltInFunctionHandle(signature),
                returnType, rowArguments);
        prestoExpressionInfo.setReturnType(returnType);
        prestoExpressionInfo.setPrestoRowExpression(resExpression);
    }
}
