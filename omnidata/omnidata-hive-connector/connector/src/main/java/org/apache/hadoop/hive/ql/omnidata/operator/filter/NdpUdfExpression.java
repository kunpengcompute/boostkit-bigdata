/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.omnidata.operator.filter;

import static io.prestosql.spi.type.BigintType.BIGINT;

import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;

import org.apache.hadoop.hive.ql.omnidata.OmniDataUtils;
import org.apache.hadoop.hive.ql.omnidata.operator.enums.NdpUdfEnum;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.OmniDataPredicate;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Hive Udf Expression
 */
public class NdpUdfExpression {

    private static final Logger LOG = LoggerFactory.getLogger(NdpUdfExpression.class);

    private final OmniDataPredicate predicateInfo;

    private boolean isPushDownUdf = true;

    public NdpUdfExpression(OmniDataPredicate predicateInfo) {
        this.predicateInfo = predicateInfo;
    }

    public boolean isPushDownUdf() {
        return isPushDownUdf;
    }

    public void createNdpUdf(ExprNodeDesc desc, List<RowExpression> arguments) {
        if (!(desc instanceof ExprNodeGenericFuncDesc)) {
            isPushDownUdf = false;
            LOG.info("OmniData Hive UDF failed to push down, since unsupported this ExprNodeDesc: [{}]",
                    desc.getClass());
            return;
        }
        ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc) desc;
        GenericUDF genericUDF = funcDesc.getGenericUDF();
        switch (NdpUdfEnum.getUdfType(genericUDF)) {
            case CAST:
                createNdpCast(funcDesc, arguments);
                break;
            case INSTR:
                createNdpInstr(funcDesc, arguments);
                break;
            case LENGTH:
                createNdpLength(funcDesc, arguments);
                break;
            case LOWER:
                createNdpLower(funcDesc, arguments);
                break;
            case REPLACE:
                createNdpReplace(funcDesc, arguments);
                break;
            case SPLIT:
                createNdpSplit(funcDesc, arguments);
                break;
            case SUBSCRIPT:
                createNdpGetarrayitem(funcDesc, arguments);
                break;
            case SUBSTR:
            case SUBSTRING:
                createNdpSubstr(funcDesc, arguments);
                break;
            case UPPER:
                createNdpUpper(funcDesc, arguments);
                break;
            case UNSUPPORTED:
            default:
                isPushDownUdf = false;
                LOG.info("OmniData Hive UDF failed to push down, since unsupported this genericUDF: [{}]", genericUDF.getClass());
        }
    }

    private void createNdpSplit(ExprNodeGenericFuncDesc funcDesc, List<RowExpression> arguments) {
        String operatorName = NdpUdfEnum.SPLIT.getSignatureName();
        Type strType = OmniDataUtils.transOmniDataType(
                funcDesc.getChildren().get(0).getTypeString());
        Type regexType = OmniDataUtils.transOmniDataType(
                funcDesc.getChildren().get(1).getTypeString());
        Type returnType = OmniDataUtils.transOmniDataType(funcDesc.getTypeString());
        List<RowExpression> rowArguments = new ArrayList<>();
        checkAttributeReference(funcDesc, strType, rowArguments);

        rowArguments.add(OmniDataUtils.transOmniDataConstantExpr(
                ((ExprNodeConstantDesc) funcDesc.getChildren()
                        .get(1)).getValue().toString(), regexType));
        Signature signature = new Signature(
                QualifiedObjectName.valueOfDefaultFunction(NdpUdfEnum.SPLIT.getOperatorName()), FunctionKind.SCALAR,
                new TypeSignature(returnType.toString()), new TypeSignature(strType.toString()),
                new TypeSignature(regexType.toString()));
        CallExpression callExpression = new CallExpression(operatorName, new BuiltInFunctionHandle(signature),
                returnType, rowArguments, Optional.empty());
        arguments.add(callExpression);
    }

    /**
     * name = "substr,substring"
     * _FUNC_(str, pos[, len]) - returns the substring of str that starts at pos and is of length len or
     * _FUNC_(bin, pos[, len]) - returns the slice of byte array that starts at pos and is of length len
     * pos is a 1-based index. If pos<0 the starting position is determined by counting backwards from the end of str.
     * Example: SELECT _FUNC_('Facebook', 5) FROM src LIMIT 1;
     * result: book
     * SELECT _FUNC_('Facebook', -5) FROM src LIMIT 1;
     * result: ebook
     * SELECT _FUNC_('Facebook', 5, 1) FROM src LIMIT 1;
     * result: b
     */
    private void createNdpSubstr(ExprNodeGenericFuncDesc funcDesc, List<RowExpression> arguments) {
        String operatorName = NdpUdfEnum.SUBSTR.getSignatureName();
        Type strType = OmniDataUtils.transOmniDataUdfType(
                funcDesc.getChildren().get(0).getTypeInfo());
        Type lenType = BIGINT;
        Type posType = BIGINT;
        Type returnType = strType;
        List<RowExpression> rowArguments = new ArrayList<>();
        checkAttributeReference(funcDesc, strType, rowArguments);
        rowArguments.add(OmniDataUtils.transOmniDataConstantExpr(
                ((ExprNodeConstantDesc) funcDesc.getChildren().get(1)).getValue().toString(),
                lenType));
        rowArguments.add(OmniDataUtils.transOmniDataConstantExpr(
                ((ExprNodeConstantDesc) funcDesc.getChildren().get(2)).getValue().toString(),
                posType));
        Signature signature = new Signature(
                QualifiedObjectName.valueOfDefaultFunction(NdpUdfEnum.SUBSTR.getOperatorName()), FunctionKind.SCALAR,
                returnType.getTypeSignature(), strType.getTypeSignature(), posType.getTypeSignature(),
                lenType.getTypeSignature());
        CallExpression callExpression = new CallExpression(operatorName, new BuiltInFunctionHandle(signature),
                returnType, rowArguments, Optional.empty());
        arguments.add(callExpression);
    }

    /**
     * name = "length"
     * FUNC_(str | binary) - Returns the length of str or number of bytes in binary data
     * Example: SELECT _FUNC_('Facebook') FROM src LIMIT 1;
     * result: 8
     */
    private void createNdpLength(ExprNodeGenericFuncDesc funcDesc, List<RowExpression> arguments) {
        String signatureName = NdpUdfEnum.LENGTH.getSignatureName();
        Type childType = OmniDataUtils.transOmniDataType(
                funcDesc.getChildren().get(0).getTypeString());
        Type returnType = BIGINT;
        List<RowExpression> rowArguments = new ArrayList<>();
        checkAttributeReference(funcDesc, childType, rowArguments);

        Signature signature = new Signature(QualifiedObjectName.valueOfDefaultFunction(NdpUdfEnum.LENGTH.getOperatorName()),
                FunctionKind.SCALAR, new TypeSignature(returnType.toString()), new TypeSignature(childType.toString()));
        CallExpression callExpression = new CallExpression(signatureName, new BuiltInFunctionHandle(signature),
                returnType, rowArguments, Optional.empty());
        arguments.add(callExpression);

    }

    /**
     * name = "replace"
     * _FUNC_(str, search, rep) - replace all substrings of 'str' that match 'search' with 'rep'
     * Example: SELECT _FUNC_('Facebook') FROM src LIMIT 1;
     * result: 'FACEBOOK'
     */
    private void createNdpReplace(ExprNodeGenericFuncDesc funcDesc, List<RowExpression> arguments) {
        String operatorName = NdpUdfEnum.REPLACE.getSignatureName();
        Type srcType = OmniDataUtils.transOmniDataUdfType(
                funcDesc.getChildren().get(0).getTypeInfo());
        Type searchType = OmniDataUtils.transOmniDataType(
                funcDesc.getChildren().get(1).getTypeString());
        Type replaceType = OmniDataUtils.transOmniDataType(
                funcDesc.getChildren().get(2).getTypeString());
        Type returnType = OmniDataUtils.transOmniDataType(funcDesc.getTypeString());
        List<RowExpression> rowArguments = new ArrayList<>();
        checkAttributeReference(funcDesc, srcType, rowArguments);

        rowArguments.add(OmniDataUtils.transOmniDataConstantExpr(
                ((ExprNodeConstantDesc) funcDesc.getChildren()
                        .get(1)).getValue().toString(), searchType));
        rowArguments.add(OmniDataUtils.transOmniDataConstantExpr(
                ((ExprNodeConstantDesc) funcDesc.getChildren()
                        .get(2)).getValue().toString(), replaceType));
        Signature signature = new Signature(
                QualifiedObjectName.valueOfDefaultFunction(NdpUdfEnum.REPLACE.getOperatorName()), FunctionKind.SCALAR,
                returnType.getTypeSignature(), srcType.getTypeSignature(),
                searchType.getTypeSignature(), replaceType.getTypeSignature());
        CallExpression callExpression = new CallExpression(operatorName, new BuiltInFunctionHandle(signature),
                returnType, rowArguments, Optional.empty());
        arguments.add(callExpression);
    }

    /**
     * name = "upper,ucase"
     * _FUNC_(str) - Returns str with all characters changed to uppercase
     * Example: SELECT _FUNC_('Facebook') FROM src LIMIT 1;
     * result: 'FACEBOOK'
     */
    private void createNdpUpper(ExprNodeGenericFuncDesc funcDesc, List<RowExpression> arguments) {
        createNdpSingleParameter(NdpUdfEnum.UPPER, funcDesc, arguments);
    }

    /**
     * name = "lower,ucase"
     * _FUNC_(str) - Returns str with all characters changed to lowercase
     * Example: SELECT _FUNC_('Facebook') FROM src LIMIT 1;
     * result: 'facebook'
     */
    private void createNdpLower(ExprNodeGenericFuncDesc funcDesc, List<RowExpression> arguments) {
        createNdpSingleParameter(NdpUdfEnum.LOWER, funcDesc, arguments);
    }

    private void createNdpCast(ExprNodeGenericFuncDesc funcDesc, List<RowExpression> arguments) {
        createNdpSingleParameter(NdpUdfEnum.CAST, funcDesc, arguments);
    }

    /**
     * name = "index"
     * _FUNC_(a, n) - Returns the n-th element of a
     */
    private void createNdpGetarrayitem(ExprNodeGenericFuncDesc funcDesc, List<RowExpression> arguments) {
        String operatorName = NdpUdfEnum.SUBSCRIPT.getSignatureName();
        Type strType = OmniDataUtils.transOmniDataType(
                funcDesc.getChildren().get(0).getTypeString());
        Type ordinalType = OmniDataUtils.transOmniDataType(
                funcDesc.getChildren().get(1).getTypeString());
        Type returnType = OmniDataUtils.transOmniDataType(funcDesc.getTypeString());
        List<RowExpression> rowArguments = new ArrayList<>();
        checkAttributeReference(funcDesc, strType, rowArguments);

        rowArguments.add(OmniDataUtils.transOmniDataConstantExpr(
                ((ExprNodeConstantDesc) funcDesc.getChildren()
                        .get(1)).getValue().toString(), ordinalType));
        Signature signature = new Signature(
                QualifiedObjectName.valueOfDefaultFunction(NdpUdfEnum.SUBSCRIPT.getOperatorName()), FunctionKind.SCALAR,
                new TypeSignature(returnType.toString()), new TypeSignature(strType.toString()),
                new TypeSignature(ordinalType.toString()));
        CallExpression callExpression = new CallExpression(operatorName, new BuiltInFunctionHandle(signature),
                returnType, rowArguments, Optional.empty());
        arguments.add(callExpression);
    }

    /**
     * name = "instr"
     * _FUNC_(str, substr) - Returns the index of the first occurance of substr in str
     * Example: SELECT _FUNC_('Facebook', 'boo') FROM src LIMIT 1;
     * result: 5
     */
    private void createNdpInstr(ExprNodeGenericFuncDesc funcDesc, List<RowExpression> arguments) {
        String operatorName = NdpUdfEnum.INSTR.getSignatureName();
        Type strType = OmniDataUtils.transOmniDataType(
                funcDesc.getChildren().get(0).getTypeString());
        Type substrType = OmniDataUtils.transOmniDataType(
                funcDesc.getChildren().get(1).getTypeString());
        Type returnType = OmniDataUtils.transOmniDataType(funcDesc.getTypeString());
        List<RowExpression> rowArguments = new ArrayList<>();
        checkAttributeReference(funcDesc, strType, rowArguments);

        rowArguments.add(OmniDataUtils.transOmniDataConstantExpr(
                ((ExprNodeConstantDesc) funcDesc.getChildren()
                        .get(1)).getValue().toString(), substrType));
        Signature signature = new Signature(
                QualifiedObjectName.valueOfDefaultFunction(NdpUdfEnum.INSTR.getOperatorName()), FunctionKind.SCALAR,
                new TypeSignature(returnType.toString()), new TypeSignature(strType.toString()),
                new TypeSignature(substrType.toString()));
        CallExpression callExpression = new CallExpression(operatorName, new BuiltInFunctionHandle(signature),
                returnType, rowArguments, Optional.empty());
        arguments.add(callExpression);
    }

    private void createNdpSingleParameter(NdpUdfEnum udfEnum, ExprNodeGenericFuncDesc funcDesc,
                                          List<RowExpression> arguments) {
        String signatureName = udfEnum.getSignatureName();
        Type childType = OmniDataUtils.transOmniDataType(
                funcDesc.getChildren().get(0).getTypeString());
        Type returnType = OmniDataUtils.transOmniDataType(funcDesc.getTypeString());
        List<RowExpression> rowArguments = new ArrayList<>();
        checkAttributeReference(funcDesc, childType, rowArguments);

        Signature signature = new Signature(QualifiedObjectName.valueOfDefaultFunction(udfEnum.getOperatorName()),
                FunctionKind.SCALAR, new TypeSignature(returnType.toString()), new TypeSignature(childType.toString()));
        CallExpression callExpression = new CallExpression(signatureName, new BuiltInFunctionHandle(signature),
                returnType, rowArguments, Optional.empty());
        arguments.add(callExpression);
    }

    private void checkAttributeReference(ExprNodeDesc desc, Type childType, List<RowExpression> rowArguments) {
        ExprNodeDesc childDesc = desc.getChildren().get(0);
        if (childDesc instanceof ExprNodeGenericFuncDesc) {
            createNdpUdf(childDesc, rowArguments);
        } else if (childDesc instanceof ExprNodeColumnDesc) {
            int colIndex = predicateInfo.getColName2ColIndex().get(((ExprNodeColumnDesc) childDesc).getColumn());
            rowArguments.add(new InputReferenceExpression(colIndex, childType));
        } else {
            isPushDownUdf = false;
            LOG.info("OmniData Hive UDF failed to push down, since unsupported this ExprNodeDesc: [{}]",
                    childDesc.getClass());
        }
    }

}