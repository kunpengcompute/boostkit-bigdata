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
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.VarcharType;

import org.apache.hadoop.hive.ql.omnidata.OmniDataUtils;
import org.apache.hadoop.hive.ql.omnidata.operator.enums.NdpUdfEnum;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.OmniDataPredicate;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIndex;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFInstr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLength;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLower;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * TestNdpUdfExpression
 *
 * @since 2022-08-24
 */
public class TestNdpUdfExpression {
    private NdpUdfExpression createUdfExpression() {
        OmniDataPredicate predicate = mock(OmniDataPredicate.class);
        Map<String, Integer> map = new HashMap<>();
        map.put("o_varchar25", 0);
        map.put("o_bigint", 1);
        when(predicate.getColName2ColIndex()).thenReturn(map);
        return new NdpUdfExpression(predicate);
    }

    @Test
    public void testGetUdfType() {
        // CAST
        GenericUDFBridge genericUDFBridge = new GenericUDFBridge("UDFToString", false,
                "org.apache.hadoop.hive.ql.udf.UDFToString");
        Assert.assertEquals(NdpUdfEnum.getUdfType(genericUDFBridge), NdpUdfEnum.CAST);

        // INSTR
        GenericUDFInstr genericUDFInstr = new GenericUDFInstr();
        Assert.assertEquals(NdpUdfEnum.getUdfType(genericUDFInstr), NdpUdfEnum.INSTR);

        // LENGTH
        GenericUDFLength genericUDFLength = new GenericUDFLength();
        Assert.assertEquals(NdpUdfEnum.getUdfType(genericUDFLength), NdpUdfEnum.LENGTH);

        // LOWER
        GenericUDFLower genericUDFLower = new GenericUDFLower();
        Assert.assertEquals(NdpUdfEnum.getUdfType(genericUDFLower), NdpUdfEnum.LOWER);

        // REPLACE
        GenericUDFBridge genericUDFBridge1 = new GenericUDFBridge("replace", false,
                "org.apache.hadoop.hive.ql.udf.UDFReplace");
        Assert.assertEquals(NdpUdfEnum.getUdfType(genericUDFBridge1), NdpUdfEnum.REPLACE);

        // SUBSCRIPT
        GenericUDFIndex genericUDFIndex = new GenericUDFIndex();
        Assert.assertEquals(NdpUdfEnum.getUdfType(genericUDFIndex), NdpUdfEnum.SUBSCRIPT);

        // SUBSTR
        GenericUDFBridge genericUDFBridge2 = new GenericUDFBridge("substr", false,
                "org.apache.hadoop.hive.ql.udf.UDFSubstr");
        Assert.assertEquals(NdpUdfEnum.getUdfType(genericUDFBridge2), NdpUdfEnum.SUBSTR);

        // UPPER
        GenericUDFUpper genericUDFUpper = new GenericUDFUpper();
        Assert.assertEquals(NdpUdfEnum.getUdfType(genericUDFUpper), NdpUdfEnum.UPPER);

        NdpUdfEnum.UPPER.setSignatureName("ndp_upper");
        NdpUdfEnum.UPPER.setOperatorName("ndp_upper");
        Assert.assertEquals(NdpUdfEnum.UPPER.getSignatureName(), "ndp_upper");
        Assert.assertEquals(NdpUdfEnum.UPPER.getOperatorName(), "ndp_upper");
    }

    @Test
    public void testCreateNdpUdf() {
        OmniDataPredicate predicate = mock(OmniDataPredicate.class);
        Map<String, Integer> map = new HashMap<>();
        map.put("o_varchar25", 0);
        map.put("o_bigint", 1);
        when(predicate.getColName2ColIndex()).thenReturn(map);
        NdpUdfExpression ndpUdfExpression = new NdpUdfExpression(predicate);
        // ExprNodeConstantDesc
        ExprNodeConstantDesc nodeConstantDesc = new ExprNodeConstantDesc();
        ndpUdfExpression.createNdpUdf(nodeConstantDesc, new ArrayList<>());
        Assert.assertFalse(ndpUdfExpression.isPushDownUdf());
    }

    @Test
    public void testCast() {
        NdpUdfExpression ndpUdfExpression = createUdfExpression();
        ExprNodeGenericFuncDesc funcDesc = mock(ExprNodeGenericFuncDesc.class);
        when(funcDesc.getGenericUDF()).thenReturn(
                new GenericUDFBridge("UDFToString", false, "org.apache.hadoop.hive.ql.udf.UDFToString"));
        when(funcDesc.getTypeString()).thenReturn("string");
        List<ExprNodeDesc> nodeDescs = new ArrayList<>();
        nodeDescs.add(new ExprNodeColumnDesc(new VarcharTypeInfo(25), "o_varchar25", "data_type_test_orc", false));
        List<RowExpression> arguments = new ArrayList<>();
        when(funcDesc.getChildren()).thenReturn(nodeDescs);
        ndpUdfExpression.createNdpUdf(funcDesc, arguments);

        List<RowExpression> rowArguments = new ArrayList<>();
        rowArguments.add(new InputReferenceExpression(0, VARCHAR));
        Signature signature = new Signature(QualifiedObjectName.valueOfDefaultFunction("$operator$cast"),
                FunctionKind.SCALAR, new TypeSignature(VARCHAR.toString()), new TypeSignature(VARCHAR.toString()));
        CallExpression callExpression = new CallExpression("cast", new BuiltInFunctionHandle(signature), VARCHAR,
                rowArguments, Optional.empty());

        Assert.assertEquals(callExpression, arguments.get(0));
    }

    @Test
    public void testReplace() {
        NdpUdfExpression ndpUdfExpression = createUdfExpression();
        ExprNodeGenericFuncDesc funcDesc = mock(ExprNodeGenericFuncDesc.class);
        when(funcDesc.getGenericUDF()).thenReturn(
                new GenericUDFBridge("replace", false, "org.apache.hadoop.hive.ql.udf.UDFReplace"));
        when(funcDesc.getTypeString()).thenReturn("string");
        List<ExprNodeDesc> nodeDescs = new ArrayList<>();
        nodeDescs.add(new ExprNodeColumnDesc(new VarcharTypeInfo(25), "o_varchar25", "data_type_test_orc", false));
        nodeDescs.add(new ExprNodeConstantDesc("UDF5"));
        nodeDescs.add(new ExprNodeConstantDesc("udf5"));
        List<RowExpression> arguments = new ArrayList<>();
        when(funcDesc.getChildren()).thenReturn(nodeDescs);
        ndpUdfExpression.createNdpUdf(funcDesc, arguments);

        List<RowExpression> rowArguments = new ArrayList<>();
        rowArguments.add(new InputReferenceExpression(0, VARCHAR));
        rowArguments.add(OmniDataUtils.transOmniDataConstantExpr("UDF5", VARCHAR));
        rowArguments.add(OmniDataUtils.transOmniDataConstantExpr("udf5", VARCHAR));
        Signature signature = new Signature(QualifiedObjectName.valueOfDefaultFunction("replace"), FunctionKind.SCALAR,
                new TypeSignature(VARCHAR.toString()), new TypeSignature(VarcharType.createVarcharType(25).toString()),
                new TypeSignature(VARCHAR.toString()), new TypeSignature(VARCHAR.toString()));
        CallExpression callExpression = new CallExpression("replace", new BuiltInFunctionHandle(signature), VARCHAR,
                rowArguments, Optional.empty());

        if (arguments.get(0) instanceof CallExpression) {
            Assert.assertEquals(callExpression.getFunctionHandle().toString(),
                    ((CallExpression) arguments.get(0)).getFunctionHandle().toString());
        }
    }

    @Test
    public void testSubstr() {
        NdpUdfExpression ndpUdfExpression = createUdfExpression();
        ExprNodeGenericFuncDesc funcDesc = mock(ExprNodeGenericFuncDesc.class);
        when(funcDesc.getGenericUDF()).thenReturn(
                new GenericUDFBridge("substr", false, "org.apache.hadoop.hive.ql.udf.UDFSubstr"));
        when(funcDesc.getTypeString()).thenReturn("string");
        List<ExprNodeDesc> nodeDescs = new ArrayList<>();
        nodeDescs.add(new ExprNodeColumnDesc(new VarcharTypeInfo(25), "o_varchar25", "data_type_test_orc", false));
        nodeDescs.add(new ExprNodeConstantDesc(1));
        nodeDescs.add(new ExprNodeConstantDesc(2));
        List<RowExpression> arguments = new ArrayList<>();
        when(funcDesc.getChildren()).thenReturn(nodeDescs);
        ndpUdfExpression.createNdpUdf(funcDesc, arguments);

        List<RowExpression> rowArguments = new ArrayList<>();
        rowArguments.add(new InputReferenceExpression(0, VARCHAR));
        rowArguments.add(OmniDataUtils.transOmniDataConstantExpr("1", BIGINT));
        rowArguments.add(OmniDataUtils.transOmniDataConstantExpr("2", BIGINT));
        Signature signature = new Signature(QualifiedObjectName.valueOfDefaultFunction("substr"), FunctionKind.SCALAR,
                new TypeSignature(VarcharType.createVarcharType(25).toString()),
                new TypeSignature(VarcharType.createVarcharType(25).toString()), new TypeSignature(BIGINT.toString()),
                new TypeSignature(BIGINT.toString()));
        CallExpression callExpression = new CallExpression("substr", new BuiltInFunctionHandle(signature), VARCHAR,
                rowArguments, Optional.empty());

        if (arguments.get(0) instanceof CallExpression) {
            Assert.assertEquals(callExpression.getFunctionHandle().toString(),
                    ((CallExpression) arguments.get(0)).getFunctionHandle().toString());
        }
    }

    @Test
    public void testInstr() {
        NdpUdfExpression ndpUdfExpression = createUdfExpression();
        ExprNodeGenericFuncDesc funcDesc = mock(ExprNodeGenericFuncDesc.class);
        when(funcDesc.getGenericUDF()).thenReturn(new GenericUDFInstr());
        when(funcDesc.getTypeString()).thenReturn("string");
        List<ExprNodeDesc> nodeDescs = new ArrayList<>();
        nodeDescs.add(new ExprNodeColumnDesc(new VarcharTypeInfo(25), "o_varchar25", "data_type_test_orc", false));
        nodeDescs.add(new ExprNodeConstantDesc("UDF5"));
        List<RowExpression> arguments = new ArrayList<>();
        when(funcDesc.getChildren()).thenReturn(nodeDescs);
        ndpUdfExpression.createNdpUdf(funcDesc, arguments);

        List<RowExpression> rowArguments = new ArrayList<>();
        rowArguments.add(new InputReferenceExpression(0, VARCHAR));
        rowArguments.add(OmniDataUtils.transOmniDataConstantExpr("UDF5", VARCHAR));
        Signature signature = new Signature(QualifiedObjectName.valueOfDefaultFunction("instr"), FunctionKind.SCALAR,
                new TypeSignature(VARCHAR.toString()), new TypeSignature(VARCHAR.toString()),
                new TypeSignature(VARCHAR.toString()));
        CallExpression callExpression = new CallExpression("instr", new BuiltInFunctionHandle(signature), VARCHAR,
                rowArguments, Optional.empty());

        if (arguments.get(0) instanceof CallExpression) {
            Assert.assertEquals(callExpression.getFunctionHandle().toString(),
                    ((CallExpression) arguments.get(0)).getFunctionHandle().toString());
        }
    }

    @Test
    public void testLength() {
        NdpUdfExpression ndpUdfExpression = createUdfExpression();
        ExprNodeGenericFuncDesc funcDesc = mock(ExprNodeGenericFuncDesc.class);
        when(funcDesc.getGenericUDF()).thenReturn(new GenericUDFLength());
        when(funcDesc.getTypeString()).thenReturn("int");
        List<ExprNodeDesc> nodeDescs = new ArrayList<>();
        nodeDescs.add(new ExprNodeColumnDesc(new VarcharTypeInfo(25), "o_varchar25", "data_type_test_orc", false));
        List<RowExpression> arguments = new ArrayList<>();
        when(funcDesc.getChildren()).thenReturn(nodeDescs);
        ndpUdfExpression.createNdpUdf(funcDesc, arguments);

        List<RowExpression> rowArguments = new ArrayList<>();
        rowArguments.add(new InputReferenceExpression(0, VARCHAR));
        Signature signature = new Signature(QualifiedObjectName.valueOfDefaultFunction("length"), FunctionKind.SCALAR,
                new TypeSignature(BIGINT.toString()), new TypeSignature(VARCHAR.toString()));
        CallExpression callExpression = new CallExpression("length", new BuiltInFunctionHandle(signature), BIGINT,
                rowArguments, Optional.empty());

        Assert.assertEquals(callExpression, arguments.get(0));
    }

    @Test
    public void testUpper() {
        NdpUdfExpression ndpUdfExpression = createUdfExpression();
        ExprNodeGenericFuncDesc funcDesc = mock(ExprNodeGenericFuncDesc.class);
        when(funcDesc.getGenericUDF()).thenReturn(new GenericUDFUpper());
        when(funcDesc.getTypeString()).thenReturn("string");
        List<ExprNodeDesc> nodeDescs = new ArrayList<>();
        nodeDescs.add(new ExprNodeColumnDesc(new VarcharTypeInfo(25), "o_varchar25", "data_type_test_orc", false));
        List<RowExpression> arguments = new ArrayList<>();
        when(funcDesc.getChildren()).thenReturn(nodeDescs);
        ndpUdfExpression.createNdpUdf(funcDesc, arguments);

        List<RowExpression> rowArguments = new ArrayList<>();
        rowArguments.add(new InputReferenceExpression(0, VARCHAR));
        Signature signature = new Signature(QualifiedObjectName.valueOfDefaultFunction("upper"), FunctionKind.SCALAR,
                new TypeSignature(VARCHAR.toString()), new TypeSignature(VARCHAR.toString()));
        CallExpression callExpression = new CallExpression("upper", new BuiltInFunctionHandle(signature), VARCHAR,
                rowArguments, Optional.empty());

        Assert.assertEquals(callExpression, arguments.get(0));
    }

    @Test
    public void testLower() {
        NdpUdfExpression ndpUdfExpression = createUdfExpression();
        ExprNodeGenericFuncDesc funcDesc = mock(ExprNodeGenericFuncDesc.class);
        when(funcDesc.getGenericUDF()).thenReturn(new GenericUDFLower());
        when(funcDesc.getTypeString()).thenReturn("string");
        List<ExprNodeDesc> nodeDescs = new ArrayList<>();
        nodeDescs.add(new ExprNodeColumnDesc(new VarcharTypeInfo(25), "o_varchar25", "data_type_test_orc", false));
        List<RowExpression> arguments = new ArrayList<>();
        when(funcDesc.getChildren()).thenReturn(nodeDescs);
        ndpUdfExpression.createNdpUdf(funcDesc, arguments);

        List<RowExpression> rowArguments = new ArrayList<>();
        rowArguments.add(new InputReferenceExpression(0, VARCHAR));
        Signature signature = new Signature(QualifiedObjectName.valueOfDefaultFunction("lower"), FunctionKind.SCALAR,
                new TypeSignature(VARCHAR.toString()), new TypeSignature(VARCHAR.toString()));
        CallExpression callExpression = new CallExpression("lower", new BuiltInFunctionHandle(signature), VARCHAR,
                rowArguments, Optional.empty());

        Assert.assertEquals(callExpression, arguments.get(0));
    }
}