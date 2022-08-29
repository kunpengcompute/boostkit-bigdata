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

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.booleanTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.intTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.stringTypeInfo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;

import org.apache.hadoop.hive.ql.omnidata.operator.predicate.OmniDataPredicate;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test for NdpFilter
 *
 * @since 2022-08-24
 */
public class TestNdpFilter {
    @Test
    public void testEqualWithUdf() {
        // cast(col1 as string) = cast(col2 as string)
        // col1
        ExprNodeGenericFuncDesc castUdf1 = mock(ExprNodeGenericFuncDesc.class);
        when(castUdf1.getGenericUDF()).thenReturn(
                new GenericUDFBridge("UDFToString", false, "org.apache.hadoop.hive.ql.udf.UDFToString"));
        when(castUdf1.getTypeString()).thenReturn("string");

        List<ExprNodeDesc> children1 = new ArrayList<>();
        children1.add(new ExprNodeColumnDesc(intTypeInfo, "col1", "test", false));
        when(castUdf1.getChildren()).thenReturn(children1);

        // col1
        ExprNodeGenericFuncDesc castUdf2 = mock(ExprNodeGenericFuncDesc.class);
        when(castUdf2.getGenericUDF()).thenReturn(
                new GenericUDFBridge("UDFToString", false, "org.apache.hadoop.hive.ql.udf.UDFToString"));
        when(castUdf2.getTypeString()).thenReturn("string");

        List<ExprNodeDesc> children2 = new ArrayList<>();
        children2.add(new ExprNodeColumnDesc(intTypeInfo, "col2", "test", false));
        when(castUdf2.getChildren()).thenReturn(children2);

        List<ExprNodeDesc> equalExprChildren = new ArrayList<>();
        equalExprChildren.add(castUdf1);
        equalExprChildren.add(castUdf2);
        ExprNodeGenericFuncDesc equalPredicate = new ExprNodeGenericFuncDesc(booleanTypeInfo, new GenericUDFOPEqual(),
                "=", equalExprChildren);
        NdpFilter equalNdpFilter = new NdpFilter(equalPredicate);
        Assert.assertEquals(NdpFilter.NdpFilterMode.ALL, equalNdpFilter.getMode());

        OmniDataPredicate predicate = mock(OmniDataPredicate.class);
        Map<String, Integer> map = new HashMap<>();
        map.put("col1", 0);
        map.put("col2", 1);
        when(predicate.getColName2ColIndex()).thenReturn(map);
        OmniDataFilter omniDataFilter = new OmniDataFilter(predicate);
        RowExpression result = omniDataFilter.getFilterExpression(equalPredicate, equalNdpFilter);

        Assert.assertEquals(equalNdpFilter.getBinaryTreeHead().getNdpOperator(), NdpFilter.NdpOperator.LEAF);
        Assert.assertEquals(equalNdpFilter.getNdpFilterLeafList().get(0).getNdpLeafOperator(),
                NdpFilter.NdpLeafOperator.EQUAL);
        Assert.assertTrue(equalNdpFilter.getNdpFilterLeafList().get(0).getLeftFilterFunc().isExistsUdf());
        if (result instanceof CallExpression) {
            Assert.assertEquals(((CallExpression) result).getDisplayName(), "EQUAL");
        }
    }

    @Test
    public void testNotEqualOperator() {
        // Not Equal: col1 != 100
        List<ExprNodeDesc> notExprChildren = new ArrayList<>();
        notExprChildren.add(new ExprNodeColumnDesc(intTypeInfo, "col1", "test", false));
        notExprChildren.add(new ExprNodeConstantDesc(intTypeInfo, 100));

        ExprNodeGenericFuncDesc notPredicate = new ExprNodeGenericFuncDesc(booleanTypeInfo, new GenericUDFOPNotEqual(),
                "!=", notExprChildren);
        NdpFilter notNdpFilter = new NdpFilter(notPredicate);
        Assert.assertEquals(NdpFilter.NdpFilterMode.ALL, notNdpFilter.getMode());

        OmniDataPredicate predicate = mock(OmniDataPredicate.class);
        Map<String, Integer> map = new HashMap<>();
        map.put("col1", 0);
        map.put("col2", 1);
        when(predicate.getColName2ColIndex()).thenReturn(map);
        OmniDataFilter omniDataFilter = new OmniDataFilter(predicate);
        RowExpression result = omniDataFilter.getFilterExpression(notPredicate, notNdpFilter);

        Assert.assertTrue(result instanceof CallExpression);
        Assert.assertEquals(((CallExpression) result).getDisplayName(), "not");
        Assert.assertEquals(notNdpFilter.getBinaryTreeHead().getNdpOperator(), NdpFilter.NdpOperator.NOT);
    }

    @Test
    public void testOrOperator() {
        // col1 = 100 or col2 = 300
        List<ExprNodeDesc> orExprChildren1 = new ArrayList<>();
        orExprChildren1.add(new ExprNodeColumnDesc(intTypeInfo, "col1", "test", false));
        orExprChildren1.add(new ExprNodeConstantDesc(intTypeInfo, 100));

        ExprNodeGenericFuncDesc orPredicate1 = new ExprNodeGenericFuncDesc(booleanTypeInfo, new GenericUDFOPEqual(),
                "=", orExprChildren1);

        List<ExprNodeDesc> orExprChildren2 = new ArrayList<>();
        orExprChildren2.add(new ExprNodeColumnDesc(intTypeInfo, "col2", "test", false));
        orExprChildren2.add(new ExprNodeConstantDesc(intTypeInfo, 300));

        ExprNodeGenericFuncDesc orPredicate2 = new ExprNodeGenericFuncDesc(booleanTypeInfo, new GenericUDFOPEqual(),
                "=", orExprChildren2);

        List<ExprNodeDesc> orExprChildren = new ArrayList<>();
        orExprChildren.add(orPredicate1);
        orExprChildren.add(orPredicate2);
        ExprNodeGenericFuncDesc orPredicate = new ExprNodeGenericFuncDesc(booleanTypeInfo, new GenericUDFOPOr(), "or",
                orExprChildren);
        NdpFilter orNdpFilter = new NdpFilter(orPredicate);
        Assert.assertEquals(NdpFilter.NdpFilterMode.ALL, orNdpFilter.getMode());

        OmniDataPredicate predicate = mock(OmniDataPredicate.class);
        Map<String, Integer> map = new HashMap<>();
        map.put("col1", 0);
        map.put("col2", 1);
        when(predicate.getColName2ColIndex()).thenReturn(map);
        OmniDataFilter omniDataFilter = new OmniDataFilter(predicate);
        RowExpression result = omniDataFilter.getFilterExpression(orPredicate, orNdpFilter);

        Assert.assertEquals(orNdpFilter.getBinaryTreeHead().getNdpOperator(), NdpFilter.NdpOperator.OR);
        Assert.assertTrue(result instanceof SpecialForm);
        Assert.assertEquals(((SpecialForm) result).getForm(), SpecialForm.Form.OR);
    }

    @Test
    public void testAndOperator() {
        // And
        List<ExprNodeDesc> andExprChildren1 = new ArrayList<>();
        andExprChildren1.add(new ExprNodeColumnDesc(intTypeInfo, "col1", "test", false));
        andExprChildren1.add(new ExprNodeConstantDesc(intTypeInfo, 100));

        List<ExprNodeDesc> andExprChildren2 = new ArrayList<>();
        andExprChildren2.add(new ExprNodeColumnDesc(intTypeInfo, "col2", "test", false));
        andExprChildren2.add(new ExprNodeConstantDesc(intTypeInfo, 300));

        List<ExprNodeDesc> andExprChildren3 = new ArrayList<>();
        andExprChildren3.add(new ExprNodeColumnDesc(intTypeInfo, "col3", "test", false));
        andExprChildren3.add(new ExprNodeConstantDesc(intTypeInfo, 500));

        ExprNodeGenericFuncDesc andPredicate1 = new ExprNodeGenericFuncDesc(booleanTypeInfo, new GenericUDFOPEqual(),
                "=", andExprChildren1);

        ExprNodeGenericFuncDesc andPredicate2 = new ExprNodeGenericFuncDesc(booleanTypeInfo, new GenericUDFOPEqual(),
                "=", andExprChildren2);

        ExprNodeGenericFuncDesc andPredicate3 = new ExprNodeGenericFuncDesc(booleanTypeInfo, new GenericUDFOPEqual(),
                "=", andExprChildren3);

        List<ExprNodeDesc> andExprChildren = new ArrayList<>();
        andExprChildren.add(andPredicate1);
        andExprChildren.add(andPredicate2);
        andExprChildren.add(andPredicate3);
        ExprNodeGenericFuncDesc andPredicate = new ExprNodeGenericFuncDesc(booleanTypeInfo, new GenericUDFOPAnd(),
                "and", andExprChildren);
        NdpFilter andNdpFilter = new NdpFilter(andPredicate);
        Assert.assertEquals(NdpFilter.NdpFilterMode.ALL, andNdpFilter.getMode());
        andNdpFilter.createNdpFilterBinaryTree(andPredicate);
        Assert.assertEquals(andNdpFilter.getBinaryTreeHead().getNdpOperator(), NdpFilter.NdpOperator.AND);
    }

    @Test
    public void testAndOperatorWithUdf() {
        // cast(col1 as string)='100' and col2 = cast(100 as string)
        // col1
        ExprNodeGenericFuncDesc castUdf = mock(ExprNodeGenericFuncDesc.class);
        when(castUdf.getGenericUDF()).thenReturn(
                new GenericUDFBridge("UDFToString", false, "org.apache.hadoop.hive.ql.udf.UDFToString"));
        when(castUdf.getTypeString()).thenReturn("string");

        List<ExprNodeDesc> children = new ArrayList<>();
        children.add(new ExprNodeColumnDesc(intTypeInfo, "col1", "test", false));
        when(castUdf.getChildren()).thenReturn(children);

        List<ExprNodeDesc> andExprChildren1 = new ArrayList<>();
        andExprChildren1.add(castUdf);
        andExprChildren1.add(new ExprNodeConstantDesc(stringTypeInfo, 100));

        ExprNodeGenericFuncDesc andPredicate1 = new ExprNodeGenericFuncDesc(booleanTypeInfo, new GenericUDFOPEqual(),
                "=", andExprChildren1);

        // col2
        List<ExprNodeDesc> andExprChildren2 = new ArrayList<>();
        andExprChildren2.add(new ExprNodeColumnDesc(stringTypeInfo, "col2", "test", false));
        andExprChildren2.add(new ExprNodeConstantDesc(intTypeInfo, 300));

        ExprNodeGenericFuncDesc andPredicate2 = new ExprNodeGenericFuncDesc(booleanTypeInfo, new GenericUDFOPEqual(),
                "=", andExprChildren2);

        List<ExprNodeDesc> andExprChildren = new ArrayList<>();
        andExprChildren.add(andPredicate1);
        andExprChildren.add(andPredicate2);
        ExprNodeGenericFuncDesc andPredicate = new ExprNodeGenericFuncDesc(booleanTypeInfo, new GenericUDFOPAnd(),
                "and", andExprChildren);
        NdpFilter andNdpFilter = new NdpFilter(andPredicate);
        Assert.assertEquals(NdpFilter.NdpFilterMode.ALL, andNdpFilter.getMode());

        OmniDataPredicate predicate = mock(OmniDataPredicate.class);
        Map<String, Integer> map = new HashMap<>();
        map.put("col1", 0);
        map.put("col2", 1);
        when(predicate.getColName2ColIndex()).thenReturn(map);
        OmniDataFilter omniDataFilter = new OmniDataFilter(predicate);
        RowExpression result = omniDataFilter.getFilterExpression(andPredicate, andNdpFilter);

        Assert.assertEquals(andNdpFilter.getBinaryTreeHead().getNdpOperator(), NdpFilter.NdpOperator.AND);
        Assert.assertTrue(andNdpFilter.getNdpFilterLeafList().get(0).getLeftFilterFunc().isExistsUdf());
        Assert.assertTrue(result instanceof SpecialForm);
        Assert.assertEquals(((SpecialForm) result).getForm(), SpecialForm.Form.AND);
    }

    @Test
    public void testInOperator() {
        // col1 in (100, 200, 300)
        List<ExprNodeDesc> inExprChildren = new ArrayList<>();
        inExprChildren.add(new ExprNodeColumnDesc(intTypeInfo, "col1", "test", false));
        inExprChildren.add(new ExprNodeConstantDesc(intTypeInfo, 100));
        inExprChildren.add(new ExprNodeConstantDesc(intTypeInfo, 200));
        inExprChildren.add(new ExprNodeConstantDesc(intTypeInfo, 300));

        ExprNodeGenericFuncDesc inPredicate = new ExprNodeGenericFuncDesc(booleanTypeInfo, new GenericUDFIn(), "in",
                inExprChildren);
        NdpFilter inNdpFilter = new NdpFilter(inPredicate);
        Assert.assertEquals(NdpFilter.NdpFilterMode.ALL, inNdpFilter.getMode());
        inNdpFilter.createNdpFilterBinaryTree(inPredicate);

        Assert.assertEquals(inNdpFilter.getBinaryTreeHead().getNdpOperator(), NdpFilter.NdpOperator.LEAF);
        Assert.assertEquals(inNdpFilter.getNdpFilterLeafList().get(0).getNdpLeafOperator(),
                NdpFilter.NdpLeafOperator.IN);
        Assert.assertFalse(inNdpFilter.getNdpFilterLeafList().get(0).getLeftFilterFunc().isExistsUdf());
    }

    @Test
    public void testNotInOperator() {
        // col1 not in (100, 200, 300)
        List<ExprNodeDesc> inExprChildren = new ArrayList<>();
        inExprChildren.add(new ExprNodeColumnDesc(intTypeInfo, "col1", "test", false));
        inExprChildren.add(new ExprNodeConstantDesc(intTypeInfo, 100));
        inExprChildren.add(new ExprNodeConstantDesc(intTypeInfo, 200));
        inExprChildren.add(new ExprNodeConstantDesc(intTypeInfo, 300));

        ExprNodeGenericFuncDesc inPredicate = new ExprNodeGenericFuncDesc(booleanTypeInfo, new GenericUDFIn(), "in",
                inExprChildren);

        List<ExprNodeDesc> notInExprChildren = new ArrayList<>();
        notInExprChildren.add(inPredicate);
        ExprNodeGenericFuncDesc notInPredicate = new ExprNodeGenericFuncDesc(booleanTypeInfo, new GenericUDFOPNot(),
                "not", notInExprChildren);
        NdpFilter notInENdpFilter = new NdpFilter(notInPredicate);
        Assert.assertEquals(NdpFilter.NdpFilterMode.ALL, notInENdpFilter.getMode());

        OmniDataPredicate predicate = mock(OmniDataPredicate.class);
        Map<String, Integer> map = new HashMap<>();
        map.put("col1", 0);
        when(predicate.getColName2ColIndex()).thenReturn(map);
        OmniDataFilter omniDataFilter = new OmniDataFilter(predicate);
        RowExpression result = omniDataFilter.getFilterExpression(notInPredicate, notInENdpFilter);

        Assert.assertEquals(notInENdpFilter.getBinaryTreeHead().getNdpOperator(), NdpFilter.NdpOperator.NOT);
        Assert.assertEquals(notInENdpFilter.getNdpFilterLeafList().get(0).getNdpLeafOperator(),
                NdpFilter.NdpLeafOperator.IN);
        Assert.assertFalse(notInENdpFilter.getNdpFilterLeafList().get(0).getLeftFilterFunc().isExistsUdf());
    }

    @Test
    public void testInOperatorWithUdf() {
        // cast(col1 as string) in ('100', '200', '300')
        ExprNodeGenericFuncDesc castUdf = mock(ExprNodeGenericFuncDesc.class);
        when(castUdf.getGenericUDF()).thenReturn(
                new GenericUDFBridge("UDFToString", false, "org.apache.hadoop.hive.ql.udf.UDFToString"));
        when(castUdf.getTypeString()).thenReturn("string");

        List<ExprNodeDesc> children = new ArrayList<>();
        children.add(new ExprNodeColumnDesc(intTypeInfo, "col1", "test", false));
        when(castUdf.getChildren()).thenReturn(children);

        List<ExprNodeDesc> inExprChildren = new ArrayList<>();
        inExprChildren.add(castUdf);
        inExprChildren.add(new ExprNodeConstantDesc(stringTypeInfo, 100));
        inExprChildren.add(new ExprNodeConstantDesc(stringTypeInfo, 200));
        inExprChildren.add(new ExprNodeConstantDesc(stringTypeInfo, 300));

        ExprNodeGenericFuncDesc inPredicate = new ExprNodeGenericFuncDesc(booleanTypeInfo, new GenericUDFIn(), "in",
                inExprChildren);
        NdpFilter inNdpFilter = new NdpFilter(inPredicate);
        Assert.assertEquals(NdpFilter.NdpFilterMode.ALL, inNdpFilter.getMode());

        OmniDataPredicate predicate = mock(OmniDataPredicate.class);
        Map<String, Integer> map = new HashMap<>();
        map.put("col1", 0);
        when(predicate.getColName2ColIndex()).thenReturn(map);
        OmniDataFilter omniDataFilter = new OmniDataFilter(predicate);
        RowExpression result = omniDataFilter.getFilterExpression(inPredicate, inNdpFilter);

        Assert.assertEquals(inNdpFilter.getBinaryTreeHead().getNdpOperator(), NdpFilter.NdpOperator.LEAF);
        Assert.assertEquals(inNdpFilter.getNdpFilterLeafList().get(0).getNdpLeafOperator(),
                NdpFilter.NdpLeafOperator.IN);
        Assert.assertTrue(inNdpFilter.getNdpFilterLeafList().get(0).getLeftFilterFunc().isExistsUdf());
        Assert.assertTrue(result instanceof SpecialForm);
        Assert.assertEquals(((SpecialForm) result).getForm(), SpecialForm.Form.IN);
    }

    @Test
    public void testBetweenOperator() {
        // between col1 100 and 200
        List<ExprNodeDesc> betweenExprChildren = new ArrayList<>();
        betweenExprChildren.add(new ExprNodeConstantDesc(booleanTypeInfo, false));
        betweenExprChildren.add(new ExprNodeColumnDesc(intTypeInfo, "col1", "test", false));
        betweenExprChildren.add(new ExprNodeConstantDesc(intTypeInfo, 100));
        betweenExprChildren.add(new ExprNodeConstantDesc(intTypeInfo, 200));

        ExprNodeGenericFuncDesc betweenPredicate = new ExprNodeGenericFuncDesc(booleanTypeInfo, new GenericUDFBetween(),
                "between", betweenExprChildren);
        NdpFilter betweenNdpFilter = new NdpFilter(betweenPredicate);
        Assert.assertEquals(NdpFilter.NdpFilterMode.ALL, betweenNdpFilter.getMode());

        OmniDataPredicate predicate = mock(OmniDataPredicate.class);
        Map<String, Integer> map = new HashMap<>();
        map.put("col1", 0);
        when(predicate.getColName2ColIndex()).thenReturn(map);
        OmniDataFilter omniDataFilter = new OmniDataFilter(predicate);
        RowExpression result = omniDataFilter.getFilterExpression(betweenPredicate, betweenNdpFilter);

        Assert.assertEquals(betweenNdpFilter.getBinaryTreeHead().getNdpOperator(), NdpFilter.NdpOperator.LEAF);
        Assert.assertEquals(betweenNdpFilter.getNdpFilterLeafList().get(0).getNdpLeafOperator(),
                NdpFilter.NdpLeafOperator.BETWEEN);
        Assert.assertFalse(betweenNdpFilter.getNdpFilterLeafList().get(0).getLeftFilterFunc().isExistsUdf());
        Assert.assertTrue(result instanceof SpecialForm);
        Assert.assertEquals(((SpecialForm) result).getForm(), SpecialForm.Form.BETWEEN);
    }

    @Test
    public void testNdpFilterFunc() {
        NdpFilterLeaf.NdpFilterFunc ndpFilterFunc = new NdpFilterLeaf.NdpFilterFunc("a", "b", true, null);
        String returnType = ndpFilterFunc.getReturnType();
        String columnName = ndpFilterFunc.getColumnName();
        boolean isPartitionColumn = ndpFilterFunc.getPartitionColumn();
        ExprNodeDesc udfExpression = ndpFilterFunc.getUdfExpression();
        Assert.assertEquals(returnType, "a");
        Assert.assertEquals(columnName, "b");
        Assert.assertTrue(isPartitionColumn);
        Assert.assertNull(udfExpression);
        ndpFilterFunc.setReturnType("a1");
        ndpFilterFunc.setColumnName("b2");
        ndpFilterFunc.setPartitionColumn(false);
        Assert.assertEquals(ndpFilterFunc.getReturnType(), "a1");
        Assert.assertEquals(ndpFilterFunc.getColumnName(), "b2");
        Assert.assertFalse(ndpFilterFunc.getPartitionColumn());
    }
}