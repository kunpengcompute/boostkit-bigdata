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

package org.apache.hadoop.hive.ql.omnidata.operator.predicate;

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.intTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.longTypeInfo;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ConstantVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColMultiplyLongScalar;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.VectorSelectDesc;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test OmniDataPredicate
 *
 * @since 2022-08-24
 */
public class TestOmniDataPredicate {
    @Test
    public void testOmniDataPredicateWithPartitionKey() {
        // build a test tableScan operator with partition key
        TableScanOperator tableScanOperator = mock(TableScanOperator.class);
        String schemaEvolutionColumns = "col0, col1, col2";
        String schemaEvolutionColumnsTypes = "int,int,int";
        when(tableScanOperator.getSchemaEvolutionColumns()).thenReturn(schemaEvolutionColumns);
        when(tableScanOperator.getSchemaEvolutionColumnsTypes()).thenReturn(schemaEvolutionColumnsTypes);

        List<Integer> neededColumnIDs = new ArrayList<>();
        neededColumnIDs.add(0);
        neededColumnIDs.add(1);

        List<String> neededColumns = new ArrayList<>();
        neededColumns.add("col0");
        neededColumns.add("col1");

        List<String> referenceColumns = new ArrayList<>();
        referenceColumns.add("col0");
        referenceColumns.add("col1");
        referenceColumns.add("col_partition");

        ArrayList<ColumnInfo> signature = new ArrayList<>();
        signature.add(new ColumnInfo("col0", intTypeInfo, "test", false));
        signature.add(new ColumnInfo("col1", intTypeInfo, "test", false));
        signature.add(new ColumnInfo("col2", intTypeInfo, "test", false));
        signature.add(new ColumnInfo("col_partition", intTypeInfo, "test", true));

        Table table = new Table("test", "test");
        TableScanDesc tableScanDesc = new TableScanDesc("test", table);
        tableScanDesc.setNeededColumns(neededColumns);
        tableScanDesc.setNeededColumnIDs(neededColumnIDs);
        tableScanDesc.setReferencedColumns(referenceColumns);

        RowSchema rowSchema = mock(RowSchema.class);
        when(rowSchema.getSignature()).thenReturn(signature);
        when(tableScanOperator.getSchema()).thenReturn(rowSchema);
        when(tableScanOperator.getConf()).thenReturn(tableScanDesc);

        OmniDataPredicate predicate = new OmniDataPredicate(tableScanOperator);
        assertTrue(predicate.isPushDown());
    }

    @Test
    public void testSum() {
        TableScanOperator tableScanOperator = mock(TableScanOperator.class);
        String schemaEvolutionColumns = "col0";
        String schemaEvolutionColumnsTypes = "long";
        when(tableScanOperator.getSchemaEvolutionColumns()).thenReturn(schemaEvolutionColumns);
        when(tableScanOperator.getSchemaEvolutionColumnsTypes()).thenReturn(schemaEvolutionColumnsTypes);

        List<Integer> neededColumnIDs = new ArrayList<>();
        neededColumnIDs.add(0);

        List<String> neededColumns = new ArrayList<>();
        neededColumns.add("col0");

        List<String> referenceColumns = new ArrayList<>();
        referenceColumns.add("col0");

        ArrayList<ColumnInfo> signature = new ArrayList<>();
        signature.add(new ColumnInfo("col0", longTypeInfo, "test", false));

        Table table = new Table("test", "test");
        TableScanDesc tableScanDesc = new TableScanDesc("test", table);
        tableScanDesc.setNeededColumns(neededColumns);
        tableScanDesc.setNeededColumnIDs(neededColumnIDs);
        tableScanDesc.setReferencedColumns(referenceColumns);

        RowSchema rowSchema = mock(RowSchema.class);
        when(rowSchema.getSignature()).thenReturn(signature);
        when(tableScanOperator.getSchema()).thenReturn(rowSchema);
        when(tableScanOperator.getConf()).thenReturn(tableScanDesc);

        OmniDataPredicate predicate = new OmniDataPredicate(tableScanOperator);
        int outputColumnId = 12;
        VectorExpression[] expressions = new VectorExpression[1];
        expressions[0] = new LongColMultiplyLongScalar(0, 2, outputColumnId);
        PrimitiveTypeInfo typeInfo = new PrimitiveTypeInfo();
        typeInfo.setTypeName("bigint");
        expressions[0].setOutputTypeInfo(typeInfo);
        expressions[0].setInputTypeInfos(typeInfo, typeInfo);
        VectorSelectDesc vectorSelectDesc = new VectorSelectDesc();
        vectorSelectDesc.setSelectExpressions(expressions);
        predicate.setSelectExpressions(vectorSelectDesc);
        predicate.addProjectionsByAgg(outputColumnId);
        if (predicate.getProjections().get(0) instanceof CallExpression) {
            Assert.assertEquals(((CallExpression) predicate.getProjections().get(0)).getDisplayName(), "MULTIPLY");
        }
    }

    @Test
    public void testConstant() throws HiveException {
        // select sum(2) from
        TableScanOperator tableScanOperator = mock(TableScanOperator.class);
        String schemaEvolutionColumns = "col0,col1";
        String schemaEvolutionColumnsTypes = "long,int";
        when(tableScanOperator.getSchemaEvolutionColumns()).thenReturn(schemaEvolutionColumns);
        when(tableScanOperator.getSchemaEvolutionColumnsTypes()).thenReturn(schemaEvolutionColumnsTypes);

        List<Integer> neededColumnIDs = new ArrayList<>();
        neededColumnIDs.add(0);
        neededColumnIDs.add(1);

        List<String> neededColumns = new ArrayList<>();
        neededColumns.add("col0");
        neededColumns.add("col1");

        List<String> referenceColumns = new ArrayList<>();
        referenceColumns.add("col0");
        referenceColumns.add("col1");

        ArrayList<ColumnInfo> signature = new ArrayList<>();
        signature.add(new ColumnInfo("col0", longTypeInfo, "test", false));
        signature.add(new ColumnInfo("col1", intTypeInfo, "test", false));

        Table table = new Table("test", "test");
        TableScanDesc tableScanDesc = new TableScanDesc("test", table);
        tableScanDesc.setNeededColumns(neededColumns);
        tableScanDesc.setNeededColumnIDs(neededColumnIDs);
        tableScanDesc.setReferencedColumns(referenceColumns);

        RowSchema rowSchema = mock(RowSchema.class);
        when(rowSchema.getSignature()).thenReturn(signature);
        when(tableScanOperator.getSchema()).thenReturn(rowSchema);
        when(tableScanOperator.getConf()).thenReturn(tableScanDesc);

        OmniDataPredicate predicate = new OmniDataPredicate(tableScanOperator);
        PrimitiveTypeInfo typeInfo = new PrimitiveTypeInfo();
        typeInfo.setTypeName("bigint");
        int outputColumnId = 12;
        VectorExpression[] expressions = new VectorExpression[1];
        expressions[0] = new ConstantVectorExpression(outputColumnId, 1L, typeInfo);
        expressions[0].setOutputTypeInfo(typeInfo);
        expressions[0].setInputTypeInfos(typeInfo);
        VectorSelectDesc vectorSelectDesc = new VectorSelectDesc();
        vectorSelectDesc.setSelectExpressions(expressions);
        predicate.setSelectExpressions(vectorSelectDesc);
        predicate.addProjectionsByAgg(outputColumnId);
        if (predicate.getProjections().get(0) instanceof ConstantExpression) {
            Assert.assertEquals("1",
                    ((ConstantExpression) predicate.getProjections().get(0)).getValue().toString());
        }
    }
}