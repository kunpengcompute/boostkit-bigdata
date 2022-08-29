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

package org.apache.hadoop.hive.ql.omnidata.physical;

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.binaryTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.booleanTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.byteTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.dateTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.doubleTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.floatTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.intTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.longTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.shortTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.stringTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.timestampTypeInfo;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.junit.Test;

import java.util.ArrayList;

/**
 * Test for Ndp Vectorized row batch context.
 *
 * @since 2022-08-23
 */
public class TestNdpVectorizedRowBatchCtx {
    private NdpVectorizedRowBatchCtx makeBatchCtx() {
        DataTypePhysicalVariation[] rowDataTypePhysicalVariations = new DataTypePhysicalVariation[] {
                DataTypePhysicalVariation.NONE, DataTypePhysicalVariation.NONE, DataTypePhysicalVariation.NONE,
                DataTypePhysicalVariation.NONE
        };
        DataTypePhysicalVariation[] scratchDataTypePhysicalVariations = new DataTypePhysicalVariation[] {
                DataTypePhysicalVariation.NONE, DataTypePhysicalVariation.NONE, DataTypePhysicalVariation.NONE
        };
        return new NdpVectorizedRowBatchCtx(new String[] {"col1", "col2", "col3", "virtual"},
                rowDataTypePhysicalVariations, new int[] {0, 1, 2}, 3, 0, 1, new VirtualColumn[] {},
                new String[] {"int", "int", "int"}, scratchDataTypePhysicalVariations);
    }

    private NdpVectorizedRowBatchCtx makeBatchCtxByPartition() {
        DataTypePhysicalVariation[] rowDataTypePhysicalVariations = new DataTypePhysicalVariation[] {
                DataTypePhysicalVariation.NONE, DataTypePhysicalVariation.NONE, DataTypePhysicalVariation.NONE,
                DataTypePhysicalVariation.NONE, DataTypePhysicalVariation.NONE, DataTypePhysicalVariation.NONE,
                DataTypePhysicalVariation.NONE, DataTypePhysicalVariation.NONE, DataTypePhysicalVariation.NONE,
                DataTypePhysicalVariation.NONE, DataTypePhysicalVariation.NONE, DataTypePhysicalVariation.NONE,
                DataTypePhysicalVariation.NONE, DataTypePhysicalVariation.NONE
        };
        return new NdpVectorizedRowBatchCtx(new String[] {
                "col1", "col2", "col3", "par1", "par2", "par3", "par4", "par5", "par6", "par7", "par8", "par9", "par10",
                "par11"
        }, rowDataTypePhysicalVariations, new int[] {0, 1, 2}, 3, 11, 0, new VirtualColumn[] {}, new String[] {},
                new DataTypePhysicalVariation[] {});
    }

    private StructTypeInfo makeStructTypeInfo() {
        StructTypeInfo structTypeInfo = new StructTypeInfo();
        ArrayList<TypeInfo> typeInfos = new ArrayList<>();
        typeInfos.add(longTypeInfo);
        typeInfos.add(intTypeInfo);
        typeInfos.add(longTypeInfo);
        ArrayList<String> names = new ArrayList<>();
        names.add("writeid");
        names.add("bucketid");
        names.add("rowid");
        structTypeInfo.setAllStructFieldTypeInfos(typeInfos);
        structTypeInfo.setAllStructFieldNames(names);
        return structTypeInfo;
    }

    @Test
    public void testVectorizedRowBatchCreate() {
        // assert NdpVectorizedRowBatchCtx
        NdpVectorizedRowBatchCtx ndpCtx = makeBatchCtx();
        assertEquals(ndpCtx.getRowColumnNames().length, 4);
        assertEquals(ndpCtx.getRowDataTypePhysicalVariations().length, 4);
        assertEquals(ndpCtx.getDataColumnNums().length, 3);
        assertEquals(ndpCtx.getVirtualColumnCount(), 1);
        assertEquals(ndpCtx.getNeededVirtualColumns().length, 0);
        assertEquals(ndpCtx.getScratchColumnTypeNames().length, 3);
        assertEquals(ndpCtx.getScratchDataTypePhysicalVariations().length, 3);
        // assert VectorizedRowBatch
        StructTypeInfo structTypeInfo = makeStructTypeInfo();
        TypeInfo[] rowColumnTypeInfos = new TypeInfo[] {intTypeInfo, intTypeInfo, intTypeInfo, structTypeInfo};
        VectorizedRowBatch rowBatch = ndpCtx.createVectorizedRowBatch(rowColumnTypeInfos);
        assertEquals(rowBatch.numCols, 7);
        assertEquals(rowBatch.getDataColumnCount(), ndpCtx.getDataColumnCount());
        assertEquals(rowBatch.projectionSize, 7);
    }

    @Test
    public void testAddPartitionColsToBatch() {
        NdpVectorizedRowBatchCtx ndpCtx = makeBatchCtxByPartition();
        TypeInfo[] rowColumnTypeInfos = new TypeInfo[] {
                intTypeInfo, intTypeInfo, intTypeInfo, booleanTypeInfo, shortTypeInfo, intTypeInfo, longTypeInfo,
                stringTypeInfo, floatTypeInfo, doubleTypeInfo, byteTypeInfo, dateTypeInfo, timestampTypeInfo, binaryTypeInfo
        };
        VectorizedRowBatch rowBatch = ndpCtx.createVectorizedRowBatch(rowColumnTypeInfos);
        Object[] partitionValues = new Object[] {
                true, (short) 2, 3, 4L, "5", 1.1f, 2.2d, null, null, null, null
        };
        ndpCtx.addPartitionColsToBatch(rowBatch, partitionValues, rowColumnTypeInfos);
        assertEquals(rowBatch.numCols, 14);
        assertEquals(rowBatch.getPartitionColumnCount(), 11);
        if (rowBatch.cols[3] instanceof LongColumnVector && rowBatch.cols[4] instanceof LongColumnVector
                && rowBatch.cols[5] instanceof LongColumnVector && rowBatch.cols[6] instanceof LongColumnVector) {
            assertEquals(((LongColumnVector) rowBatch.cols[3]).vector[0], 1L);
            assertEquals(((LongColumnVector) rowBatch.cols[4]).vector[0], 2L);
            assertEquals(((LongColumnVector) rowBatch.cols[5]).vector[0], 3L);
            assertEquals(((LongColumnVector) rowBatch.cols[6]).vector[0], 4L);
        }
    }
}