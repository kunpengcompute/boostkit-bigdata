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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.IntervalDayTimeColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.IOPrepareCache;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.FileSplit;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Context for Ndp Vectorized row batch.
 *
 * @since 2022-05-28
 */
public class NdpVectorizedRowBatchCtx implements Serializable {

    private static final long serialVersionUID = 1L;

    private String[] rowColumnNames;

    private DataTypePhysicalVariation[] rowDataTypePhysicalVariations;

    private int[] dataColumnNums;

    private int dataColumnCount;

    private int partitionColumnCount;

    private int virtualColumnCount;

    private VirtualColumn[] neededVirtualColumns;

    private String[] scratchColumnTypeNames;

    private DataTypePhysicalVariation[] scratchDataTypePhysicalVariations;

    /**
     * Constructor for VectorizedRowBatchCtx
     */
    public NdpVectorizedRowBatchCtx() {
    }

    public NdpVectorizedRowBatchCtx(VectorizedRowBatchCtx vectorizedRowBatchCtx) {
        this(vectorizedRowBatchCtx.getRowColumnNames(), vectorizedRowBatchCtx.getRowdataTypePhysicalVariations(),
                vectorizedRowBatchCtx.getDataColumnNums(), vectorizedRowBatchCtx.getDataColumnCount(),
                vectorizedRowBatchCtx.getPartitionColumnCount(), vectorizedRowBatchCtx.getVirtualColumnCount(),
                vectorizedRowBatchCtx.getNeededVirtualColumns(), vectorizedRowBatchCtx.getScratchColumnTypeNames(),
                vectorizedRowBatchCtx.getScratchDataTypePhysicalVariations());
    }

    @JsonCreator
    public NdpVectorizedRowBatchCtx(@JsonProperty("rowColumnNames") String[] rowColumnNames,
                                    @JsonProperty("rowDataTypePhysicalVariations") DataTypePhysicalVariation[] rowDataTypePhysicalVariations,
                                    @JsonProperty("dataColumnNums") int[] dataColumnNums, @JsonProperty("dataColumnCount") int dataColumnCount,
                                    @JsonProperty("partitionColumnCount") int partitionColumnCount,
                                    @JsonProperty("virtualColumnCount") int virtualColumnCount,
                                    @JsonProperty("neededVirtualColumns") VirtualColumn[] neededVirtualColumns,
                                    @JsonProperty("scratchColumnTypeNames") String[] scratchColumnTypeNames,
                                    @JsonProperty("scratchDataTypePhysicalVariations")
                                            DataTypePhysicalVariation[] scratchDataTypePhysicalVariations) {
        this.rowColumnNames = rowColumnNames;
        this.rowDataTypePhysicalVariations = rowDataTypePhysicalVariations;
        this.dataColumnNums = dataColumnNums;
        this.dataColumnCount = dataColumnCount;
        this.partitionColumnCount = partitionColumnCount;
        this.virtualColumnCount = virtualColumnCount;
        this.neededVirtualColumns = neededVirtualColumns;
        this.scratchColumnTypeNames = scratchColumnTypeNames;
        this.scratchDataTypePhysicalVariations = scratchDataTypePhysicalVariations;
    }

    private ColumnVector createColumnVectorFromRowColumnTypeInfos(int columnNum, TypeInfo[] rowColumnTypeInfos) {
        TypeInfo typeInfo = rowColumnTypeInfos[columnNum];
        DataTypePhysicalVariation dataTypePhysicalVariation = DataTypePhysicalVariation.NONE;
        if (rowDataTypePhysicalVariations != null) {
            dataTypePhysicalVariation = rowDataTypePhysicalVariations[columnNum];
        }
        return VectorizedBatchUtil.createColumnVector(typeInfo, dataTypePhysicalVariation);
    }

    public boolean isVirtualColumnNeeded(String virtualColumnName) {
        for (VirtualColumn neededVirtualColumn : neededVirtualColumns) {
            if (neededVirtualColumn.getName().equals(virtualColumnName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Creates a Vectorized row batch and the column vectors.
     *
     * @return VectorizedRowBatch
     */
    public VectorizedRowBatch createVectorizedRowBatch(TypeInfo[] rowColumnTypeInfos) {
        final int nonScratchColumnCount = rowColumnTypeInfos.length;
        final int totalColumnCount = nonScratchColumnCount + scratchColumnTypeNames.length;
        VectorizedRowBatch result = new VectorizedRowBatch(totalColumnCount);

        if (dataColumnNums == null) {
            // All data and partition columns.
            for (int i = 0; i < nonScratchColumnCount; i++) {
                result.cols[i] = createColumnVectorFromRowColumnTypeInfos(i, rowColumnTypeInfos);
            }
        } else {
            // Create only needed/included columns data columns.
            for (int i = 0; i < dataColumnNums.length; i++) {
                int columnNum = dataColumnNums[i];
                Preconditions.checkState(columnNum < nonScratchColumnCount);
                result.cols[columnNum] = createColumnVectorFromRowColumnTypeInfos(columnNum, rowColumnTypeInfos);
            }
            // Always create partition and virtual columns.
            final int partitionEndColumnNum = dataColumnCount + partitionColumnCount;
            for (int partitionColumnNum = dataColumnCount;
                 partitionColumnNum < partitionEndColumnNum; partitionColumnNum++) {
                result.cols[partitionColumnNum] = VectorizedBatchUtil.createColumnVector(
                        rowColumnTypeInfos[partitionColumnNum]);
            }
            final int virtualEndColumnNum = partitionEndColumnNum + virtualColumnCount;
            for (int virtualColumnNum = partitionEndColumnNum;
                 virtualColumnNum < virtualEndColumnNum; virtualColumnNum++) {
                String virtualColumnName = rowColumnNames[virtualColumnNum];
                if (!isVirtualColumnNeeded(virtualColumnName)) {
                    continue;
                }
                result.cols[virtualColumnNum] = VectorizedBatchUtil.createColumnVector(
                        rowColumnTypeInfos[virtualColumnNum]);
            }
        }

        for (int i = 0; i < scratchColumnTypeNames.length; i++) {
            String typeName = scratchColumnTypeNames[i];
            DataTypePhysicalVariation dataTypePhysicalVariation = scratchDataTypePhysicalVariations[i];
            result.cols[nonScratchColumnCount + i] = VectorizedBatchUtil.createColumnVector(typeName,
                    dataTypePhysicalVariation);
        }

        // UNDONE: Also remember virtualColumnCount...
        result.setPartitionInfo(dataColumnCount, partitionColumnCount);

        result.reset();
        return result;
    }

    public static void getPartitionValues(NdpVectorizedRowBatchCtx vrbCtx, Configuration hiveConf, FileSplit split,
                                          Object[] partitionValues, TypeInfo[] rowColumnTypeInfos) {
        MapWork mapWork = Utilities.getMapWork(hiveConf);
        getPartitionValues(vrbCtx, mapWork, split, partitionValues, rowColumnTypeInfos);
    }

    public static void getPartitionValues(NdpVectorizedRowBatchCtx vrbCtx, MapWork mapWork, FileSplit split,
                                          Object[] partitionValues, TypeInfo[] rowColumnTypeInfos) {
        Map<Path, PartitionDesc> pathToPartitionInfo = mapWork.getPathToPartitionInfo();
        try {
            PartitionDesc partDesc = HiveFileFormatUtils.getFromPathRecursively(pathToPartitionInfo, split.getPath(),
                    IOPrepareCache.get().getPartitionDescMap());
            getPartitionValues(vrbCtx, partDesc, partitionValues, rowColumnTypeInfos);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getPartitionValues(NdpVectorizedRowBatchCtx vrbCtx, PartitionDesc partDesc,
                                          Object[] partitionValues, TypeInfo[] rowColumnTypeInfos) {

        LinkedHashMap<String, String> partSpec = partDesc.getPartSpec();

        for (int i = 0; i < vrbCtx.partitionColumnCount; i++) {
            Object objectValue;
            if (partSpec == null) {
                // For partition-less table, initialize partValue to empty string.
                // We can have partition-less table even if we have partition keys
                // when there is only only partition selected and the partition key is not
                // part of the projection/include list.
                objectValue = null;
            } else {
                String key = vrbCtx.rowColumnNames[vrbCtx.dataColumnCount + i];

                // Create a Standard java object Inspector
                TypeInfo partColTypeInfo = rowColumnTypeInfos[vrbCtx.dataColumnCount + i];
                ObjectInspector objectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
                        partColTypeInfo);
                objectValue = ObjectInspectorConverters.
                        getConverter(PrimitiveObjectInspectorFactory.
                                javaStringObjectInspector, objectInspector).
                        convert(partSpec.get(key));
                if (partColTypeInfo instanceof CharTypeInfo) {
                    objectValue = ((HiveChar) objectValue).getStrippedValue();
                }
            }
            partitionValues[i] = objectValue;
        }
    }

    public void addPartitionColsToBatch(VectorizedRowBatch batch, Object[] partitionValues,
                                        TypeInfo[] rowColumnTypeInfos) {
        addPartitionColsToBatch(batch.cols, partitionValues, rowColumnTypeInfos);
    }

    public void addPartitionColsToBatch(ColumnVector[] cols, Object[] partitionValues, TypeInfo[] rowColumnTypeInfos) {
        if (partitionValues != null) {
            for (int i = 0; i < partitionColumnCount; i++) {
                Object value = partitionValues[i];

                int colIndex = dataColumnCount + i;
                String partitionColumnName = rowColumnNames[colIndex];
                PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) rowColumnTypeInfos[colIndex];
                switch (primitiveTypeInfo.getPrimitiveCategory()) {
                    case BOOLEAN: {
                        LongColumnVector lcv = (LongColumnVector) cols[colIndex];
                        if (value == null) {
                            setNull(lcv);
                        } else {
                            if (value instanceof Boolean) {
                                lcv.fill((Boolean) value ? 1 : 0);
                            }
                        }
                    }
                    break;
                    case BYTE: {
                        LongColumnVector lcv = (LongColumnVector) cols[colIndex];
                        if (value == null) {
                            setNull(lcv);
                        } else {
                            lcv.fill((Byte) value);
                        }
                    }
                    break;
                    case SHORT: {
                        LongColumnVector lcv = (LongColumnVector) cols[colIndex];
                        if (value == null) {
                            setNull(lcv);
                        } else {
                            lcv.fill((Short) value);
                        }
                    }
                    break;
                    case INT: {
                        LongColumnVector lcv = (LongColumnVector) cols[colIndex];
                        if (value == null) {
                            setNull(lcv);
                        } else {
                            lcv.fill((Integer) value);
                        }
                    }
                    break;
                    case LONG: {
                        LongColumnVector lcv = (LongColumnVector) cols[colIndex];
                        if (value == null) {
                            setNull(lcv);
                        } else {
                            lcv.fill((Long) value);
                        }
                    }
                    break;
                    case DATE: {
                        LongColumnVector lcv = (LongColumnVector) cols[colIndex];
                        if (value == null) {
                            setNull(lcv);
                        } else {
                            lcv.fill(DateWritableV2.dateToDays((Date) value));
                        }
                    }
                    break;
                    case TIMESTAMP: {
                        TimestampColumnVector lcv = (TimestampColumnVector) cols[colIndex];
                        if (value == null) {
                            setNull(lcv);
                        } else {
                            lcv.fill(((Timestamp) value).toSqlTimestamp());
                        }
                    }
                    break;
                    case INTERVAL_YEAR_MONTH: {
                        LongColumnVector lcv = (LongColumnVector) cols[colIndex];
                        if (value == null) {
                            setNull(lcv);
                        } else {
                            lcv.fill(((HiveIntervalYearMonth) value).getTotalMonths());
                        }
                    }
                    case INTERVAL_DAY_TIME: {
                        IntervalDayTimeColumnVector icv = (IntervalDayTimeColumnVector) cols[colIndex];
                        if (value == null) {
                            setNull(icv);
                        } else {
                            icv.fill(((HiveIntervalDayTime) value));
                        }
                    }
                    case FLOAT: {
                        DoubleColumnVector dcv = (DoubleColumnVector) cols[colIndex];
                        if (value == null) {
                            setNull(dcv);
                        } else {
                            dcv.fill((Float) value);
                        }
                    }
                    break;
                    case DOUBLE: {
                        DoubleColumnVector dcv = (DoubleColumnVector) cols[colIndex];
                        if (value == null) {
                            setNull(dcv);
                        } else {
                            dcv.fill((Double) value);
                        }
                    }
                    break;
                    case DECIMAL: {
                        DecimalColumnVector dv = (DecimalColumnVector) cols[colIndex];
                        if (value == null) {
                            setNull(dv);
                        } else {
                            dv.fill((HiveDecimal) value);
                        }
                    }
                    break;
                    case BINARY: {
                        BytesColumnVector bcv = (BytesColumnVector) cols[colIndex];
                        byte[] bytes = (byte[]) value;
                        if (bytes == null) {
                            setNull(bcv);
                        } else {
                            bcv.fill(bytes);
                        }
                    }
                    break;
                    case STRING:
                    case CHAR:
                    case VARCHAR: {
                        BytesColumnVector bcv = (BytesColumnVector) cols[colIndex];
                        String sVal = value.toString();
                        if (sVal == null) {
                            setNull(bcv);
                        } else {
                            bcv.fill(sVal.getBytes());
                        }
                    }
                    break;
                    default:
                        throw new RuntimeException(
                                "Unable to recognize the partition type " + primitiveTypeInfo.getPrimitiveCategory()
                                        + " for column " + partitionColumnName);
                }
            }
        }
    }

    private void setNull(ColumnVector cv) {
        cv.noNulls = false;
        cv.isNull[0] = true;
        cv.isRepeating = true;
    }

    @JsonProperty
    public String[] getRowColumnNames() {
        return rowColumnNames;
    }

    @JsonProperty
    public DataTypePhysicalVariation[] getRowDataTypePhysicalVariations() {
        return rowDataTypePhysicalVariations;
    }

    @JsonProperty
    public int[] getDataColumnNums() {
        return dataColumnNums;
    }

    @JsonProperty
    public int getDataColumnCount() {
        return dataColumnCount;
    }

    @JsonProperty
    public int getPartitionColumnCount() {
        return partitionColumnCount;
    }

    @JsonProperty
    public int getVirtualColumnCount() {
        return virtualColumnCount;
    }

    @JsonProperty
    public VirtualColumn[] getNeededVirtualColumns() {
        return neededVirtualColumns;
    }

    @JsonProperty
    public String[] getScratchColumnTypeNames() {
        return scratchColumnTypeNames;
    }

    @JsonProperty
    public DataTypePhysicalVariation[] getScratchDataTypePhysicalVariations() {
        return scratchDataTypePhysicalVariations;
    }
}