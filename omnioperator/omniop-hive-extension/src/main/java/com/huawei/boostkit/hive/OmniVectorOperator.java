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

package com.huawei.boostkit.hive;

import static com.huawei.boostkit.hive.cache.VectorCache.BATCH;
import static com.huawei.boostkit.hive.converter.VecConverter.CONVERTER_MAP;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;

import com.huawei.boostkit.hive.cache.VecBufferCache;
import com.huawei.boostkit.hive.cache.VectorCache;
import com.huawei.boostkit.hive.converter.StructConverter;
import com.huawei.boostkit.hive.converter.VecConverter;
import com.huawei.boostkit.hive.shuffle.OmniVecBatchSerDe;
import com.huawei.boostkit.hive.shuffle.VecSerdeBody;

import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.RecordSource;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyObjectInspectorParameters;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyObjectInspectorParametersImpl;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OmniVectorOperator extends OmniHiveOperator<OmniVectorDesc> {
    private boolean isToVector;

    private transient VecConverter[][] converters;

    public transient VectorCache[] vectorCache;

    public transient VecBufferCache[] vecBufferCaches;

    private transient boolean isKeyValue;

    private transient List<StructField>[] flatFields;

    private transient VecConverter[][] flatConverters;

    private transient RecordSource[] source;

    private transient boolean[] fetchDone;

    private int[] rowCount;

    private transient StructObjectInspector[] soi;
    private transient List<StructField>[] fields;
    private transient int[] keyFiledNum;

    private transient boolean isVecBatchSerDe;

    public OmniVectorOperator() {
        super();
    }

    public OmniVectorOperator(CompilationOpContext ctx) {
        super(ctx);
    }

    public OmniVectorOperator(CompilationOpContext ctx, OmniVectorDesc conf) {
        super(ctx);
        this.conf = conf;
        this.isToVector = conf.getIsToVector();
    }

    @Override
    protected void initializeOp(Configuration hconf) throws HiveException {
        super.initializeOp(hconf);
        if (parentOperators.isEmpty() || (parentOperators.get(0) instanceof LimitOperator && parentOperators.get(0).getParentOperators().isEmpty())) {
            isKeyValue = true;
        }
        converters = new VecConverter[inputObjInspectors.length][];
        vectorCache = new VectorCache[inputObjInspectors.length];
        vecBufferCaches = new VecBufferCache[inputObjInspectors.length];
        flatFields = new List[inputObjInspectors.length];
        flatConverters = new VecConverter[inputObjInspectors.length][];
        fetchDone = new boolean[inputObjInspectors.length];
        rowCount = new int[inputObjInspectors.length];
        soi = new StructObjectInspector[inputObjInspectors.length];
        fields = new List[inputObjInspectors.length];
        keyFiledNum = new int[inputObjInspectors.length];

        for (int i = 0; i < inputObjInspectors.length; i++) {
            soi[i] = (StructObjectInspector) inputObjInspectors[i];
        }
        if (isKeyValue && childOperators.get(0) instanceof OmniMergeJoinOperator) {
            source = ((TezContext) MapredContext.get()).getRecordSources();
        }
        ReduceWork reduceWork = Utilities.getReduceWork(hconf);
        if (reduceWork != null && reduceWork.getKeyDesc().getProperties().get(SERIALIZATION_LIB)
                .equals(OmniVecBatchSerDe.class.getName())) {
            isVecBatchSerDe = true;
        }
        for (int i = 0; i < soi.length; i++) {
            fields[i] = soi[i].getAllStructFieldRefs().stream().map(field -> (StructField) field)
                    .collect(Collectors.toList());
            converters[i] = fields[i].stream().map(field -> {
                if (field.getFieldObjectInspector() instanceof PrimitiveObjectInspector) {
                    PrimitiveTypeInfo primitiveTypeInfo = ((PrimitiveObjectInspector) field.getFieldObjectInspector())
                            .getTypeInfo();
                    return CONVERTER_MAP.get(primitiveTypeInfo.getPrimitiveCategory());
                } else {
                    return new StructConverter((StructObjectInspector) field.getFieldObjectInspector());
                }
            }).toArray(VecConverter[]::new);
            if (isKeyValue) {
                keyFiledNum[i] = ((StructObjectInspector) fields[i].get(0).getFieldObjectInspector())
                        .getAllStructFieldRefs().size();
                flatFields[i] = Arrays.stream(converters[i])
                        .flatMap(converter -> ((StructConverter) converter).getFields().stream())
                        .collect(Collectors.toList());
                flatConverters[i] = Arrays.stream(converters[i])
                        .flatMap(converter -> Arrays.stream(((StructConverter) converter).getConverters()))
                        .toArray(VecConverter[]::new);
            }
            if (isKeyValue) {
                if (isVecBatchSerDe) {
                    List<TypeInfo> typeInfos = flatFields[i].stream()
                            .map(field -> ((PrimitiveObjectInspector) field.getFieldObjectInspector()).getTypeInfo())
                            .collect(Collectors.toList());
                    vecBufferCaches[i] = new VecBufferCache(flatFields[i].size(), typeInfos);
                } else {
                    vectorCache[i] = new VectorCache(flatFields[i].size());
                }
            } else {
                vectorCache[i] = new VectorCache(fields[i].size());
            }
        }
        if (!this.childOperators.isEmpty() && childOperators.get(0) instanceof OmniMergeJoinOperator
                || childOperators.get(0) instanceof OmniMapJoinOperator && ((OmniMapJoinDesc) childOperators.get(0).getConf()).isDynamicPartitionHashJoin()) {
            List<String> fieldNames = new ArrayList<>();
            for (int i = 0; i < inputObjInspectors.length; i++) {
                fieldNames.add(String.valueOf(i));
            }
            outputObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                    Arrays.asList(soi));
        }
        if (!isToVector) {
            outputObjInspector = convertLazyToJavaInspector(fields[0]);
        }
    }

    public static StandardStructObjectInspector convertLazyToJavaInspector(
            List<? extends StructField> allStructFieldRefs) {
        List<String> structFieldNames = new ArrayList<>();
        List<ObjectInspector> structFieldObjectInspectors = new ArrayList<>();
        allStructFieldRefs.forEach(field -> {
            structFieldNames.add(field.getFieldName());
            if (!(field.getFieldObjectInspector() instanceof PrimitiveObjectInspector)) {
                return;
            }
            switch (((PrimitiveObjectInspector) field.getFieldObjectInspector()).getPrimitiveCategory()) {
                case VARCHAR:
                case CHAR:
                case STRING:
                    LazyObjectInspectorParameters lazyParam = new LazyObjectInspectorParametersImpl(false, (byte) 0,
                            false, null, null, null);
                    structFieldObjectInspectors.add(LazyPrimitiveObjectInspectorFactory.getLazyObjectInspector(
                            ((PrimitiveObjectInspector) field.getFieldObjectInspector()).getTypeInfo(), lazyParam));
                    break;
                case DECIMAL:
                    structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                            ((PrimitiveObjectInspector) field.getFieldObjectInspector()).getTypeInfo()));
                    break;
                default:
                    structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                            ((PrimitiveObjectInspector) field.getFieldObjectInspector()).getTypeInfo()));
            }
        });
        return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
    }

    private VecBatch getVecBatchFromRow(Object row) {
        if (row instanceof VecBatch) {
            return (VecBatch) row;
        }
        if (row instanceof List) {
            for (int i = 0; i < ((List<?>) row).size(); i++) {
                if (((List<?>) row).get(i) instanceof VecBatch) {
                    return (VecBatch) ((List<?>) row).get(i);
                }
            }
        }
        return null;
    }

    @Override
    public void process(Object row, int tag) throws HiveException {
        if (isToVector) {
            processToVector(row, tag);
        } else {
            processToOmni(row, tag);
        }
    }

    private void processToVector(Object row, int tag) throws HiveException {
        if (isVecBatchSerDe && isKeyValue) {
            List vecSerdeBodies = (List) row;
            VecSerdeBody[] key = (VecSerdeBody[]) vecSerdeBodies.get(0);
            VecSerdeBody[] value = (VecSerdeBody[]) vecSerdeBodies.get(1);
            dealVecBatchSerDeData(key, tag, 0);
            dealVecBatchSerDeData(value, tag, key != null ? key.length : 0);
        } else {
            for (int i = 0; i < fields[tag].size(); i++) {
                if (isKeyValue) {
                    Object structFieldData = soi[tag].getStructFieldData(row, fields[tag].get(i));
                    Object[] value = (Object[]) ((StructConverter) converters[tag][i]).calculateValue(structFieldData,
                            ((StructObjectInspector) fields[tag].get(i).getFieldObjectInspector()));
                    int offset = i == 0 ? 0 : ((StructConverter) converters[tag][0]).getFields().size();
                    for (int j = 0; j < value.length; j++) {
                        vectorCache[tag].dataCache[offset + j][rowCount[tag]] = value[j];
                    }
                } else {
                    Object structFieldData = soi[tag].getStructFieldData(row, fields[tag].get(i));
                    vectorCache[tag].dataCache[i][rowCount[tag]] = converters[tag][i].calculateValue(structFieldData,
                            ((PrimitiveObjectInspector) fields[tag].get(i).getFieldObjectInspector()).getTypeInfo());
                }
            }
        }
        rowCount[tag]++;
        if (rowCount[tag] < BATCH) {
            return;
        }
        forwardNext(tag);
        rowCount[tag] = 0;
    }

    private void dealVecBatchSerDeData(VecSerdeBody[] vecSerdeBodies, int tag, int offset) {
        if (vecSerdeBodies == null) {
            return;
        }
        vecBufferCaches[tag].addVecSerdeBody(vecSerdeBodies, rowCount[tag], offset);
    }

    private void processToOmni(Object row, int tag) throws HiveException {
        VecBatch vecBatchFromRow = getVecBatchFromRow(row);
        if (vecBatchFromRow == null) {
            throw new HiveException("isToVector is not right");
        }
        StructObjectInspector outputSoi = (StructObjectInspector) outputObjInspector;
        List<? extends StructField> outputFields = outputSoi.getAllStructFieldRefs();
        Vec[] vecs = vecBatchFromRow.getVectors();
        for (int j = 0; j < vecBatchFromRow.getRowCount(); j++) {
            List<Object> output = new ArrayList<>();
            for (int i = 0; i < vecs.length; i++) {
                ObjectInspector fieldObjectInspector = outputFields.get(i).getFieldObjectInspector();
                if (fieldObjectInspector instanceof PrimitiveObjectInspector) {
                    output.add(converters[tag][i].fromOmniVec(vecs[i], j,
                            ((PrimitiveObjectInspector) outputFields.get(i).getFieldObjectInspector())));
                }
            }
            forward(output.toArray(), null);
        }
        vecBatchFromRow.releaseAllVectors();
        vecBatchFromRow.close();
    }

    private void forwardNext(int tag) throws HiveException {
        if (isVecBatchSerDe && isKeyValue) {
            this.forward(new VecBatch(vecBufferCaches[tag].getValueVecBatchCache(rowCount[tag]), rowCount[tag]), tag);
            return;
        }
        Vec[] vecs = new Vec[isKeyValue ? flatFields[tag].size() : fields[tag].size()];
        if (isKeyValue) {
            IntStream.range(0, this.flatFields[tag].size()).forEach(i -> {
                vecs[i] = flatConverters[tag][i].toOmniVec(
                        Arrays.copyOfRange(vectorCache[tag].dataCache[i], 0, rowCount[tag]), rowCount[tag],
                        ((PrimitiveObjectInspector) flatFields[tag].get(i).getFieldObjectInspector()).getTypeInfo());
            });
        } else {
            IntStream.range(0, fields[tag].size()).forEach(i -> {
                vecs[i] = converters[tag][i].toOmniVec(
                        Arrays.copyOfRange(vectorCache[tag].dataCache[i], 0, rowCount[tag]), rowCount[tag],
                        ((PrimitiveObjectInspector) fields[tag].get(i).getFieldObjectInspector()).getTypeInfo());
            });
        }
        VecBatch vecBatch = new VecBatch(vecs, this.rowCount[tag]);
        this.forward(vecBatch, tag);
    }

    @Override
    public String getName() {
        return OmniVectorOperator.getOperatorName();
    }

    public static String getOperatorName() {
        return "OMNI_VEC";
    }

    @Override
    public OperatorType getType() {
        return null;
    }

    public boolean isKeyValue() {
        return isKeyValue;
    }

    public int[] getRowCount() {
        return rowCount;
    }

    @Override
    public void close(boolean abort) throws HiveException {
        // here to process the remaining data in the cache
        if (source != null) {
            for (int i = 0; i < source.length; i++) {
                if (i != ((OmniMergeJoinOperator) childOperators.get(0)).getPosBigTable()) {
                    fetchDone[i] = ((OmniMergeJoinOperator) childOperators.get(0)).getFetchDone()[i];
                    while (!fetchDone[i]) {
                        fetchDone[i] = !source[i].pushRecord();
                    }
                    ((OmniMergeJoinOperator) childOperators.get(0)).getFetchDone()[i] = fetchDone[i];
                }
            }
        }
        for (int i = 0; i < rowCount.length; i++) {
            if (rowCount[i] > 0) {
                forwardNext(i);
                rowCount[i] = 0;
            }
        }
        super.close(abort);
    }

    public void pushRestData(int tag) throws HiveException {
        if (rowCount[tag] > 0) {
            forwardNext(tag);
            rowCount[tag] = 0;
        }
    }
}