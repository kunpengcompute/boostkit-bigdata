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

import static com.huawei.boostkit.hive.JoinUtils.getExprFromExprNode;
import static com.huawei.boostkit.hive.JoinUtils.getExprNodeColumnEvaluator;
import static com.huawei.boostkit.hive.JoinUtils.getTypeFromInspectors;
import static com.huawei.boostkit.hive.cache.VectorCache.BATCH;
import static com.huawei.boostkit.hive.converter.VecConverter.CONVERTER_MAP;

import com.huawei.boostkit.hive.cache.VecBufferCache;
import com.huawei.boostkit.hive.converter.VecConverter;
import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;
import com.huawei.boostkit.hive.shuffle.OmniVecBatchSerDe;
import com.huawei.boostkit.hive.shuffle.VecSerdeBody;

import nova.hetu.omniruntime.constants.JoinType;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.operator.config.OverflowConfig;
import nova.hetu.omniruntime.operator.config.SpillConfig;
import nova.hetu.omniruntime.operator.filter.OmniFilterAndProjectOperatorFactory;
import nova.hetu.omniruntime.operator.join.OmniHashBuilderWithExprOperatorFactory;
import nova.hetu.omniruntime.operator.join.OmniLookupJoinWithExprOperatorFactory;
import nova.hetu.omniruntime.operator.join.OmniLookupOuterJoinWithExprOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.AbstractMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ExprNodeColumnEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinKey;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinKeyObject;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectSerDeContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContextRegion;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveWritableObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hive.common.util.ReflectionUtil;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class OmniMapJoinOperator extends AbstractMapJoinOperator<MapJoinDesc>
        implements Serializable, VectorizationContextRegion {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(OmniMapJoinOperator.class.getName());

    public static final Map<Integer, JoinType> JOIN_TYPE_MAP = new HashMap<Integer, JoinType>() {
        {
            put(JoinDesc.INNER_JOIN, JoinType.OMNI_JOIN_TYPE_INNER);
            put(JoinDesc.LEFT_OUTER_JOIN, JoinType.OMNI_JOIN_TYPE_LEFT);
            put(JoinDesc.RIGHT_OUTER_JOIN, JoinType.OMNI_JOIN_TYPE_RIGHT);
            put(JoinDesc.FULL_OUTER_JOIN, JoinType.OMNI_JOIN_TYPE_FULL);
            put(JoinDesc.LEFT_SEMI_JOIN, JoinType.OMNI_JOIN_TYPE_LEFT_SEMI);
        }
    };
    private transient OmniOperatorFactory omniLookupJoinWithExprOperatorFactory;
    private transient OmniHashBuilderWithExprOperatorFactory omniHashBuilderWithExprOperatorFactory;

    private transient OmniOperator joinOperator;

    private transient OmniOperator buildOperator;

    private transient List<ObjectInspector>[] buildInspectors;

    private transient int[] order;

    private transient List<Integer> buildIndexes;

    private transient Map<Integer, Integer>[] valuePosToKeyPos;

    private boolean changedCtx;

    private VectorizationContext vectorizationContext;

    private transient List<VecBatch>[] buildVecs;

    private transient String cacheKey;
    private transient MapJoinTableContainerSerDe[] mapJoinTableSerdes;
    private transient ObjectCache cache;
    private static List<List<VecBatch>> vecBatchNeedClose = new ArrayList<>();
    private static boolean addedCloseThread;
    private transient Iterator<VecBatch> output;

    private transient OmniFilterAndProjectOperatorFactory filterAndProjectOperatorFactory;

    private transient OmniOperator filterOperator;

    public OmniMapJoinOperator() {
        super();
    }

    public OmniMapJoinOperator(CompilationOpContext ctx) {
        super(ctx);
    }

    public OmniMapJoinOperator(AbstractMapJoinOperator<? extends MapJoinDesc> mjop, MapJoinDesc mapJoinDesc) {
        super(mjop);
        this.conf = new OmniMapJoinDesc(mapJoinDesc);
        this.changedCtx = false;
        this.vectorizationContext = null;
    }

    public OmniMapJoinOperator(AbstractMapJoinOperator<? extends MapJoinDesc> mjop, MapJoinDesc mapJoinDesc,
                               boolean changedCtx, VectorizationContext vectorizationContext) {
        super(mjop);
        this.conf = new OmniMapJoinDesc(mapJoinDesc);
        this.changedCtx = changedCtx;
        this.vectorizationContext = vectorizationContext;
    }

    @Override
    protected void initializeOp(Configuration hconf) throws HiveException {
        if (conf.isDynamicPartitionHashJoin()) {
            generateNewInputObjInspectors();
        }
        super.initializeOp(hconf);
        if (conf.isDynamicPartitionHashJoin() && vectorizationContext == null) {
            rebuildUnvectorizedDynaicJoinInspector();
        }
        // initialize buildOperator
        buildIndexes = new ArrayList<>();
        buildInspectors = new ArrayList[joinKeysObjectInspectors.length];
        buildVecs = new ArrayList[joinKeysObjectInspectors.length];
        for (int buildIndex = 0; buildIndex < joinValuesObjectInspectors.length; buildIndex++) {
            if (buildIndex == posBigTable) {
                continue;
            }
            buildIndexes.add(buildIndex);
            buildInspectors[buildIndex] = new ArrayList<>(joinKeysObjectInspectors[buildIndex]);
            buildInspectors[buildIndex].addAll(joinValuesObjectInspectors[buildIndex]);
        }
        DataType[] buildTypes = getTypeFromInspectors(Arrays.stream(buildInspectors).filter(Objects::nonNull)
                .flatMap(List::stream).collect(Collectors.toList()));
        omniHashBuilderWithExprOperatorFactory = getOmniHashBuilderWithExprOperatorFactory(buildTypes,
                buildIndexes.get(buildIndexes.size() - 1));
        buildOperator = omniHashBuilderWithExprOperatorFactory.createOperator();
        omniLookupJoinWithExprOperatorFactory = getOmniLookupOperatorFactory(omniHashBuilderWithExprOperatorFactory,
                posBigTable, buildIndexes, buildTypes);
        joinOperator = omniLookupJoinWithExprOperatorFactory.createOperator();
        order = getOrder(posBigTable, joinKeysObjectInspectors.length);
        initKeyPosToValuePos();
        String queryId = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVEQUERYID);
        cacheKey = "HASH_MAP_" + this.getOperatorId() + "_container";
        cache = ObjectCacheFactory.getCache(hconf, queryId, false);
        final ExecMapperContext mapContext = getExecContext();
        final MapredContext mrContext = MapredContext.get();
        mapJoinTableSerdes = new MapJoinTableContainerSerDe[conf.getTagLength()];
        generateMapMetaData();
        if (!conf.isBucketMapJoin() && !conf.isDynamicPartitionHashJoin()) {
            Future<List<List<VecBatch>>> future = cache.retrieveAsync(cacheKey,
                    () -> loadBuildVec(mapContext, mrContext));
            asyncInitOperations.add(future);
        } else if (!isInputFileChangeSensitive(mapContext)) {
            List<List<VecBatch>> vecBatchList = loadBuildVec(mapContext, mrContext);
            for (int i = 0; i < vecBatchList.size(); i++) {
                buildVecs[buildIndexes.get(i)] = vecBatchList.get(i);
            }
        }
        if (conf.getResidualFilterExprs() != null && !conf.getResidualFilterExprs().isEmpty()) {
            BaseExpression root = ExpressionUtils.build((ExprNodeGenericFuncDesc) conf.getResidualFilterExprs().get(0),
                    outputObjInspector);
            List<? extends StructField> allStructFieldRefs = ((StructObjectInspector) outputObjInspector)
                    .getAllStructFieldRefs();
            DataType[] inputTypes = new DataType[allStructFieldRefs.size()];
            String[] projections = new String[allStructFieldRefs.size()];
            for (int i = 0; i < allStructFieldRefs.size(); i++) {
                if (allStructFieldRefs.get(i).getFieldObjectInspector() instanceof PrimitiveObjectInspector) {
                    PrimitiveTypeInfo typeInfo = ((PrimitiveObjectInspector) allStructFieldRefs.get(i)
                            .getFieldObjectInspector()).getTypeInfo();
                    int omniType = TypeUtils.convertHiveTypeToOmniType(typeInfo);
                    inputTypes[i] = TypeUtils.buildInputDataType(typeInfo);
                    projections[i] = TypeUtils.buildExpression(typeInfo, omniType, i);
                }
            }
            this.filterAndProjectOperatorFactory = new OmniFilterAndProjectOperatorFactory(root.toString(), inputTypes,
                    Arrays.asList(projections), 1, new OperatorConfig(SpillConfig.NONE, new OverflowConfig(), true));
            this.filterOperator = this.filterAndProjectOperatorFactory.createOperator();
        }
        if (!addedCloseThread) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    for (List<VecBatch> vecBatchList : vecBatchNeedClose) {
                        if (vecBatchList != null) {
                            for (VecBatch vecBatch : vecBatchList) {
                                vecBatch.releaseAllVectors();
                                vecBatch.close();
                            }
                        }
                    }
                    LOG.info("release vecBatchNeedClose succeed");
                } catch (Exception e) {
                    LOG.error("release vecBatchNeedClose failed", e);
                }
            }));
            addedCloseThread = true;
        }
    }

    private void rebuildUnvectorizedDynaicJoinInspector() throws HiveException {
        for (int i = 0; i < inputObjInspectors.length; i++) {
            StructObjectInspector structObjectInspector = (StructObjectInspector) inputObjInspectors[i];
            inputObjInspectors[i] = Utilities.constructVectorizedReduceRowOI(
                    (StructObjectInspector) structObjectInspector.getAllStructFieldRefs().get(0)
                            .getFieldObjectInspector(),
                    (StructObjectInspector) structObjectInspector.getAllStructFieldRefs().get(1)
                            .getFieldObjectInspector());
        }
    }

    private void generateNewInputObjInspectors() {
        ObjectInspector[] newInputObjInspectors = new ObjectInspector[conf.getTagOrder().length];
        for (int i = 0; i < conf.getTagOrder().length; i++) {
            newInputObjInspectors[i] = ((StructObjectInspector) inputObjInspectors[0]).getAllStructFieldRefs().get(i)
                    .getFieldObjectInspector();
        }
        inputObjInspectors = newInputObjInspectors;
    }

    private List<List<VecBatch>> loadBuildVec(ExecMapperContext mapContext, MapredContext mrContext)
            throws HiveException {
        List<List<VecBatch>> vecBatches = new ArrayList<>();
        for (int index : buildIndexes) {
            vecBatches.add(getVectorFromMap(index, mapContext, mrContext));
        }
        if (!conf.isDynamicPartitionHashJoin()) {
            LOG.info("add vecBatchNeedClose succeed");
            vecBatchNeedClose.addAll(vecBatches);
        }
        return vecBatches;
    }

    private boolean isInputFileChangeSensitive(ExecMapperContext mapContext) {
        return !(mapContext == null || mapContext.getLocalWork() == null
                || mapContext.getLocalWork().getInputFileChangeSensitive() == false);
    }

    private List<VecBatch> getVectorFromMap(int pos, ExecMapperContext mapContext, MapredContext mrContext)
            throws HiveException {
        if (mapJoinTableSerdes[pos].getValueContext().getSerDe() instanceof OmniVecBatchSerDe) {
            return getVectorFromMapVecBatchValue(pos, mapContext, mrContext);
        } else {
            return getVectorFromMapLazyBinaryValue(pos, mapContext, mrContext);
        }
    }

    private List<VecBatch> getVectorFromMapLazyBinaryValue(int pos, ExecMapperContext mapContext,
                                                           MapredContext mrContext) throws HiveException {
        Map<Integer, String> parentToInput = this.getConf().getParentToInput();
        List<VecBatch> buildVecBatches = new ArrayList<>();
        Object[][] values = new Object[buildInspectors[pos].size()][BATCH];
        VecConverter[] buildConverters = buildInspectors[pos].stream()
                .map(inspector -> CONVERTER_MAP
                        .get(((AbstractPrimitiveWritableObjectInspector) inspector).getPrimitiveCategory()))
                .toArray(VecConverter[]::new);
        PrimitiveTypeInfo[] buildTypeInfos = buildInspectors[pos].stream()
                .map(inspector -> ((AbstractPrimitiveWritableObjectInspector) inspector).getTypeInfo())
                .toArray(PrimitiveTypeInfo[]::new);
        try {
            TezContext tezContext = (TezContext) mrContext;
            String inputName = parentToInput.get(pos);
            LogicalInput input = tezContext.getInput(inputName);
            input.start();
            tezContext.getTezProcessorContext().waitForAnyInputReady(Collections.<Input>singletonList(input));
            Reader kvReader = input.getReader();
            OmniReaderWrapper omniReaderWrapper = new OmniReaderWrapper(kvReader);
            MapJoinObjectSerDeContext keyCtx = mapJoinTableSerdes[pos].getKeyContext();
            MapJoinObjectSerDeContext valCtx = mapJoinTableSerdes[pos].getValueContext();
            ByteStream.Output output = new ByteStream.Output(0);
            AbstractSerDe serde = valCtx.getSerDe();
            int rowLength = ObjectInspectorUtils.getStructSize(serde.getObjectInspector());
            // if output includes key and key doesn't have expression, then the value of
            // hashMapWrapper will not contain key.
            boolean needAddKey = rowLength < joinValuesObjectInspectors[pos].size();
            Object[] valueArray = new Object[rowLength];
            int rowCount = 0;
            Object[] keyValueArray = new Object[buildInspectors[pos].size()];
            Object[] key;
            Writable currentKey;
            Writable value;
            List<Object> valueList;
            while (omniReaderWrapper.next()) {
                currentKey = (Writable) omniReaderWrapper.getCurrentKey();
                value = (Writable) omniReaderWrapper.getCurrentValue();
                key = ((MapJoinKeyObject) MapJoinKey.read(output, keyCtx, currentKey)).getKeyObjects();
                System.arraycopy(key, 0, keyValueArray, 0, key.length);
                if (rowLength > 0) {
                    ObjectInspectorUtils.copyStructToArray(serde.deserialize(value), serde.getObjectInspector(),
                            ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE, valueArray, 0);
                }
                if (needAddKey) {
                    valueList = new ArrayList<>(Arrays.asList(valueArray));
                    for (Map.Entry<Integer, Integer> keyValueEntry : valuePosToKeyPos[pos].entrySet()) {
                        valueList.add(keyValueEntry.getKey(), key[keyValueEntry.getValue()]);
                    }
                    System.arraycopy(valueList.toArray(), 0, keyValueArray, key.length, valueList.size());
                } else {
                    System.arraycopy(valueArray, 0, keyValueArray, key.length, valueArray.length);
                }
                for (int i = 0; i < keyValueArray.length; i++) {
                    values[i][rowCount] = buildConverters[i].calculateValue(keyValueArray[i], buildTypeInfos[i]);
                }
                rowCount++;
                if (rowCount == BATCH) {
                    List<Vec> vecs = new ArrayList<>();
                    for (int i = 0; i < values.length; i++) {
                        vecs.add(buildConverters[i].toOmniVec(values[i], rowCount, buildTypeInfos[i]));
                    }
                    buildVecBatches.add(new VecBatch(vecs, rowCount));
                    rowCount = 0;
                }
            }
            if (rowCount > 0) {
                List<Vec> vecs = new ArrayList<>();
                for (int i = 0; i < values.length; i++) {
                    vecs.add(buildConverters[i].toOmniVec(values[i], rowCount, buildTypeInfos[i]));
                }
                buildVecBatches.add(new VecBatch(vecs, rowCount));
            }
            return buildVecBatches;
        } catch (Exception e) {
            throw new HiveException("read vec from map failed", e);
        }
    }

    private List<VecBatch> getVectorFromMapVecBatchValue(int pos, ExecMapperContext mapContext, MapredContext mrContext)
            throws HiveException {
        Map<Integer, String> parentToInput = this.getConf().getParentToInput();
        List<VecBatch> buildVecBatches = new ArrayList<>();
        int keyLength = joinKeysObjectInspectors[pos].size();
        try {
            TezContext tezContext = (TezContext) mrContext;
            String inputName = parentToInput.get(pos);
            LogicalInput input = tezContext.getInput(inputName);
            input.start();
            tezContext.getTezProcessorContext().waitForAnyInputReady(Collections.<Input>singletonList(input));
            Reader kvReader = input.getReader();
            OmniReaderWrapper omniReaderWrapper = new OmniReaderWrapper(kvReader);
            MapJoinObjectSerDeContext keyCtx = mapJoinTableSerdes[pos].getKeyContext();
            MapJoinObjectSerDeContext valCtx = mapJoinTableSerdes[pos].getValueContext();
            OmniVecBatchSerDe valueSerde = (OmniVecBatchSerDe) valCtx.getSerDe();
            OmniVecBatchSerDe keySerde = (OmniVecBatchSerDe) keyCtx.getSerDe();
            List<TypeInfo> keyValueType = new ArrayList<>(keySerde.getColumnTypes());
            keyValueType.addAll(valueSerde.getColumnTypes());
            VecBufferCache vecBufferCache = new VecBufferCache(keyValueType.size(), keyValueType);
            int rowLength = ObjectInspectorUtils.getStructSize(valueSerde.getObjectInspector());
            // if output includes key and key doesn't have expression, then the value of
            // hashMapWrapper will not contain key.
            boolean needAddKey = rowLength < joinValuesObjectInspectors[pos].size();
            int rowCount = 0;
            VecSerdeBody[] key;
            Writable currentKey;
            Writable value;
            while (omniReaderWrapper.next()) {
                currentKey = (Writable) omniReaderWrapper.getCurrentKey();
                value = (Writable) omniReaderWrapper.getCurrentValue();
                key = (VecSerdeBody[]) keySerde.deserialize(currentKey);
                vecBufferCache.addVecSerdeBody(key, rowCount, 0);
                if (rowLength > 0) {
                    VecSerdeBody[] vecSerdeBodies = (VecSerdeBody[]) valueSerde.deserialize(value);
                    vecBufferCache.addVecSerdeBody(vecSerdeBodies, rowCount, keyLength);
                }
                rowCount++;
                if (rowCount == BATCH) {
                    setBuildVecBatches(pos, buildVecBatches, vecBufferCache, needAddKey, rowCount, keyLength);
                    rowCount = 0;
                }
            }
            if (rowCount > 0) {
                setBuildVecBatches(pos, buildVecBatches, vecBufferCache, needAddKey, rowCount, keyLength);
            }
            return buildVecBatches;
        } catch (Exception e) {
            throw new HiveException("read vec from map failed", e);
        }
    }

    private void setBuildVecBatches(int pos, List<VecBatch> buildVecBatches, VecBufferCache vecBufferCache,
                                    boolean needAddKey, int rowCount, int keyLength) {
        Vec[] keyValueVecs = new Vec[buildInspectors[pos].size()];
        Vec[] cachedVecs = vecBufferCache.getValueVecBatchCache(rowCount);
        System.arraycopy(cachedVecs, 0, keyValueVecs, 0, keyLength);
        List<Vec> valueVecs = new ArrayList<>();
        for (int i = keyLength; i < cachedVecs.length; i++) {
            valueVecs.add(cachedVecs[i]);
        }
        if (needAddKey) {
            for (Map.Entry<Integer, Integer> keyValueEntry : valuePosToKeyPos[pos].entrySet()) {
                valueVecs.add(keyValueEntry.getKey(),
                        cachedVecs[keyValueEntry.getValue()].slice(0, cachedVecs[keyValueEntry.getValue()].getSize()));
            }
        }
        System.arraycopy(valueVecs.toArray(), 0, keyValueVecs, keyLength, valueVecs.size());
        buildVecBatches.add(new VecBatch(keyValueVecs, rowCount));
        valueVecs = null;
    }

    public void generateMapMetaData() throws HiveException {
        try {
            TableDesc keyTableDesc = conf.getKeyTblDesc();
            AbstractSerDe keySerializer = (AbstractSerDe) ReflectionUtil
                    .newInstance(keyTableDesc.getDeserializerClass(), null);
            SerDeUtils.initializeSerDe(keySerializer, null, keyTableDesc.getProperties(), null);
            MapJoinObjectSerDeContext keyContext = new MapJoinObjectSerDeContext(keySerializer, false);
            for (int pos = 0; pos < conf.getTagOrder().length; pos++) {
                if (pos == posBigTable) {
                    continue;
                }
                TableDesc valueTableDesc;
                if (conf.getNoOuterJoin()) {
                    valueTableDesc = conf.getValueTblDescs().get(pos);
                } else {
                    valueTableDesc = conf.getValueFilteredTblDescs().get(pos);
                }
                AbstractSerDe valueSerDe = (AbstractSerDe) ReflectionUtil
                        .newInstance(valueTableDesc.getDeserializerClass(), null);
                SerDeUtils.initializeSerDe(valueSerDe, null, valueTableDesc.getProperties(), null);
                MapJoinObjectSerDeContext valueContext = new MapJoinObjectSerDeContext(valueSerDe, hasFilter(pos));
                mapJoinTableSerdes[pos] = new MapJoinTableContainerSerDe(keyContext, valueContext);
            }
        } catch (SerDeException e) {
            throw new HiveException(e);
        }
    }

    private void initKeyPosToValuePos() {
        valuePosToKeyPos = new Map[joinKeysObjectInspectors.length];
        for (int i = 0; i < valuePosToKeyPos.length; i++) {
            valuePosToKeyPos[i] = new TreeMap<>();
        }
        for (int buildIndex : buildIndexes) {
            Map<String, Integer> buildKeyColNameToId = new HashMap<>();
            for (int i = 0; i < joinKeys[buildIndex].size(); i++) {
                if (joinKeys[buildIndex].get(i) instanceof ExprNodeColumnEvaluator) {
                    buildKeyColNameToId
                            .put(((ExprNodeColumnEvaluator) joinKeys[buildIndex].get(i)).getExpr().getColumn(), i);
                }
            }
            List<ExprNodeEvaluator> buildvalueEvaluators = getExprNodeColumnEvaluator(joinValues[buildIndex]);
            if (buildvalueEvaluators.isEmpty()) {
                return;
            }
            for (int i = 0; i < joinValues[buildIndex].size(); i++) {
                String colName = ((ExprNodeColumnEvaluator) buildvalueEvaluators.get(i)).getExpr().getColumn();
                if (buildKeyColNameToId.containsKey(colName)) {
                    valuePosToKeyPos[buildIndex].put(i, buildKeyColNameToId.get(colName));
                }
            }
        }
    }

    private OmniHashBuilderWithExprOperatorFactory getOmniHashBuilderWithExprOperatorFactory(DataType[] buildTypes,
                                                                                             int buildIndex) {
        Map<String, Integer> buildKeyColNameToId = new HashMap<>();
        List<ExprNodeEvaluator> buildExprNodeColumnEvaluators = getExprNodeColumnEvaluator(joinKeys[buildIndex]);
        for (int i = 0; i < buildExprNodeColumnEvaluators.size(); i++) {
            buildKeyColNameToId
                    .put(((ExprNodeColumnEvaluator) buildExprNodeColumnEvaluators.get(i)).getExpr().getColumn(), i);
        }
        String[] buildHashKeys = getExprFromExprNode(buildExprNodeColumnEvaluators, buildKeyColNameToId,
                inputObjInspectors[buildIndex]);
        return new OmniHashBuilderWithExprOperatorFactory(buildTypes, buildHashKeys, 1);
    }

    private OmniOperatorFactory getOmniLookupOperatorFactory(
            OmniHashBuilderWithExprOperatorFactory omniHashBuilderWithExprOperatorFactory, int probeIndex,
            List<Integer> buildIndexes, DataType[] buildTypes) {
        List<? extends StructField> probeInputfields = ((StructObjectInspector) inputObjInspectors[probeIndex])
                .getAllStructFieldRefs();
        List<String> probeOriginalName = ((StandardStructObjectInspector) inputObjInspectors[probeIndex])
                .getOriginalColumnNames();
        List<ObjectInspector> probeInspectors = probeInputfields.stream().map(StructField::getFieldObjectInspector)
                .collect(Collectors.toList());
        DataType[] probeTypes = getTypeFromInspectors(probeInspectors);
        Map<String, Integer> probekeyColNameToId = new HashMap<>();
        List<String> probeOutputfieldsName = getExprNodeColumnEvaluator(joinValues[probeIndex]).stream()
                .map(evaluator -> ((ExprNodeColumnEvaluator) evaluator).getExpr().getColumn())
                .collect(Collectors.toList());
        int[] probeOutputCols = new int[joinValuesObjectInspectors[probeIndex].size()];
        for (int i = 0; i < probeInputfields.size(); i++) {
            String fieldName = conf.isDynamicPartitionHashJoin()
                    ? probeOriginalName.get(i)
                    : probeInputfields.get(i).getFieldName();
            probekeyColNameToId.put(fieldName, i);
        }
        for (int i = 0; i < probeOutputfieldsName.size(); i++) {
            probeOutputCols[i] = probekeyColNameToId.get(probeOutputfieldsName.get(i));
        }
        // if there is ExprNodeConstantEvaluator in joinValues
        if (probeOutputfieldsName.size() < probeOutputCols.length) {
            probeOutputCols[probeOutputCols.length - 1] = probeInputfields.size() - 1;
        }
        String[] probeHashKeys = getExprFromExprNode(joinKeys[probeIndex], probekeyColNameToId,
                inputObjInspectors[probeIndex]);
        int[] buildOutputCols = new int[buildIndexes.stream()
                .mapToInt(buildIndex -> joinValuesObjectInspectors[buildIndex].size()).sum()];
        DataType[] buildOutputTypes = new DataType[buildOutputCols.length];
        int start = 0;
        int bias = 0;
        for (int buildIndex : buildIndexes) {
            bias = bias + joinKeysObjectInspectors[buildIndex].size();
            for (int i = start; i < start + joinValuesObjectInspectors[buildIndex].size(); i++) {
                buildOutputCols[i] = i + bias;
                buildOutputTypes[i] = buildTypes[i + bias];
            }
            start = start + joinValuesObjectInspectors[buildIndex].size();
        }
        JoinType joinType = JOIN_TYPE_MAP.get(condn[buildIndexes.size() - 1].getType());
        if (joinType == JoinType.OMNI_JOIN_TYPE_FULL) {
            return new OmniLookupOuterJoinWithExprOperatorFactory(probeTypes, probeOutputCols, probeHashKeys,
                    buildOutputCols, buildOutputTypes, omniHashBuilderWithExprOperatorFactory);
        } else {
            return new OmniLookupJoinWithExprOperatorFactory(probeTypes, probeOutputCols, probeHashKeys,
                    buildOutputCols, buildOutputTypes, joinType, omniHashBuilderWithExprOperatorFactory);
        }
    }

    // get the OmniLookupOperatorFactory of two build tables' join
    private OmniOperatorFactory getInnerOmniLookupOperatorFactory(
            OmniHashBuilderWithExprOperatorFactory omniHashBuilderWithExprOperatorFactory, int probeIndex,
            List<Integer> buildIndexes, DataType[] buildTypes) {
        List<ObjectInspector> probeInspectors = new ArrayList<>();
        probeInspectors.addAll(joinKeysObjectInspectors[probeIndex]);
        probeInspectors.addAll(joinValuesObjectInspectors[probeIndex]);
        DataType[] probeTypes = getTypeFromInspectors(probeInspectors);
        Map<String, Integer> probekeyColNameToId = new HashMap<>();
        List<ExprNodeEvaluator> exprNodeColumnEvaluators = getExprNodeColumnEvaluator(joinKeys[probeIndex]);
        for (int i = 0; i < exprNodeColumnEvaluators.size(); i++) {
            probekeyColNameToId.put(((ExprNodeColumnEvaluator) exprNodeColumnEvaluators.get(i)).getExpr().getColumn(),
                    i);
        }
        int[] probeOutputCols = new int[probeInspectors.size()];
        for (int i = 0; i < probeInspectors.size(); i++) {
            probeOutputCols[i] = i;
        }
        String[] probeHashKeys = getExprFromExprNode(exprNodeColumnEvaluators, probekeyColNameToId,
                inputObjInspectors[probeIndex]);
        int[] buildOutputCols = new int[buildIndexes.stream()
                .mapToInt(buildIndex -> joinValuesObjectInspectors[buildIndex].size()
                        + joinKeysObjectInspectors[buildIndex].size())
                .sum()];
        DataType[] buildOutputTypes = new DataType[buildOutputCols.length];
        for (int i = 0; i < buildOutputCols.length; i++) {
            buildOutputCols[i] = i;
            buildOutputTypes[i] = buildTypes[i];
        }
        JoinType joinType = JOIN_TYPE_MAP.get(condn[buildIndexes.size() - 1].getType());
        if (joinType == JoinType.OMNI_JOIN_TYPE_FULL) {
            return new OmniLookupOuterJoinWithExprOperatorFactory(probeTypes, probeOutputCols, probeHashKeys,
                    buildOutputCols, buildOutputTypes, omniHashBuilderWithExprOperatorFactory);
        } else {
            return new OmniLookupJoinWithExprOperatorFactory(probeTypes, probeOutputCols, probeHashKeys,
                    buildOutputCols, buildOutputTypes, joinType, omniHashBuilderWithExprOperatorFactory);
        }
    }

    @Override
    protected void completeInitializationOp(Object[] os) throws HiveException {
        if (os.length > 0) {
            List<List<VecBatch>> vecBatchList = (List<List<VecBatch>>) os[0];
            for (int i = 0; i < vecBatchList.size(); i++) {
                buildVecs[buildIndexes.get(i)] = vecBatchList.get(i);
            }
        }
        if (joinKeysObjectInspectors.length > 2) {
            // There may be many build tables, need to join build tables first, and use the
            // result as input of buildOperator
            List<Integer> buildList = new ArrayList<>();
            List<VecBatch> probeVec;
            Iterator<VecBatch> output = null;
            for (int i = buildIndexes.size() - 2; i >= 0; i--) {
                buildList.add(buildIndexes.get(i + 1));
                int probeIndex = buildIndexes.get(i);
                probeVec = getVectorFromCache(probeIndex);
                DataType[] buildTypes = getTypeFromInspectors(buildList.stream()
                        .flatMap(index -> buildInspectors[index].stream()).collect(Collectors.toList()));
                OmniHashBuilderWithExprOperatorFactory innerBuildFactory = getOmniHashBuilderWithExprOperatorFactory(
                        buildTypes, buildIndexes.get(i + 1));
                OmniOperator innerBuildOperator = innerBuildFactory.createOperator();
                OmniOperatorFactory innerJoinOperatorFactory = getInnerOmniLookupOperatorFactory(innerBuildFactory,
                        probeIndex, buildList, buildTypes);
                OmniOperator innerJoinOperator = innerJoinOperatorFactory.createOperator();
                if (output != null) {
                    while (output.hasNext()) {
                        innerBuildOperator.addInput(output.next());
                    }
                } else {
                    List<VecBatch> buildVec = getVectorFromCache(buildIndexes.get(buildIndexes.size() - 1));
                    for (VecBatch vecBatch : buildVec) {
                        innerBuildOperator.addInput(vecBatch);
                    }
                }
                innerBuildOperator.getOutput();
                for (VecBatch vecBatch : probeVec) {
                    innerJoinOperator.addInput(vecBatch);
                    output = innerJoinOperator.getOutput();
                    while (output.hasNext()) {
                        buildOperator.addInput(output.next());
                    }
                }
                innerBuildOperator.close();
                innerBuildFactory.close();
                innerJoinOperator.close();
                innerJoinOperatorFactory.close();
            }
            buildOperator.getOutput();
        } else {
            List<VecBatch> vecBatches = getVectorFromCache(1 - posBigTable);
            for (VecBatch vecBatch : vecBatches) {
                if (vecBatch.getVectorCount() > 0) {
                    buildOperator.addInput(vecBatch);
                }
            }
            buildOperator.getOutput();
        }
    }

    private List<VecBatch> getVectorFromCache(int index) {
        if (conf.isDynamicPartitionHashJoin()) {
            return buildVecs[index];
        }
        return buildVecs[index].stream().map(OmniHiveOperator::copyVecBatch).collect(Collectors.toList());
    }

    private int[] getOrder(int posBigTable, int aliasNum) {
        if (posBigTable == 0) {
            return null;
        }
        int[] order = new int[aliasNum];
        order[0] = posBigTable;
        int index = 1;
        for (int i = 0; i < aliasNum; i++) {
            if (i == posBigTable) {
                continue;
            }
            order[index++] = i;
        }
        return order;
    }

    private VecBatch reorderVecs(VecBatch vecBatch, int[] order, List<ObjectInspector>[] joinValuesObjectInspectors) {
        // because omni operator's output oder will always be [probeTable buildTable]
        // but hive is according to tag' order
        // so need to reorder output col
        // The property order means the current order of output cols of tables
        // For example {1, 0, 2} means the current order is Table1 Table0 Table2, we
        // need to reorder it to Table0 Table1 Table2
        Vec[] newVecs = new Vec[vecBatch.getVectors().length];
        int srcPos = 0;
        for (int i = 0; i < order.length; i++) {
            int destPos = 0;
            for (int j = 0; j < order[i]; j++) {
                destPos = destPos + joinValuesObjectInspectors[j].size();
            }
            System.arraycopy(vecBatch.getVectors(), srcPos, newVecs, destPos,
                    joinValuesObjectInspectors[order[i]].size());
            srcPos = srcPos + joinValuesObjectInspectors[order[i]].size();
        }
        vecBatch.close();
        return new VecBatch(newVecs, vecBatch.getRowCount());
    }

    private Optional<String> generateResidualFilter() {
        if (conf.getResidualFilterExprs() == null || conf.getResidualFilterExprs().isEmpty()) {
            return Optional.empty();
        }
        ExprNodeGenericFuncDesc predicate = (ExprNodeGenericFuncDesc) conf.getResidualFilterExprs().get(0);
        return Optional.of(ExpressionUtils.build(predicate, outputObjInspector).toString());
    }

    @Override
    public void process(Object row, int tag) throws HiveException {
        VecBatch input = (VecBatch) row;
        this.joinOperator.addInput(input);
        output = this.joinOperator.getOutput();
        while (output.hasNext()) {
            VecBatch vecBatch = output.next();
            if (order != null) {
                vecBatch = reorderVecs(vecBatch, order, joinValuesObjectInspectors);
            }
            if (filterOperator != null) {
                filterAndForward(vecBatch);
            } else {
                forward(vecBatch, outputObjInspector);
            }
        }
    }

    private void filterAndForward(VecBatch vecBatch) throws HiveException {
        filterOperator.addInput(vecBatch);
        output = this.filterOperator.getOutput();
        while (output.hasNext()) {
            forward(output.next(), outputObjInspector);
        }
    }

    @Override
    protected void forward(Object row, ObjectInspector rowInspector) throws HiveException {
        VecBatch vecBatch = null;
        VecBatch[] vecBatches = new VecBatch[childOperatorsArray.length];
        if (row instanceof VecBatch) {
            vecBatch = (VecBatch) row;
            vecBatches[0] = vecBatch;
            this.runTimeNumRows += vecBatch.getRowCount();
            if (childOperatorsArray.length > 1) {
                for (int i = 1; i < vecBatches.length; i++) {
                    if (!childOperatorsArray[i].getDone()) {
                        vecBatches[i] = OmniHiveOperator.copyVecBatch(vecBatch);
                    }
                }
            }
        }
        if (getDone()) {
            if (vecBatch != null) {
                vecBatch.releaseAllVectors();
                vecBatch.close();
            }
            return;
        }
        int childrenDone = 0;
        for (int i = 0; i < childOperatorsArray.length; i++) {
            Operator<? extends OperatorDesc> o = childOperatorsArray[i];
            if (o.getDone()) {
                childrenDone++;
            } else {
                if (vecBatch != null) {
                    o.process(vecBatches[i], childOperatorsTag[i]);
                } else {
                    o.process(row, childOperatorsTag[i]);
                }
            }
        }
        // if all children are done, this operator is also done
        if (childrenDone != 0 && childrenDone == childOperatorsArray.length) {
            setDone(true);
            if (vecBatch != null) {
                vecBatch.releaseAllVectors();
                vecBatch.close();
            }
        }
    }

    @Override
    public String getName() {
        return OmniMapJoinOperator.getOperatorName();
    }

    public static String getOperatorName() {
        return "OMNI-MAPJOIN";
    }

    @Override
    public OperatorType getType() {
        return OperatorType.MAPJOIN;
    }

    public boolean isChangedCtx() {
        return changedCtx;
    }

    @Override
    public VectorizationContext getOutputVectorizationContext() {
        return vectorizationContext;
    }

    @Override
    public void closeOp(boolean abort) throws HiveException {
        joinOperator.close();
        buildOperator.close();
        omniLookupJoinWithExprOperatorFactory.close();
        omniHashBuilderWithExprOperatorFactory.close();
        output = null;
        if (filterOperator != null) {
            filterOperator.close();
            filterAndProjectOperatorFactory.close();
        }
        super.closeOp(abort);
    }

    @Override
    public void startGroup() throws HiveException {
    }

    @Override
    public void endGroup() throws HiveException {
    }

    public void publicSetDone(boolean done) {
        this.done = done;
    }

    private static class OmniReaderWrapper {
        private boolean isSingleValue;
        private KeyValueReader originValueReader;
        private KeyValuesReader originValuesReader;

        public OmniReaderWrapper(Reader reader) {
            if (reader instanceof KeyValueReader) {
                isSingleValue = true;
                originValueReader = (KeyValueReader) reader;
            } else {
                originValuesReader = (KeyValuesReader) reader;
            }
        }

        public boolean next() throws IOException {
            if (isSingleValue) {
                return originValueReader.next();
            }
            if (hasNextValue()) {
                return true;
            }
            return originValuesReader.next();
        }

        public boolean hasNextValue() throws IOException {
            if (isSingleValue) {
                return false;
            }
            return originValuesReader.getCurrentValues().iterator().hasNext();
        }

        public Object getCurrentKey() throws IOException {
            if (isSingleValue) {
                return originValueReader.getCurrentKey();
            }
            return originValuesReader.getCurrentKey();
        }

        public Object getCurrentValue() throws IOException {
            if (isSingleValue) {
                return originValueReader.getCurrentValue();
            }
            return originValuesReader.getCurrentValues().iterator().next();
        }
    }
}