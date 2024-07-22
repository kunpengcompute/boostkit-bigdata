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

import static com.huawei.boostkit.hive.JoinUtils.getExprNodeColumnEvaluator;
import static com.huawei.boostkit.hive.OmniMapJoinOperator.JOIN_TYPE_MAP;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;

import nova.hetu.omniruntime.constants.JoinType;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.OmniOperatorFactory;
import nova.hetu.omniruntime.operator.join.OmniSmjBufferedTableWithExprOperatorFactory;
import nova.hetu.omniruntime.operator.join.OmniSmjStreamedTableWithExprOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.ShortVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.ExprNodeColumnEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.RecordSource;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.CommonMergeJoinDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

public class OmniMergeJoinOperator extends CommonJoinOperator<OmniMergeJoinDesc> implements Serializable {
    protected static final int SMJ_NEED_ADD_STREAM_TBL_DATA = 2;
    protected static final int SMJ_NEED_ADD_BUFFERED_TBL_DATA = 3;
    protected static final int SCAN_FINISH = 4;
    protected static final int RES_INIT = 0;
    protected static final int SMJ_FETCH_JOIN_DATA = 5;

    private static final long serialVersionUID = 1L;

    protected transient RecordSource[] sources;
    protected transient boolean[] fetchDone;

    protected transient OmniSmjBufferedTableWithExprOperatorFactory[] bufferFactories;
    protected transient OmniSmjStreamedTableWithExprOperatorFactory[] streamFactories;
    protected transient OmniOperator[] bufferOperators;
    protected transient OmniOperator[] streamOperators;

    protected transient int[] resCode;
    protected transient int[] flowControlCode;
    protected transient Queue<VecBatch>[] streamData;
    protected transient Queue<VecBatch>[] bufferData;
    protected transient DataType[][] streamTypes;
    protected transient DataType[][] bufferTypes;
    protected int posBigTable;
    private OmniVectorOperator omniVectorOperator;

    public OmniMergeJoinOperator(CompilationOpContext ctx) {
        super(ctx);
    }

    public OmniMergeJoinOperator(CompilationOpContext ctx, CommonMergeJoinDesc commonMergeJoinDesc) {
        super(ctx);
        this.conf = new OmniMergeJoinDesc(commonMergeJoinDesc);
        this.posBigTable = commonMergeJoinDesc.getPosBigTable();
    }

    /**
     * Kryo ctor.
     */
    protected OmniMergeJoinOperator() {
        super();
    }

    @Override
    // If mergeJoinOperator has 3 tables, first join table0 and table1, and output
    // all columns of table0 and table1.
    // Then use the output to join table2, and output required columns.
    protected void initializeOp(Configuration hconf) throws HiveException {
        int sourceNum = parentOperators.get(0).getInputObjInspectors().length;
        ObjectInspector[] newInputObjInspectors = new ObjectInspector[sourceNum];
        for (int i = 0; i < sourceNum; i++) {
            newInputObjInspectors[i] = ((StructObjectInspector) inputObjInspectors[0]).getAllStructFieldRefs().get(i)
                    .getFieldObjectInspector();
        }
        inputObjInspectors = newInputObjInspectors;
        super.initializeOp(hconf);
        fetchDone = new boolean[sourceNum];
        streamFactories = new OmniSmjStreamedTableWithExprOperatorFactory[sourceNum - 1];
        streamOperators = new OmniOperator[sourceNum - 1];
        bufferFactories = new OmniSmjBufferedTableWithExprOperatorFactory[sourceNum - 1];
        bufferOperators = new OmniOperator[sourceNum - 1];
        resCode = new int[sourceNum - 1];
        flowControlCode = new int[sourceNum - 1];
        Arrays.fill(flowControlCode, SMJ_NEED_ADD_STREAM_TBL_DATA);
        streamData = new Queue[sourceNum - 1];
        bufferData = new Queue[sourceNum - 1];
        for (int i = 0; i < streamData.length; i++) {
            streamData[i] = new LinkedList<>();
            bufferData[i] = new LinkedList<>();
        }
        streamTypes = new DataType[sourceNum - 1][];
        bufferTypes = new DataType[sourceNum - 1][];
        for (int i = 1; i < sourceNum; i++) {
            generateOmniOperator(i, true);
        }
        generateOmniOperator(streamFactories.length, false);
        sources = ((TezContext) MapredContext.get()).getRecordSources();
        if (parentOperators.get(0) instanceof OmniVectorOperator) {
            omniVectorOperator = (OmniVectorOperator) parentOperators.get(0);
        }
    }

    private void generateOmniOperator(int bufferIndex, boolean isGetAll) throws HiveException {
        int opIndex = bufferIndex - 1;
        List<Integer> streamAliasList = new ArrayList<>();
        for (int i = 0; i < bufferIndex; i++) {
            streamAliasList.add(i);
        }
        streamFactories[opIndex] =
                (OmniSmjStreamedTableWithExprOperatorFactory) getFactory(streamAliasList, null,
                isGetAll, opIndex);
        streamOperators[opIndex] = streamFactories[opIndex].createOperator();
        bufferFactories[opIndex] = (OmniSmjBufferedTableWithExprOperatorFactory) getFactory(Arrays.asList(bufferIndex),
                streamFactories[opIndex], isGetAll, opIndex);
        bufferOperators[opIndex] = bufferFactories[opIndex].createOperator();
    }

    private OmniOperatorFactory getFactory(List<Integer> aliasList,
                                           OmniSmjStreamedTableWithExprOperatorFactory streamFactory,
                                           boolean isGetAll, int opIndex) throws HiveException {
        List<? extends StructField> inputFields = aliasList.stream()
                .flatMap(alias -> ((StructObjectInspector) inputObjInspectors[alias]).getAllStructFieldRefs().stream()
                        .flatMap(keyValue -> ((StructObjectInspector) keyValue.getFieldObjectInspector())
                                .getAllStructFieldRefs().stream())).collect(Collectors.toList());
        DataType[] inputTypes = new DataType[inputFields.size()];
        List<Map<String, Integer>> colNameToId = new ArrayList<>();
        aliasList.forEach(a -> colNameToId.add(new HashMap<>()));
        int[] fieldNum = new int[aliasList.size()];
        fieldNum[0] = ((StructObjectInspector) inputObjInspectors[aliasList.get(0)]).getAllStructFieldRefs().stream()
                .mapToInt(keyValue -> ((StructObjectInspector) keyValue.getFieldObjectInspector())
                        .getAllStructFieldRefs().size()).sum();
        for (int i = 1; i < fieldNum.length; i++) {
            fieldNum[i] = fieldNum[i - 1]
                    + ((StructObjectInspector) inputObjInspectors[aliasList.get(i)]).getAllStructFieldRefs().stream()
                    .mapToInt(keyValue -> ((StructObjectInspector) keyValue.getFieldObjectInspector())
                            .getAllStructFieldRefs().size()).sum();
        }
        int tagIndex = 0;
        for (int i = 0; i < inputFields.size(); i++) {
            if (i >= fieldNum[tagIndex]) {
                ++tagIndex;
            }
            inputTypes[i] = TypeUtils.buildInputDataType(((AbstractPrimitiveObjectInspector) inputFields.get(i)
                    .getFieldObjectInspector()).getTypeInfo());
            colNameToId.get(tagIndex).put(inputFields.get(i).getFieldName(), i);
        }
        int[] outputCols;
        if (isGetAll) {
            outputCols = new int[inputTypes.length];
            for (int i = 0; i < inputTypes.length; i++) {
                outputCols[i] = i;
            }
        } else {
            int start = 0;
            outputCols = new int[aliasList.stream().mapToInt(a -> joinValuesObjectInspectors[a].size()).sum()];
            for (int i = 0; i < aliasList.size(); i++) {
                List<String> outputFieldsName = getExprNodeColumnEvaluator(joinValues[aliasList.get(i)]).stream()
                        .map(evaluator -> ((ExprNodeColumnEvaluator) evaluator).getExpr().getColumn()
                                .split("\\.")[1]).collect(Collectors.toList());
                for (int j = start; j < start + outputFieldsName.size(); j++) {
                    outputCols[j] = colNameToId.get(i).get(outputFieldsName.get(j - start));
                }
                start += outputFieldsName.size();
            }
        }
        String[] hashKey = getHashKey(aliasList, streamFactory, opIndex, colNameToId);
        JoinType joinType = JOIN_TYPE_MAP.get(condn[opIndex].getType());
        if (streamFactory == null) {
            Optional<String> filter = generateFilter(opIndex);
            streamTypes[opIndex] = inputTypes;
            return new OmniSmjStreamedTableWithExprOperatorFactory(inputTypes, hashKey, outputCols, joinType, filter);
        } else {
            bufferTypes[opIndex] = inputTypes;
            return new OmniSmjBufferedTableWithExprOperatorFactory(inputTypes, hashKey, outputCols, streamFactory);
        }
    }

    // sql like cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk will have
    // residualJoinFilters
    private Optional<String> generateFilter(int opIndex) throws HiveException {
        ExprNodeGenericFuncDesc joinFilter = getJoinFilter(opIndex);
        if ((residualJoinFilters == null || residualJoinFilters.get(opIndex) == null) && joinFilter == null) {
            return Optional.empty();
        }
        BaseExpression root;
        if (joinFilter == null) {
            Map<String, List<String>> inputColNameToExprName = getInputColNameToExprName();
            List<? extends StructField> fields = Utilities.constructVectorizedReduceRowOI((StructObjectInspector) ((StructObjectInspector) inputObjInspectors[opIndex]).getAllStructFieldRefs().get(0).getFieldObjectInspector(),
                    (StructObjectInspector) ((StructObjectInspector) inputObjInspectors[opIndex]).getAllStructFieldRefs().get(1).getFieldObjectInspector()).getAllStructFieldRefs();
            List<String> fieldNames = fields.stream().map(field -> {
                        String key = field.getFieldName()
                                .replace("value.", "VALUE.").replace("key.", "KEY.");
                        if (inputColNameToExprName.containsKey(key)) {
                            List<String> exprNames = inputColNameToExprName.get(key);
                            return exprNames.get(Math.min(exprNames.size() - 1, opIndex))
                                    .replace("value.", "").replace("key.", "");
                        } else {
                            return field.getFieldName()
                                    .replace("value.", "").replace("key.", "");
                        }
                    }
            ).collect(Collectors.toList());
            int bufferIndex = opIndex + 1;
            fields = Utilities.constructVectorizedReduceRowOI((StructObjectInspector) ((StructObjectInspector) inputObjInspectors[bufferIndex]).getAllStructFieldRefs().get(0).getFieldObjectInspector(),
                    (StructObjectInspector) ((StructObjectInspector) inputObjInspectors[bufferIndex]).getAllStructFieldRefs().get(1).getFieldObjectInspector()).getAllStructFieldRefs();
            fieldNames.addAll(fields.stream().map(field -> {
                        String key = field.getFieldName()
                                .replace("value.", "VALUE.").replace("key.", "KEY.");
                        if (inputColNameToExprName.containsKey(key)) {
                            List<String> exprNames = inputColNameToExprName.get(key);
                            return exprNames.get(Math.min(exprNames.size() - 1, bufferIndex))
                                    .replace("value.", "").replace("key.", "");
                        } else {
                            return field.getFieldName()
                                    .replace("value.", "").replace("key.", "");
                        }
                    }
            ).collect(Collectors.toList()));
            List<ObjectInspector> inspectors =
                    fields.stream().map(StructField::getFieldObjectInspector).collect(Collectors.toList());
            inspectors.addAll(fields.stream().map(StructField::getFieldObjectInspector).collect(Collectors.toList()));

            StructObjectInspector exprObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                    inspectors);
            root = ExpressionUtils.build(getResidualFilter(), exprObjInspector);
        } else {
            StructObjectInspector flattenInspector = Utilities.constructVectorizedReduceRowOI((StructObjectInspector) ((StructObjectInspector) inputObjInspectors[opIndex]).getAllStructFieldRefs().get(0).getFieldObjectInspector(),
                    (StructObjectInspector) ((StructObjectInspector) inputObjInspectors[opIndex]).getAllStructFieldRefs().get(1).getFieldObjectInspector());
            List<ObjectInspector> inspectors = flattenInspector.getAllStructFieldRefs()
                    .stream().map(field -> field.getFieldObjectInspector()).collect(Collectors.toList());
            List<String> fieldNames = flattenInspector.getAllStructFieldRefs()
                    .stream().map(field -> field.getFieldName()).collect(Collectors.toList());
            StructObjectInspector exprObjInspector = ObjectInspectorFactory
                    .getStandardStructObjectInspector(fieldNames, inspectors);
            root = ExpressionUtils.build(joinFilter, exprObjInspector);
        }
        return Optional.of(root.toString());
    }

    private Map<String, List<String>> getInputColNameToExprName() {
        Map<String, List<String>> inputColNameToExprName = new HashMap<>();
        for (Map.Entry<String, ExprNodeDesc> entry : conf.getColumnExprMap().entrySet()) {
            ExprNodeColumnDesc exprNodeColumnDesc = (ExprNodeColumnDesc) entry.getValue();
            if (!inputColNameToExprName.containsKey(exprNodeColumnDesc.getColumn())) {
                inputColNameToExprName.put(exprNodeColumnDesc.getColumn(), new ArrayList<>());
            }
            inputColNameToExprName.get(exprNodeColumnDesc.getColumn()).add(entry.getKey());
        }
        return inputColNameToExprName;
    }

    private ExprNodeGenericFuncDesc getJoinFilter(int opIndex) {
        List<ExprNodeDesc> filters =
                joinFilters[opIndex].stream().map(ExprNodeEvaluator::getExpr).collect(Collectors.toList());
        if (filters.isEmpty()) {
            return null;
        }
        if (filters.size() == 1) {
            return (ExprNodeGenericFuncDesc) filters.get(0);
        }
        try {
            return ExprNodeGenericFuncDesc.newInstance(new GenericUDFOPAnd(), filters);
        } catch (UDFArgumentException e) {
            throw new RuntimeException("wrong UDF", e);
        }
    }

    private ExprNodeGenericFuncDesc getResidualFilter() {
        List<ExprNodeDesc> filters =
                residualJoinFilters.stream().map(ExprNodeEvaluator::getExpr).collect(Collectors.toList());
        if (filters.size() == 1) {
            return (ExprNodeGenericFuncDesc) filters.get(0);
        }
        try {
            return ExprNodeGenericFuncDesc.newInstance(new GenericUDFOPAnd(), filters);
        } catch (UDFArgumentException e) {
            throw new RuntimeException("wrong udf", e);
        }
    }

    private String[] getHashKey(List<Integer> aliasList, OmniSmjStreamedTableWithExprOperatorFactory streamFactory,
                                int index, List<Map<String, Integer>> colNameToId) {
        List<String> expressions = new ArrayList<>();
        int keyIndex = streamFactory == null ? condn[index].getLeft() : condn[index].getRight();
        for (int i = 0; i < aliasList.size(); i++) {
            if (aliasList.get(i) != keyIndex) {
                continue;
            }
            int finalI = i;
            expressions = ((StructObjectInspector) ((StructObjectInspector) inputObjInspectors[aliasList.get(i)])
                    .getAllStructFieldRefs().get(0).getFieldObjectInspector()).getAllStructFieldRefs().stream()
                    .map(field -> TypeUtils.buildExpression(
                            ((AbstractPrimitiveObjectInspector) field.getFieldObjectInspector()).getTypeInfo(),
                            colNameToId.get(finalI).get(field.getFieldName()))).collect(Collectors.toList());
        }
        return expressions.toArray(new String[0]);
    }

    @Override
    public void process(Object row, int tag) throws HiveException {
        VecBatch input = (VecBatch) row;
        if (flowControlCode[flowControlCode.length - 1] == SCAN_FINISH) {
            input.releaseAllVectors();
            input.close();
            setDone(true);
            return;
        }
        if (tag == 0 && flowControlCode[0] != SCAN_FINISH) {
            streamData[0].offer(input);
        } else if (tag >= 1 && flowControlCode[tag - 1] != SCAN_FINISH) {
            bufferData[tag - 1].offer(input);
        } else {
            input.releaseAllVectors();
            input.close();
            return;
        }
        if (tag == posBigTable) {
            processOmni(0, 1);
            for (int opIndex = 1; opIndex < streamFactories.length; opIndex++) {
                if (!streamData[opIndex].isEmpty()) {
                    processOmni(opIndex, opIndex + 1);
                }
            }
        }
    }

    protected void processOmni(int opIndex, int bufferIndex) throws HiveException {
        if (flowControlCode[opIndex] != SCAN_FINISH && resCode[opIndex] == RES_INIT) {
            if (flowControlCode[opIndex] == SMJ_NEED_ADD_STREAM_TBL_DATA) {
                processOmniSmj(opIndex, opIndex, streamData,
                        streamOperators, SMJ_NEED_ADD_STREAM_TBL_DATA, streamTypes);
            } else {
                processOmniSmj(opIndex, bufferIndex, bufferData,
                        bufferOperators, SMJ_NEED_ADD_BUFFERED_TBL_DATA, bufferTypes);
            }
        }
        if (resCode[opIndex] == SMJ_FETCH_JOIN_DATA) {
            dealOutput(opIndex);
        }
    }

    private void dealOutput(int opIndex) throws HiveException {
        Iterator<VecBatch> output = bufferOperators[opIndex].getOutput();
        while (!getDone() && output.hasNext()) {
            VecBatch vecBatch = output.next();
            if (streamFactories.length <= opIndex + 1) {
                forward(vecBatch, outputObjInspector);
                continue;
            }
            if (flowControlCode[opIndex + 1] == SCAN_FINISH) {
                vecBatch.releaseAllVectors();
                vecBatch.close();
                continue;
            }
            streamData[opIndex + 1].offer(vecBatch);
            if (flowControlCode[opIndex + 1] == SMJ_NEED_ADD_STREAM_TBL_DATA) {
                processOmni(opIndex + 1, opIndex + 2);
            }
        }
        resCode[opIndex] = RES_INIT;
    }

    protected void processOmniSmj(int opIndex, int dataIndex, Queue<VecBatch>[] data, OmniOperator[] operators,
                                  int controlCode, DataType[][] types) throws HiveException {
        if (!data[opIndex].isEmpty()) {
            while (flowControlCode[opIndex] == controlCode && resCode[opIndex] == RES_INIT
                    && !data[opIndex].isEmpty()) {
                setStatus(operators[opIndex].addInput(data[opIndex].poll()), opIndex);
            }
            return;
        }
        if (opIndex == dataIndex && opIndex > 0 && flowControlCode[opIndex - 1] == SCAN_FINISH) {
            setStatus(operators[opIndex].addInput(createEofVecBatch(types[opIndex])), opIndex);
            return;
        }

        if (!fetchDone[dataIndex] && posBigTable != dataIndex) {
            while (flowControlCode[opIndex] == controlCode && resCode[opIndex] == RES_INIT && !fetchDone[dataIndex]) {
                fetchDone[dataIndex] = !sources[dataIndex].pushRecord();
                if (!data[opIndex].isEmpty()) {
                    setStatus(operators[opIndex].addInput(data[opIndex].poll()), opIndex);
                }
            }
        } else if (omniVectorOperator.getRowCount()[dataIndex] > 0 && posBigTable != dataIndex) {
            omniVectorOperator.pushRestData(dataIndex);
            if (!data[opIndex].isEmpty()) {
                setStatus(operators[opIndex].addInput(data[opIndex].poll()), opIndex);
            }
        } else if (omniVectorOperator.getRowCount()[dataIndex] == 0) {
            setStatus(operators[opIndex].addInput(createEofVecBatch(types[opIndex])), opIndex);
        }
    }

    protected void setStatus(int code, int tag) {
        flowControlCode[tag] = code >> 16;
        resCode[tag] = code & 0xFFFF;
    }

    public int getPosBigTable() {
        return posBigTable;
    }

    @Override
    public void startGroup() throws HiveException {
    }

    @Override
    public void endGroup() throws HiveException {
    }

    public void publicSetDone(boolean isDone) {
        this.done = isDone;
    }

    @Override
    public String getName() {
        return "OMNI_MERGEJOIN";
    }

    @Override
    public OperatorType getType() {
        return OperatorType.MERGEJOIN;
    }

    @Override
    public void close(boolean isAbort) throws HiveException {
        if (!allInitializedParentsAreClosed()) {
            return;
        }
        if (sources == null) {
            fetchDone = new boolean[]{true, true, true};
        }
        Set<Integer> needDeal = new HashSet<>();
        for (int opIndex = streamFactories.length - 1; opIndex >= 0; opIndex--) {
            if (flowControlCode[opIndex] == SCAN_FINISH) {
                break;
            }
            needDeal.add(opIndex);
        }
        for (int opIndex = 0; opIndex < streamFactories.length; opIndex++) {
            if (!needDeal.contains(opIndex)) {
                continue;
            }
            while (!getDone() && flowControlCode[opIndex] != SCAN_FINISH && flowControlCode[opIndex] != 0) {
                processOmni(opIndex, opIndex + 1);
            }
        }
        super.close(isAbort);
    }

    protected VecBatch createEofVecBatch(DataType[] dataTypes) {
        Vec[] vecs = new Vec[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            switch (dataTypes[i].getId()) {
                case OMNI_INT:
                case OMNI_DATE32:
                    vecs[i] = new IntVec(0);
                    break;
                case OMNI_LONG:
                case OMNI_DECIMAL64:
                    vecs[i] = new LongVec(0);
                    break;
                case OMNI_DOUBLE:
                    vecs[i] = new DoubleVec(0);
                    break;
                case OMNI_BOOLEAN:
                    vecs[i] = new BooleanVec(0);
                    break;
                case OMNI_CHAR:
                case OMNI_VARCHAR:
                    vecs[i] = new VarcharVec(0);
                    break;
                case OMNI_DECIMAL128:
                    vecs[i] = new Decimal128Vec(0);
                    break;
                case OMNI_SHORT:
                    vecs[i] = new ShortVec(0);
                    break;
                default:
                    throw new IllegalArgumentException(String.format("VecType %s is not supported in %s yet",
                            dataTypes[i].getClass().getSimpleName(), this.getClass().getSimpleName()));
            }
        }
        return new VecBatch(vecs, 0);
    }

    public boolean[] getFetchDone() {
        return fetchDone;
    }

    @Override
    protected void forward(Object row, ObjectInspector rowInspector) throws HiveException {
        VecBatch vecBatch = (VecBatch) row;
        this.runTimeNumRows += vecBatch.getRowCount();
        if (getDone()) {
            vecBatch.releaseAllVectors();
            vecBatch.close();
            return;
        }
        int childrenDone = 0;
        for (int i = 0; i < childOperatorsArray.length; i++) {
            Operator<? extends OperatorDesc> o = childOperatorsArray[i];
            if (o.getDone()) {
                childrenDone++;
            } else {
                o.process(row, childOperatorsTag[i]);
            }
        }

        if (childrenDone != 0 && childrenDone == childOperatorsArray.length) {
            setDone(true);
            vecBatch.releaseAllVectors();
            vecBatch.close();
        }
    }

    @Override
    public void closeOp(boolean isAbort) throws HiveException {
        for (int i = 0; i < streamOperators.length; i++) {
            streamOperators[i].close();
            bufferOperators[i].close();
            streamFactories[i].close();
            bufferFactories[i].close();
            for (VecBatch vecBatch : streamData[i]) {
                vecBatch.releaseAllVectors();
                vecBatch.close();
            }
            for (VecBatch vecBatch : bufferData[i]) {
                vecBatch.releaseAllVectors();
                vecBatch.close();
            }
        }
        super.closeOp(isAbort);
    }
}