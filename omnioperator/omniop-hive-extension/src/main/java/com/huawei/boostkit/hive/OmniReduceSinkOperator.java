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

import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_BOOLEAN;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_CHAR;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_DECIMAL128;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_DOUBLE;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_INT;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_LONG;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_SHORT;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_VARCHAR;
import static org.apache.hadoop.hive.ql.optimizer.SortedDynPartitionOptimizer.BUCKET_NUMBER_COL_NAME;
import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.UNIFORM;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;
import com.huawei.boostkit.hive.shuffle.OmniVecBatchSerDe;
import com.huawei.boostkit.hive.shuffle.VecWrapper;

import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.operator.config.OverflowConfig;
import nova.hetu.omniruntime.operator.config.SpillConfig;
import nova.hetu.omniruntime.operator.project.OmniProjectOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.ShortVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.VariableWidthVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.ExprNodeColumnEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.PTFTopNHash;
import org.apache.hadoop.hive.ql.exec.TerminalOperator;
import org.apache.hadoop.hive.ql.exec.TopNHash;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveChar;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveVarchar;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hive.common.util.Murmur3;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class OmniReduceSinkOperator extends TerminalOperator<ReduceSinkDesc>
        implements Serializable, TopNHash.BinaryCollector {
    private static final long serialVersionUID = 1L;

    protected transient OutputCollector out;

    /**
     * The evaluators for the key columns. Key columns decide the sort order on
     * the reducer side. Key columns are passed to the reducer in the "key".
     */
    protected transient ExprNodeEvaluator[] keyEval;

    /**
     * The evaluators for the value columns. Value columns are passed to reducer
     * in the "value".
     */
    protected transient ExprNodeEvaluator[] valueEval;

    /**
     * The evaluators for the partition columns (CLUSTER BY or DISTRIBUTE BY in
     * Hive language). Partition columns decide the reducer that the current row
     * goes to. Partition columns are not passed to reducer.
     */
    protected transient ExprNodeEvaluator[] partitionEval;

    /**
     * Evaluators for bucketing columns. This is used to compute bucket number.
     */
    protected transient ExprNodeEvaluator[] bucketEval = null;

    // TODO: we use MetadataTypedColumnsetSerDe for now, till DynamicSerDe is ready
    protected transient Serializer keySerializer;
    protected transient boolean keyIsText;
    protected transient Serializer valueSerializer;
    protected transient byte[] tagByte = new byte[1];
    protected transient int numDistributionKeys;
    protected transient int numDistinctExprs;
    protected transient String[] inputAliases; // input aliases of this RS for join (used for PPD)
    protected transient boolean isUseUniformHash = false;

    // picks topN K:V pairs from input.
    protected transient TopNHash reducerHash;
    protected transient HiveKey keyWritable = new HiveKey();
    protected transient ObjectInspector keyObjectInspector;
    protected transient ObjectInspector valueObjectInspector;
    protected transient Object[] cachedValues;
    protected transient List<List<Integer>> distinctColIndices;
    protected transient Random random;

    protected transient BiFunction<Object[], ObjectInspector[], Integer> hashFunc;

    /**
     * This two dimensional array holds key data and a corresponding Union object
     * which contains the tag identifying the aggregate expression for distinct
     * columns.
     * <p>
     * If there is no distinct expression, cachedKeys is simply like this.
     * cachedKeys[0] = [col0][col1]
     * <p>
     * with two distict expression, union(tag:key) is attatched for each distinct
     * expression
     * cachedKeys[0] = [col0][col1][0:dist1]
     * cachedKeys[1] = [col0][col1][1:dist2]
     * <p>
     * in this case, child GBY evaluates distict values with expression like
     * KEY.col2:0.dist1
     * see {@link ExprNodeColumnEvaluator}
     */
    // TODO: we only ever use one row of these at a time. Why do we need to cache
    // multiple?
    protected transient Object[][] cachedKeys;

    protected transient long cntr = 1L;
    protected transient long logEveryNRows = 0L;

    private transient ObjectInspector[] partitionObjectInspectors;
    private transient ObjectInspector[] bucketObjectInspectors;
    private transient int buckColIdxInKey;

    /**
     * {@link org.apache.hadoop.hive.ql.optimizer.SortedDynPartitionOptimizer}
     */
    private transient int buckColIdxInKeyForSdpo = -1;
    private boolean isFirstRow;
    private boolean isSkipTag = false;
    private transient int[] valueIndex; // index for value(+ from keys, - from values)
    private transient Set<Integer> decimal128ConvertDecimal64Cols = new HashSet<>();

    private long[] keyFieldId;
    private long[] valueFieldId;
    private transient boolean isNeedProject;

    private transient OmniOperator projectOperator;
    private transient boolean isReduceSinkCanReplaceKey;
    private transient VecWrapper[] vecWrappers;

    public OmniReduceSinkOperator(CompilationOpContext ctx) {
        super(ctx);
    }

    /**
     * Kryo ctor.
     */
    protected OmniReduceSinkOperator() {
        super();
    }

    public OmniReduceSinkOperator(CompilationOpContext ctx, ReduceSinkDesc conf, boolean isReduceSinkCanReplaceKey) {
        super(ctx);
        this.conf = conf;
        this.isReduceSinkCanReplaceKey = isReduceSinkCanReplaceKey;
    }

    @Override
    protected void initializeOp(Configuration hconf) throws HiveException {
        super.initializeOp(hconf);
        try {
            long l = Runtime.getRuntime().maxMemory();
            this.conf.setMaxMemoryAvailable(l);

            ArrayList<ExprNodeDesc> keyCols = conf.getKeyCols();
            for (ExprNodeDesc valueCol : keyCols) {
                if (valueCol instanceof ExprNodeGenericFuncDesc || valueCol instanceof ExprNodeConstantDesc) {
                    isNeedProject = true;
                }
            }
            keyFieldId = new long[keyCols.size()];
            int expressionKeyCount = 0;
            for (int i = 0; i < keyCols.size(); i++) {
                ExprNodeDesc nodeDesc = keyCols.get(i);
                if (nodeDesc instanceof ExprNodeColumnDesc) {
                    StructField structFieldRef = ((StructObjectInspector) inputObjInspectors[0])
                            .getStructFieldRef(nodeDesc.getExprString());
                    keyFieldId[i] = structFieldRef.getFieldID();
                } else if (nodeDesc instanceof ExprNodeGenericFuncDesc) {
                    StructField structFieldRef = ((StructObjectInspector) inputObjInspectors[0])
                            .getStructFieldRef(nodeDesc.getCols().get(0));
                    keyFieldId[i] = expressionKeyCount;
                    ++expressionKeyCount;
                }
            }
            ArrayList<ExprNodeDesc> valueCols = this.conf.getValueCols();
            for (ExprNodeDesc keyCol : valueCols) {
                if (keyCol instanceof ExprNodeGenericFuncDesc || keyCol instanceof ExprNodeConstantDesc) {
                    isNeedProject = true;
                }
            }
            this.valueFieldId = new long[valueCols.size()];

            for (int i = 0; i < valueCols.size(); ++i) {
                ExprNodeDesc exprNodeDesc = valueCols.get(i);
                if (exprNodeDesc instanceof ExprNodeColumnDesc) {
                    StructField structFieldRef = ((StructObjectInspector) this.inputObjInspectors[0])
                            .getStructFieldRef(exprNodeDesc.getExprString());
                    this.valueFieldId[i] = structFieldRef.getFieldID();
                }
            }

            numRows = 0;
            cntr = 1;
            logEveryNRows = HiveConf.getLongVar(hconf, HiveConf.ConfVars.HIVE_LOG_N_RECORDS);

            List<ExprNodeDesc> keys = conf.getKeyCols();

            if (LOG.isDebugEnabled()) {
                LOG.debug("keys size is " + keys.size());
                for (ExprNodeDesc k : keys) {
                    LOG.debug("Key exprNodeDesc " + k.getExprString());
                }
            }

            keyEval = new ExprNodeEvaluator[keys.size()];
            int i = 0;
            for (ExprNodeDesc e : keys) {
                if (e instanceof ExprNodeConstantDesc
                        && (BUCKET_NUMBER_COL_NAME).equals(((ExprNodeConstantDesc) e).getValue())) {
                    buckColIdxInKeyForSdpo = i;
                }
                keyEval[i++] = ExprNodeEvaluatorFactory.get(e);
            }

            numDistributionKeys = conf.getNumDistributionKeys();
            distinctColIndices = conf.getDistinctColumnIndices();
            numDistinctExprs = distinctColIndices.size();

            valueEval = new ExprNodeEvaluator[conf.getValueCols().size()];
            i = 0;
            for (ExprNodeDesc e : conf.getValueCols()) {
                valueEval[i++] = ExprNodeEvaluatorFactory.get(e);
            }

            partitionEval = new ExprNodeEvaluator[conf.getPartitionCols().size()];
            i = 0;
            for (ExprNodeDesc e : conf.getPartitionCols()) {
                int index = ExprNodeDescUtils.indexOf(e, keys);
                partitionEval[i++] = index < 0 ? ExprNodeEvaluatorFactory.get(e) : keyEval[index];
            }

            if (conf.getBucketCols() != null && !conf.getBucketCols().isEmpty()) {
                bucketEval = new ExprNodeEvaluator[conf.getBucketCols().size()];

                i = 0;
                for (ExprNodeDesc e : conf.getBucketCols()) {
                    int index = ExprNodeDescUtils.indexOf(e, keys);
                    bucketEval[i++] = index < 0 ? ExprNodeEvaluatorFactory.get(e) : keyEval[index];
                }

                buckColIdxInKey = conf.getPartitionCols().size();
            }

            int tag = conf.getTag();
            tagByte[0] = (byte) tag;
            isSkipTag = conf.getSkipTag();
            if (LOG.isInfoEnabled()) {
                LOG.info("Using tag = " + tag);
            }

            TableDesc keyTableDesc = conf.getKeySerializeInfo();
            keySerializer = (Serializer) keyTableDesc.getDeserializerClass().newInstance();
            keySerializer.initialize(null, keyTableDesc.getProperties());
            keyIsText = keySerializer.getSerializedClass().equals(Text.class);
            ((OmniVecBatchSerDe) (keySerializer)).setFieldId(keyFieldId);

            TableDesc valueTableDesc = conf.getValueSerializeInfo();
            valueSerializer = (Serializer) valueTableDesc.getDeserializerClass().newInstance();
            valueSerializer.initialize(null, valueTableDesc.getProperties());
            ((OmniVecBatchSerDe) (valueSerializer)).setFieldId(valueFieldId);

            int limit = conf.getTopN();
            float memUsage = conf.getTopNMemoryUsage();

            if (limit >= 0 && memUsage > 0) {
                reducerHash = conf.isPTFReduceSink() ? new PTFTopNHash() : new TopNHash();
                reducerHash.initialize(limit, memUsage, conf.isMapGroupBy(), this, conf, hconf);
            }

            isUseUniformHash = conf.getReducerTraits().contains(UNIFORM);

            isFirstRow = true;
            // isAcidOp flag has to be checked to use JAVA hash which works like
            // identity function for integers, necessary to read RecordIdentifier
            // incase of ACID updates/deletes.
            boolean isAcidOp = conf.getWriteType() == AcidUtils.Operation.UPDATE
                    || conf.getWriteType() == AcidUtils.Operation.DELETE;
            hashFunc = bucketingVersion == 2 && !isAcidOp
                    ? ObjectInspectorUtils::getBucketHashCode
                    : ObjectInspectorUtils::getBucketHashCodeOld;
            if (isNeedProject) {
                generatorProject();
            }
            getDecimal128ConvertDecimal64Cols();
        } catch (Exception e) {
            String msg = "Error initializing ReduceSinkOperator: " + e.getMessage();
            LOG.error(msg, e);
            throw new RuntimeException(e);
        }
        int inputFieldNum = ((StructObjectInspector) inputObjInspectors[0]).getAllStructFieldRefs().size();
        vecWrappers = new VecWrapper[isNeedProject
                ? conf.getKeyCols().size() + conf.getValueCols().size()
                : inputFieldNum];
        for (int i = 0; i < vecWrappers.length; i++) {
            vecWrappers[i] = new VecWrapper();
        }
    }

    private String extractNumberPart(ExprNodeColumnDesc col) {
        String columnName = col.getColumn();
        return columnName.replaceAll("\\D+", "");
    }

    private void getDecimal128ConvertDecimal64Cols() {
        ArrayList<ExprNodeDesc> allCols = new ArrayList<>(conf.getKeyCols());
        allCols.addAll(conf.getValueCols());
        List<? extends StructField> allStructFieldRefs =
                ((StructObjectInspector) inputObjInspectors[0]).getAllStructFieldRefs();
        List<ExprNodeDesc> sortedAllCols = allCols.stream().filter(col -> col instanceof ExprNodeColumnDesc)
                .filter(col -> !extractNumberPart((ExprNodeColumnDesc) col).isEmpty())
                .sorted(Comparator.comparingInt(col -> Integer.parseInt(extractNumberPart((ExprNodeColumnDesc) col))))
                .collect(Collectors.toList());
        if (allStructFieldRefs.size() != sortedAllCols.size()) {
            return;
        }
        for (int i = 0; i < allStructFieldRefs.size(); i++) {
            ObjectInspector fieldObjectInspector = allStructFieldRefs.get(i).getFieldObjectInspector();
            TypeInfo typeInfo = sortedAllCols.get(i).getTypeInfo();
            if (fieldObjectInspector instanceof WritableHiveDecimalObjectInspector
                    && ((DecimalTypeInfo) ((WritableHiveDecimalObjectInspector) fieldObjectInspector).getTypeInfo()).getPrecision() > 18
                    && (typeInfo instanceof DecimalTypeInfo) && ((DecimalTypeInfo) typeInfo).getPrecision() <= 18) {
                decimal128ConvertDecimal64Cols.add(i);
            }
        }
    }

    private void generatorProject() {
        List<? extends StructField> neededFields = ((StructObjectInspector) inputObjInspectors[0])
                .getAllStructFieldRefs();

        DataType[] inputTypes = neededFields.stream()
                .map(field -> TypeUtils
                        .buildInputDataType(((PrimitiveObjectInspector) field.getFieldObjectInspector()).getTypeInfo()))
                .toArray(DataType[]::new);
        List<ExprNodeDesc> keyValueCols = new ArrayList<>(conf.getKeyCols());
        keyValueCols.addAll(conf.getValueCols());
        String[] expressions = new String[keyValueCols.size()];
        for (int i = 0; i < keyValueCols.size(); i++) {
            ExprNodeDesc exprNodeDesc = keyValueCols.get(i);
            if (exprNodeDesc instanceof ExprNodeGenericFuncDesc) {
                expressions[i] = ExpressionUtils.build((ExprNodeGenericFuncDesc) exprNodeDesc, inputObjInspectors[0])
                        .toString();
            } else {
                BaseExpression node = ExpressionUtils.createNode(exprNodeDesc, inputObjInspectors[0]);
                if (node != null) {
                    expressions[i] = node.toString();
                } else {
                    expressions[i] = null;
                }
            }
        }
        OmniProjectOperatorFactory projectOperatorFactory = new OmniProjectOperatorFactory(expressions, inputTypes, 1,
                new OperatorConfig(SpillConfig.NONE, new OverflowConfig(), true));
        this.projectOperator = projectOperatorFactory.createOperator();
        for (int i = 0; i < conf.getKeyCols().size(); i++) {
            keyFieldId[i] = i;
        }
        ((OmniVecBatchSerDe) (keySerializer)).setFieldId(keyFieldId);
        for (int i = 0; i < conf.getValueCols().size(); i++) {
            valueFieldId[i] = i + conf.getKeyCols().size();
        }
        ((OmniVecBatchSerDe) (valueSerializer)).setFieldId(valueFieldId);
    }

    /**
     * Initializes array of ExprNodeEvaluator. Adds Union field for distinct
     * column indices for group by.
     * Puts the return values into a StructObjectInspector with output column
     * names.
     * <p>
     * If distinctColIndices is empty, the object inspector is same as
     * {@link Operator#initEvaluatorsAndReturnStruct(ExprNodeEvaluator[], List, ObjectInspector)}
     */
    protected static StructObjectInspector initEvaluatorsAndReturnStruct(ExprNodeEvaluator[] evals,
                                                                         List<List<Integer>> distinctColIndices,
                                                                         List<String> outputColNames, int length,
                                                                         ObjectInspector rowInspector) throws HiveException {
        int inspectorLen = evals.length > length ? length + 1 : evals.length;
        List<ObjectInspector> sois = new ArrayList<ObjectInspector>(inspectorLen);

        // keys
        ObjectInspector[] fieldObjectInspectors = initEvaluators(evals, 0, length, rowInspector);
        sois.addAll(Arrays.asList(fieldObjectInspectors));

        if (outputColNames.size() > length) {
            // union keys
            assert distinctColIndices != null;
            List<ObjectInspector> uois = new ArrayList<ObjectInspector>();
            for (List<Integer> distinctCols : distinctColIndices) {
                List<String> names = new ArrayList<String>();
                List<ObjectInspector> eois = new ArrayList<ObjectInspector>();
                int numExprs = 0;
                for (int i : distinctCols) {
                    names.add(HiveConf.getColumnInternalName(numExprs));
                    eois.add(evals[i].initialize(rowInspector));
                    numExprs++;
                }
                uois.add(ObjectInspectorFactory.getStandardStructObjectInspector(names, eois));
            }
            UnionObjectInspector uoi = ObjectInspectorFactory.getStandardUnionObjectInspector(uois);
            sois.add(uoi);
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(outputColNames, sois);
    }

    private LongVec decimal128VecConvertDecimal64Vec(Decimal128Vec decimal128Vec) {
        int size = decimal128Vec.getSize();
        LongVec longVec = new LongVec(size);
        for (int i = 0; i < size; i++) {
            longVec.set(i, decimal128Vec.getBigInteger(i).longValue());
        }
        return longVec;
    }

    private VecBatch convertVec(VecBatch vecBatch) {
        Vec[] vectors = vecBatch.getVectors();
        for (int i : decimal128ConvertDecimal64Cols) {
            if (vectors[i] instanceof Decimal128Vec) {
                vectors[i] = decimal128VecConvertDecimal64Vec((Decimal128Vec) vectors[i]);
            }
        }
        return new VecBatch(vectors);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(Object row, int tag) throws HiveException {
        VecBatch input = (VecBatch) row;
        if (!decimal128ConvertDecimal64Cols.isEmpty()) {
            input = convertVec(input);
        }
        if (isNeedProject) {
            this.projectOperator.addInput(input);
            Iterator<VecBatch> output = this.projectOperator.getOutput();
            while (output.hasNext()) {
                processVecbatch(output.next(), tag);
            }
        } else {
            processVecbatch(input, tag);
        }
    }

    private void processVecbatch(VecBatch vecBatch, int tag) throws HiveException {
        Vec[] expand = expandDictionary(vecBatch);
        for (int i = 0; i < expand.length; i++) {
            vecWrappers[i].isNull = expand[i].getRawValueNulls();
            vecWrappers[i].value = expand[i].getValuesBuf().getBytes(0, expand[i].getValuesBuf().getCapacity());
            if (expand[i] instanceof VariableWidthVec) {
                vecWrappers[i].offset = ((VariableWidthVec) expand[i]).getRawValueOffset();
            }
        }
        for (int i = 0; i < vecBatch.getRowCount(); ++i) {
            for (int j = 0; j < expand.length; j++) {
                vecWrappers[j].index = i;
            }
            this.perProcess(expand, tag, i);
        }
        vecBatch.close();
        for (Vec vector : expand) {
            if (!vector.isClosed()) {
                vector.close();
            }
        }
    }

    private static Vec[] expandDictionary(VecBatch vecBatch) {
        Vec[] vecs = new Vec[vecBatch.getVectorCount()];
        for (int i = 0; i < vecBatch.getVectorCount(); ++i) {
            Vec vector = vecBatch.getVector(i);
            if (vector instanceof DictionaryVec) {
                vecs[i] = ((DictionaryVec) vector).expandDictionary();
                vector.close();
            } else {
                vecs[i] = vector;
            }
        }
        return vecs;
    }

    private void perProcess(Vec[] row, int tag, int index) throws HiveException {
        try {
            ObjectInspector rowInspector = inputObjInspectors[tag];
            if (isFirstRow) {
                isFirstRow = false;
                // TODO: this is fishy - we init object inspectors based on first tag. We
                // should either init for each tag, or if rowInspector doesn't really
                // matter, then we can create this in ctor and get rid of firstRow.
                List<String> fileNames = new ArrayList<>();
                List<ObjectInspector> objectInspectors = new ArrayList<>();
                if (rowInspector instanceof StandardStructObjectInspector) {
                    List<? extends StructField> allStructFieldRefs = ((StandardStructObjectInspector) rowInspector)
                            .getAllStructFieldRefs();
                    for (StructField structField : allStructFieldRefs) {
                        fileNames.add(structField.getFieldName());
                        if (structField.getFieldObjectInspector() instanceof WritableDateObjectInspector) {
                            PrimitiveTypeInfo primitiveTypeInfo = new PrimitiveTypeInfo();
                            primitiveTypeInfo.setTypeName("bigint");
                            ObjectInspector objectInspector = TypeInfoUtils
                                    .getStandardWritableObjectInspectorFromTypeInfo(primitiveTypeInfo);
                            objectInspectors.add(objectInspector);
                        } else {
                            objectInspectors.add(structField.getFieldObjectInspector());
                        }
                    }
                }
                StandardStructObjectInspector standardStructObjectInspector = ObjectInspectorFactory
                        .getStandardStructObjectInspector(fileNames, objectInspectors);
                if (LOG.isInfoEnabled()) {
                    LOG.info("keys are " + conf.getOutputKeyColumnNames() + " num distributions: "
                            + conf.getNumDistributionKeys());
                }
                keyObjectInspector = initEvaluatorsAndReturnStruct(keyEval, distinctColIndices,
                        conf.getOutputKeyColumnNames(), numDistributionKeys, rowInspector);
                valueObjectInspector = initEvaluatorsAndReturnStruct(valueEval, conf.getOutputValueColumnNames(),
                        rowInspector);
                partitionObjectInspectors = initEvaluators(partitionEval, standardStructObjectInspector);
                if (bucketEval != null) {
                    bucketObjectInspectors = initEvaluators(bucketEval, rowInspector);
                }
                int numKeys = numDistinctExprs > 0 ? numDistinctExprs : 1;
                int keyLen = numDistinctExprs > 0 ? numDistributionKeys + 1 : numDistributionKeys;
                cachedKeys = new Object[numKeys][keyLen];
                cachedValues = new Object[valueEval.length];
            }

            // Determine distKeyLength (w/o distincts), and then add the first if present.
            // populateCachedDistributionKeys(row, index);

            // replace bucketing columns with hashcode % numBuckets
            int bucketNumber = -1;
            if (bucketEval != null) {
                bucketNumber = computeBucketNumber(row, conf.getNumBuckets(), index);
                cachedKeys[0][buckColIdxInKey] = new Text(String.valueOf(bucketNumber));
            }
            if (buckColIdxInKeyForSdpo != -1) {
                cachedKeys[0][buckColIdxInKeyForSdpo] = new Text(String.valueOf(bucketNumber));
            }
            HiveKey firstKey = toHiveKey(vecWrappers, tag, null);
            int distKeyLength = firstKey.getDistKeyLength();
            if (numDistinctExprs > 0) {
                populateCachedDistinctKeys(row, 0);
                firstKey = toHiveKey(cachedKeys[0], tag, distKeyLength);
            }

            final int hashCode;

            // distKeyLength doesn't include tag, but includes buckNum in cachedKeys[0]
            if (isUseUniformHash && partitionEval.length > 0) {
                hashCode = computeMurmurHash(firstKey);
            } else {
                hashCode = computeHashCode(row, bucketNumber, index);
            }
            firstKey.setHashCode(hashCode);

            /*
             * in case of TopN for windowing, we need to distinguish between rows with
             * null partition keys and rows with value 0 for partition keys.
             */
            boolean isPartKeyNull = conf.isPTFReduceSink() && partitionKeysAreNull(row, index);

            // Try to store the first key.
            // if TopNHashes aren't active, always forward
            // if TopNHashes are active, proceed if not already excluded (i.e order by limit)
            final int firstIndex = (reducerHash != null)
                    ? reducerHash.tryStoreKey(firstKey, isPartKeyNull)
                    : TopNHash.FORWARD;
            if (firstIndex == TopNHash.EXCLUDE) {
                return; // Nothing to do.
            }
            // Compute value and hashcode - we'd either store or forward them.

            // each time serialize each row, because of different key.

            BytesWritable value = (BytesWritable) valueSerializer.serialize(vecWrappers, valueObjectInspector);

            if (firstIndex == TopNHash.FORWARD) {
                collect(firstKey, value);
            } else {
                // invariant: reducerHash != null
                assert firstIndex >= 0;
                reducerHash.storeValue(firstIndex, firstKey.hashCode(), value, false);
            }

            // All other distinct keys will just be forwarded. This could be optimized...
            for (int i = 1; i < numDistinctExprs; i++) {
                System.arraycopy(cachedKeys[0], 0, cachedKeys[i], 0, numDistributionKeys);
                populateCachedDistinctKeys(row, i);
                HiveKey hiveKey = toHiveKey(cachedKeys[i], tag, distKeyLength);
                hiveKey.setHashCode(hashCode);
                collect(hiveKey, value);
            }
        } catch (HiveException e) {
            throw e;
        } catch (Exception e) {
            throw new HiveException(e);
        }
    }

    private boolean ifContainsValue(int index) {
        long[] values = this.valueFieldId;

        for (long l : values) {
            if (l == (long) index) {
                return true;
            }
        }

        return false;
    }

    private int computeBucketNumber(Object row, int numBuckets, int index) throws HiveException, SerDeException {
        Object[] bucketFieldValues = new Object[bucketEval.length];
        for (int i = 0; i < bucketEval.length; i++) {
            Vec vector = ((Vec[]) row)[(int) keyFieldId[i]];
            bucketFieldValues[i] = getVecValue(vector, (int) keyFieldId[i], index);
        }
        return ObjectInspectorUtils.getBucketNumber(hashFunc.apply(bucketFieldValues, bucketObjectInspectors),
                numBuckets);
    }

    private void populateCachedDistributionKeys(Object row, int index) throws HiveException {
        for (int i = 0; i < numDistributionKeys; i++) {
            Vec vector = ((Vec[]) row)[(int) keyFieldId[i]];
            cachedKeys[0][i] = getVecValue(vector, (int) keyFieldId[i], index);
        }
        if (cachedKeys[0].length > numDistributionKeys) {
            cachedKeys[0][numDistributionKeys] = null;
        }
    }

    private Object getVecValue(Vec vector, int keyFieldId, int index) {
        // index 0 means row 1, currently vecBatch has only one row.
        if (vector.isNull(index)) {
            return null;
        }
        DataType type = vector.getType();
        if (type.getId() == OMNI_INT) {
            return ((IntVec) vector).get(index);
        }
        if (type.getId() == OMNI_LONG) {
            return ((LongVec) vector).get(index);
        }
        if (type.getId() == OMNI_DOUBLE) {
            return ((DoubleVec) vector).get(index);
        }
        if (type.getId() == OMNI_BOOLEAN) {
            return ((BooleanVec) vector).get(index);
        }
        if (type.getId() == OMNI_SHORT) {
            return ((ShortVec) vector).get(index);
        }

        if (type.getId() == OMNI_DECIMAL128) {
            byte[] result = ((Decimal128Vec) vector).getBytes(index);
            StructField structField = ((StandardStructObjectInspector) inputObjInspectors[0]).getAllStructFieldRefs()
                    .get(keyFieldId);
            WritableHiveDecimalObjectInspector objectInspector = (WritableHiveDecimalObjectInspector) structField
                    .getFieldObjectInspector();
            DecimalTypeInfo typeInfo = (DecimalTypeInfo) objectInspector.getTypeInfo();
            return HiveDecimal.createFromBigIntegerBytesAndScale(result, typeInfo.getScale());
        }
        if (type.getId() == OMNI_VARCHAR) {
            return new Text(((VarcharVec) vector).get(index));
        }
        if (type.getId() == OMNI_CHAR) {
            Text text = new Text(((VarcharVec) vector).get(index));
            String s = text.toString();
            StringUtils.stripEnd(s, " ");
            text.set(s);
            return text;
        }
        return null;
    }

    /**
     * Populate distinct keys part of cachedKeys for a particular row.
     */
    private void populateCachedDistinctKeys(Object row, int index) throws HiveException {
        StandardUnionObjectInspector.StandardUnion union;
        cachedKeys[index][numDistributionKeys] = union = new StandardUnionObjectInspector.StandardUnion((byte) index,
                new Object[distinctColIndices.get(index).size()]);
        Object[] distinctParameters = (Object[]) union.getObject();
        for (int distinctParamI = 0; distinctParamI < distinctParameters.length; distinctParamI++) {
            distinctParameters[distinctParamI] = keyEval[distinctColIndices.get(index).get(distinctParamI)]
                    .evaluate(row);
        }
        union.setTag((byte) index);
    }

    protected final int computeMurmurHash(HiveKey firstKey) {
        return Murmur3.hash32(firstKey.getBytes(), firstKey.getDistKeyLength(), 0);
    }

    /**
     * For Acid Update/Delete case, we expect a single partitionEval of the form
     * UDFToInteger(ROW__ID) and buckNum == -1 so that the result of this method
     * is to return the bucketId extracted from ROW__ID unless it optimized by
     * {@link org.apache.hadoop.hive.ql.optimizer.SortedDynPartitionOptimizer}
     */
    private int computeHashCode(Object row, int buckNum, int index) throws HiveException {
        // Evaluate the HashCode
        int keyHashCode = 0;
        if (partitionEval.length == 0) {
            // If no partition cols, just distribute the data uniformly
            // to provide better load balance. If the requirement is to have a single
            // reducer, we should
            // set the number of reducers to 1. Use a constant seed to make the code
            // deterministic.
            if (random == null) {
                random = new Random(12345);
            }
            keyHashCode = random.nextInt();
        } else {
            Object[] bucketFieldValues = new Object[partitionEval.length];
            for (int i = 0; i < partitionEval.length; i++) {
                // Vec vector = ((VecBatch) row).getVector((int) keyFieldId[i]);
                // bucketFieldValues[i] = getVecValueWrapHive(vector, (int) keyFieldId[i]);
                Vec vector = ((Vec[]) row)[(int) keyFieldId[i]];
                bucketFieldValues[i] = getVecValueWrapHive(vector, (int) keyFieldId[i], index);
            }
            keyHashCode = hashFunc.apply(bucketFieldValues, partitionObjectInspectors);
        }
        int hashCode = buckNum < 0 ? keyHashCode : keyHashCode * 31 + buckNum;
        if (LOG.isTraceEnabled()) {
            LOG.trace("Going to return hash code " + hashCode);
        }
        return hashCode;
    }

    private Object getVecValueWrapHive(Vec vector, int indexInVector, int index) {
        if (vector.isNull(index)) {
            return null;
        } else {
            ObjectInspector fieldObjectInspector = ((StandardStructObjectInspector) this.inputObjInspectors[0])
                    .getAllStructFieldRefs().get(indexInVector).getFieldObjectInspector();
            return fieldObjectInspector instanceof AbstractPrimitiveJavaObjectInspector
                    ? this.getOriginValue(vector, index, fieldObjectInspector)
                    : this.getWritableValue(vector, index, fieldObjectInspector);
        }
    }

    private Object getOriginValue(Vec vector, int index, ObjectInspector fieldObjectInspector) {
        PrimitiveObjectInspector.PrimitiveCategory primitiveCategory =
                ((PrimitiveObjectInspector) fieldObjectInspector).getPrimitiveCategory();
        DataType type = vector.getType();
        switch (primitiveCategory) {
            case INT:
                return fieldObjectInspector.getTypeName().equals("date")
                        ? Date.ofEpochDay(((IntVec) vector).get(index))
                        : ((IntVec) vector).get(index);
            case LONG:
                return ((LongVec) vector).get(index);
            case DOUBLE:
                return ((DoubleVec) vector).get(index);
            case BOOLEAN:
                return ((BooleanVec) vector).get(index);
            case SHORT:
                return ((ShortVec) vector).get(index);
            case DECIMAL: {
                DecimalTypeInfo typeInfo =
                        (DecimalTypeInfo) ((WritableHiveDecimalObjectInspector) fieldObjectInspector).getTypeInfo();
                if (type.getId() == OMNI_LONG) {
                    long value = ((LongVec) vector).get(index);
                    return HiveDecimal.create(value, typeInfo.getScale());
                }
                byte[] result = ((Decimal128Vec) vector).getBytes(index);
                return HiveDecimal.createFromBigIntegerBytesAndScale(result, typeInfo.getScale());
            }
            case VARCHAR:
                return new Text(((VarcharVec) vector).get(index));
            case CHAR: {
                Text text = new Text(((VarcharVec) vector).get(index));
                String s = text.toString();
                StringUtils.stripEnd(s, " ");
                text.set(s);
                return text;
            }
            default:
                return null;
        }
    }

    private Object getWritableValue(Vec vector, int index, ObjectInspector fieldObjectInspector) {
        PrimitiveObjectInspector.PrimitiveCategory primitiveCategory =
                ((PrimitiveObjectInspector) fieldObjectInspector).getPrimitiveCategory();
        DataType type = vector.getType();
        switch (primitiveCategory) {
            case INT:
                return fieldObjectInspector.getTypeName().equals("date")
                        ? new LongWritable(((IntVec) vector).get(index))
                        : new IntWritable(((IntVec) vector).get(index));
            case LONG:
                return new LongWritable(((LongVec) vector).get(index));
            case DOUBLE:
                return new DoubleWritable(((DoubleVec) vector).get(index));
            case BOOLEAN:
                return new BooleanWritable(((BooleanVec) vector).get(index));
            case SHORT:
                return new ShortWritable(((ShortVec) vector).get(index));
            case DECIMAL: {
                DecimalTypeInfo typeInfo =
                        (DecimalTypeInfo) ((WritableHiveDecimalObjectInspector) fieldObjectInspector).getTypeInfo();
                if (type.getId() == OMNI_LONG) {
                    long value = ((LongVec) vector).get(index);
                    HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable();
                    hiveDecimalWritable.setFromLongAndScale(value, typeInfo.getScale());
                    return hiveDecimalWritable;
                }
                byte[] result = ((Decimal128Vec) vector).getBytes(index);
                return new HiveDecimalWritable(result, typeInfo.getScale());
            }
            case VARCHAR: {
                if (fieldObjectInspector instanceof LazyHiveVarcharObjectInspector) {
                    LazyHiveVarchar lazyHiveVarchar
                            = new LazyHiveVarchar((LazyHiveVarcharObjectInspector) fieldObjectInspector);
                    ByteArrayRef byteArrayRef = new ByteArrayRef();
                    byteArrayRef.setData(((VarcharVec) vector).get(index));
                    lazyHiveVarchar.init(byteArrayRef, 0, ((VarcharVec) vector).get(index).length);
                    return lazyHiveVarchar;
                } else if (fieldObjectInspector instanceof LazyStringObjectInspector) {
                    LazyString lazyString = new LazyString((LazyStringObjectInspector) fieldObjectInspector);
                    ByteArrayRef byteArrayRef = new ByteArrayRef();
                    byteArrayRef.setData(((VarcharVec) vector).get(index));
                    lazyString.init(byteArrayRef, 0, ((VarcharVec) vector).get(index).length);
                    return lazyString;
                } else {
                    return new Text(((VarcharVec) vector).get(index));
                }
            }
            case CHAR: {
                if (fieldObjectInspector instanceof LazyHiveCharObjectInspector) {
                    LazyHiveChar lazyHiveChar = new LazyHiveChar((LazyHiveCharObjectInspector) fieldObjectInspector);
                    ByteArrayRef byteArrayRef = new ByteArrayRef();
                    byteArrayRef.setData(((VarcharVec) vector).get(index));
                    lazyHiveChar.init(byteArrayRef, 0, ((VarcharVec) vector).get(index).length);
                    return lazyHiveChar;
                } else if (fieldObjectInspector instanceof LazyStringObjectInspector) {
                    LazyString lazyString = new LazyString((LazyStringObjectInspector) fieldObjectInspector);
                    ByteArrayRef byteArrayRef = new ByteArrayRef();
                    byteArrayRef.setData(((VarcharVec) vector).get(index));
                    lazyString.init(byteArrayRef, 0, ((VarcharVec) vector).get(index).length);
                    return lazyString;
                } else {
                    Text text = new Text(((VarcharVec) vector).get(index));
                    String s = text.toString();
                    StringUtils.stripEnd(s, " ");
                    text.set(s);
                    return text;
                }
            }
            default:
                return null;
        }
    }

    private boolean partitionKeysAreNull(Object row, int index) throws HiveException {
        if (partitionEval.length != 0) {
            for (int i = 0; i < partitionEval.length; i++) {
                Vec vector = ((Vec[]) row)[(int) keyFieldId[i]];
                Object o = getVecValue(vector, (int) keyFieldId[i], index);
                if (o != null) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Serialize the keys and append the tag
     */
    protected HiveKey toHiveKey(Object obj, int tag, Integer distLength) throws SerDeException {
        BinaryComparable key = (BinaryComparable) keySerializer.serialize(obj, keyObjectInspector);
        int keyLength = key.getLength();
        if (tag == -1 || isSkipTag) {
            keyWritable.set(key.getBytes(), 0, keyLength);
        } else {
            keyWritable.setSize(keyLength);
            System.arraycopy(key.getBytes(), 0, keyWritable.get(), 0, keyLength);
        }
        keyWritable.setDistKeyLength((distLength == null) ? keyLength : distLength);
        return keyWritable;
    }

    @Override
    public void collect(byte[] key, byte[] value, int hash) throws IOException {
        HiveKey keyWritable = new HiveKey(key, hash);
        BytesWritable valueWritable = new BytesWritable(value);
        collect(keyWritable, valueWritable);
    }

    protected void collect(BytesWritable keyWritable, Writable valueWritable) throws IOException {
        // Since this is a terminal operator, update counters explicitly -
        // forward is not called
        if (out != null) {
            numRows++;
            runTimeNumRows++;
            if (LOG.isTraceEnabled()) {
                if (numRows == cntr) {
                    cntr = logEveryNRows == 0 ? cntr * 10 : numRows + logEveryNRows;
                    if (cntr < 0 || numRows < 0) {
                        cntr = 0;
                        numRows = 1;
                    }
                    LOG.info(toString() + ": records written - " + numRows);
                }
            }
            out.collect(keyWritable, valueWritable);
        }
    }

    private BytesWritable makeValueWritable(Object row) throws Exception {
        int length = valueEval.length;

        // Evaluate the value
        for (int i = 0; i < length; i++) {
            cachedValues[i] = valueEval[i].evaluate(row);
        }

        // Serialize the value
        return (BytesWritable) valueSerializer.serialize(cachedValues, valueObjectInspector);
    }

    @Override
    protected void closeOp(boolean isAbort) throws HiveException {
        if (!isAbort && reducerHash != null) {
            reducerHash.flush();
        }
        runTimeNumRows = numRows;
        super.closeOp(isAbort);
        out = null;
        random = null;
        reducerHash = null;
        if (LOG.isTraceEnabled()) {
            LOG.info(toString() + ": records written - " + numRows);
        }
    }

    /**
     * @return the name of the operator
     */
    @Override
    public String getName() {
        return "RS_OMNI";
    }

    @Override
    public OperatorType getType() {
        return OperatorType.REDUCESINK;
    }

    @Override
    public boolean opAllowedBeforeMapJoin() {
        return false;
    }

    public void setSkipTag(boolean isSkipTag) {
        this.isSkipTag = isSkipTag;
    }

    public void setValueIndex(int[] valueIndex) {
        this.valueIndex = valueIndex;
    }

    public int[] getValueIndex() {
        return valueIndex;
    }

    public void setInputAliases(String[] inputAliases) {
        this.inputAliases = inputAliases;
    }

    public String[] getInputAliases() {
        return inputAliases;
    }

    @Override
    public boolean getIsReduceSink() {
        return true;
    }

    @Override
    public String getReduceOutputName() {
        return conf.getOutputName();
    }

    @Override
    public void setOutputCollector(OutputCollector out) {
        this.out = out;
    }

    public ReduceSinkDesc getConf() {
        return new OmniReduceSinkDesc(this.conf);
    }
}
