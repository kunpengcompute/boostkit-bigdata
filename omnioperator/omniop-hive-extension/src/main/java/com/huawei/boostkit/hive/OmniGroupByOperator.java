package com.huawei.boostkit.hive;

import com.huawei.boostkit.hive.cache.VectorCache;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;
import javolution.util.FastBitSet;
import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.aggregator.OmniHashAggregationWithExprOperatorFactory;
import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.operator.config.OverflowConfig;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.vector.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationDesc;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContextRegion;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;

import java.io.Serializable;
import java.util.*;

import static com.huawei.boostkit.hive.expression.TypeUtils.buildInputDataType;
import static nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_COUNT_ALL;
import static org.apache.hadoop.hive.ql.exec.GroupByOperator.groupingSet2BitSet;

public class OmniGroupByOperator extends OmniHiveOperator<OmniGroupByDesc> implements Serializable, VectorizationContextRegion {
    private static final long serialVeserssionUID = 1L;
    private transient OmniHashAggregationWithExprOperatorFactory omniHashAggregationWithExprOperatorFactory;
    private transient OmniOperator omniOperator;
    private transient List<ExprNodeEvaluator> keyFields;
    private transient boolean firstRow;
    private transient List<List<ExprNodeEvaluator>> aggChannelFields;
    private transient ObjectInspector[] keyObjectInspectors;

    // current key ObjectInspectors are standard ObjectInspectors
    private transient ObjectInspector[] currentKeyObjectInspectors;
    private transient ExprNodeEvaluator[][] aggregationParameterFields;
    private transient ObjectInspector[][] aggregationParameterObjectInspectors;
    private transient GenericUDAFEvaluator[] aggregationEvaluators;
    private transient List<? extends StructField> allStructFieldRefs;
    private transient List<? extends StructField> keyStructFieldRefs;
    private transient int numKeys;
    private transient List<? extends StructField> valueStructFieldRefs;
    private transient boolean groupingSetsPresent;
    private transient int groupingSetsPosition;
    private transient List<Long> groupingSets;
    private transient FastBitSet[] groupingSetsBitSet;
    private transient LongWritable[] newKeysGroupingSets;
    private transient int outputKeyLength;
    private VectorizationContext vOutContext;
    private transient ObjectInspector[] aggOutputObjectInspectors;
    private transient ObjectInspector[] objectInspectors;
    private transient ArrayList<AggregationDesc> aggs;
    private transient VectorAggregationDesc[] vecAggrDescs;
    private transient List<String> outputFieldNames;
    private transient List<Integer> constantColumnIds;
    private transient List<ExprNodeConstantEvaluator> constantEvaluators;
    private transient Vec[] constantVec;

    public OmniGroupByOperator() {
        super();
    }

    public OmniGroupByOperator(CompilationOpContext ctx) {
        super(ctx);
    }

    public OmniGroupByOperator(CompilationOpContext ctx, GroupByDesc conf, VectorizationContext vectorizationContext, VectorizationContext vOutContext) {
        super(ctx);
        this.conf = new OmniGroupByDesc(conf);
        this.vOutContext = vOutContext;
    }

    private void commonInitialize(Configuration hconf) throws HiveException {
        allStructFieldRefs = ((StructObjectInspector) inputObjInspectors[0]).getAllStructFieldRefs();
        numKeys = conf.getKeys().size();
        keyFields = new ArrayList<>();
        constantColumnIds = new ArrayList<>();
        constantEvaluators = new ArrayList<>();
        keyObjectInspectors = new ObjectInspector[numKeys];
        currentKeyObjectInspectors = new ObjectInspector[numKeys];
        for (int i = 0; i < numKeys; i++) {
            keyFields.add(ExprNodeEvaluatorFactory.get(conf.getKeys().get(i), hconf));
        }

        aggs = conf.getAggregators();
        int aggSize = aggs.size();
        aggregationParameterFields = new ExprNodeEvaluator[aggSize][];
        aggregationParameterObjectInspectors = new ObjectInspector[aggSize][];
        aggChannelFields = new ArrayList<>();
        for (int i = 0; i < aggSize; i++) {
            AggregationDesc agg = aggs.get(i);
            ArrayList<ExprNodeDesc> parameters = agg.getParameters();
            aggregationParameterFields[i] = new ExprNodeEvaluator[parameters.size()];
            aggregationParameterObjectInspectors[i] = new ObjectInspector[parameters.size()];
            List<ExprNodeEvaluator> exprNodes = new ArrayList<>();
            for (int j = 0; j < parameters.size(); j++) {
                aggregationParameterFields[i][j] = ExprNodeEvaluatorFactory.get(parameters.get(j), hconf);
                exprNodes.add(aggregationParameterFields[i][j]);
            }
            aggChannelFields.add(exprNodes);
        }
        outputKeyLength = conf.pruneGroupingSetId() ? numKeys - 1 : numKeys;
        outputFieldNames = new ArrayList<>(conf.getOutputColumnNames());
    }

    private void nonVectorizedInitialize() throws HiveException {
        ObjectInspector rowInspector = inputObjInspectors[0];
        keyObjectInspectors = new ObjectInspector[numKeys];
        currentKeyObjectInspectors = new ObjectInspector[numKeys];
        for (int i = 0; i < numKeys; i++) {
            keyObjectInspectors[i] = keyFields.get(i).initialize(rowInspector);
            currentKeyObjectInspectors[i] = ObjectInspectorUtils.getStandardObjectInspector(keyObjectInspectors[i], ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);
        }
        int aggSize = aggs.size();
        for (int i = 0; i < aggSize; i++) {
            AggregationDesc agg = aggs.get(i);
            ArrayList<ExprNodeDesc> parameters = agg.getParameters();
            for (int j = 0; j < parameters.size(); j++) {
                aggregationParameterObjectInspectors[i][j] = aggregationParameterFields[i][j].initialize(rowInspector);
            }
        }

        // build outputDataType
        aggregationEvaluators = new GenericUDAFEvaluator[conf.getAggregators().size()];
        for (int i = 0; i < aggregationEvaluators.length; i++) {
            AggregationDesc agg = conf.getAggregators().get(i);
            aggregationEvaluators[i] = agg.getGenericUDAFEvaluator();
        }

        // init outputObjectInspectors
        objectInspectors = new ObjectInspector[outputKeyLength + aggregationEvaluators.length];
        for (int i = 0; i < outputKeyLength; i++) {
            objectInspectors[i] = currentKeyObjectInspectors[i];
        }
        for (int i = 0; i < aggregationEvaluators.length; i++) {
            objectInspectors[outputKeyLength + i] = aggregationEvaluators[i].init(conf.getAggregators().get(i).getMode(), aggregationParameterObjectInspectors[i]);
        }
        aggOutputObjectInspectors = Arrays.copyOfRange(objectInspectors, outputKeyLength, outputKeyLength + aggregationEvaluators.length);
    }

    private void vectorizedInitialize() throws HiveException {
        final int aggregateCount = vecAggrDescs.length;
        objectInspectors = new ObjectInspector[outputKeyLength + aggregateCount];
        List<ExprNodeDesc> keyDesc = conf.getKeys();
        for (int i = 0; i < outputKeyLength; i++) {
            VectorExpressionWriter vew = VectorExpressionWriterFactory.genVectorExpressionWritable(keyDesc.get(i));
            objectInspectors[i] = vew.getObjectInspector();
        }
        aggOutputObjectInspectors = new ObjectInspector[aggregateCount];
        for (int i = 0; i < aggregateCount; i++) {
            ObjectInspector objInsp = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(vecAggrDescs[i].getOutputTypeInfo());
            aggOutputObjectInspectors[i] = objInsp;
            objectInspectors[i + outputKeyLength] = objInsp;
        }
    }

    private void createOmniOperator() {
        List<String> aggOutputFieldsNames = outputFieldNames.subList(outputFieldNames.size() - aggs.size(),
                outputFieldNames.size());
        StandardStructObjectInspector aggOutputObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(aggOutputFieldsNames, Arrays.asList(aggOutputObjectInspectors));
        List<? extends StructField> aggOutputFields = aggOutputObjInspector.getAllStructFieldRefs();

        FunctionType[] aggFunctionTypes = getFunctionTypeFromAggs(aggs);
        DataType[][] aggOutputTypes = getTwoDimenOutputDataType(aggOutputFields);
        String[] aggChannelsFilter = {null};
        OverflowConfig overflowConfig = new OverflowConfig(OverflowConfig.OverflowConfigId.OVERFLOW_CONFIG_NULL);
        OperatorConfig operatorConfig = new OperatorConfig(overflowConfig);
        boolean[] isInputRaws = getIsInputRaws(aggs);
        boolean[] isOutputPartials = getIsOutputPartials(aggs);
        String[] groupByChanel;
        String[][] aggChannels;
        DataType[] sourceTypes;

        // Initialize the constants for the grouping sets, so that they can be re-used for each row
        groupingSetsPresent = conf.isGroupingSetsPresent();
        if (groupingSetsPresent) {
            groupingSets = conf.getListGroupingSets();
            groupingSetsPosition = conf.getGroupingSetPosition();
            newKeysGroupingSets = new LongWritable[groupingSets.size()];
            groupingSetsBitSet = new FastBitSet[groupingSets.size()];
            int pos = 0;
            for (Long groupingSet : groupingSets) {
                // Create the mapping corresponding to the grouping set
                newKeysGroupingSets[pos] = new LongWritable(groupingSet);
                groupingSetsBitSet[pos] = groupingSet2BitSet(groupingSet, groupingSetsPosition);
            }
        }

        if ((allStructFieldRefs.size() == 2) && (allStructFieldRefs.get(0).getFieldName().equals("key"))) {
            keyStructFieldRefs = ((StandardStructObjectInspector) allStructFieldRefs.get(0).getFieldObjectInspector()).getAllStructFieldRefs();
            valueStructFieldRefs = ((StandardStructObjectInspector) allStructFieldRefs.get(1).getFieldObjectInspector()).getAllStructFieldRefs();
            groupByChanel = getExprFromStructField(keyStructFieldRefs);
            aggChannels = getTwoDimenExprFromStructField(valueStructFieldRefs);
            sourceTypes = getDataTypeFromStructField(keyStructFieldRefs, valueStructFieldRefs);
        } else {
            groupByChanel = getExprFromExprNode(keyFields, false);
            aggChannels = getTwoDimenExprFromExprNode(aggChannelFields);
            sourceTypes = getDataTypeFromStructField(allStructFieldRefs);
        }
        omniHashAggregationWithExprOperatorFactory = new OmniHashAggregationWithExprOperatorFactory(groupByChanel, aggChannels,
                aggChannelsFilter, sourceTypes, aggFunctionTypes, aggOutputTypes, isInputRaws, isOutputPartials, operatorConfig);
        omniOperator = omniHashAggregationWithExprOperatorFactory.createOperator();
    }

    @Override
    protected void initializeOp(Configuration hconf) throws HiveException {
        super.initializeOp(hconf);
        commonInitialize(hconf);
        if (this.conf.getVectorDesc() == null) {
            nonVectorizedInitialize();
        } else {
            this.vecAggrDescs = ((VectorGroupByDesc) this.conf.getVectorDesc()).getVecAggrDescs();
            vectorizedInitialize();
        }
        outputObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(outputFieldNames, Arrays.asList(objectInspectors));
        createOmniOperator();
        firstRow = true;
        constantVec = new Vec[constantColumnIds.size()];
        for (int i = 0; i < constantVec.length; i++) {
            constantVec[i] = createConstantVec(constantEvaluators.get(i), VectorCache.BATCH);
        }
    }

    private boolean[] getIsInputRaws(ArrayList<AggregationDesc> aggs) {
        int size = aggs.size();
        boolean[] isInputRaws = new boolean[size];
        for (int i = 0; i < size; i++) {
            if (aggs.get(i).getMode() == GenericUDAFEvaluator.Mode.PARTIAL1 ||
                    aggs.get(i).getMode() == GenericUDAFEvaluator.Mode.COMPLETE) {
                isInputRaws[i] = true;
            } else {
                isInputRaws[i] = false;
            }
        }
        return isInputRaws;
    }

    private boolean[] getIsOutputPartials(ArrayList<AggregationDesc> aggs) {
        int size = aggs.size();
        boolean[] isOutputPartials = new boolean[size];
        for (int i = 0; i < size; i++) {
            if (aggs.get(i).getMode() == GenericUDAFEvaluator.Mode.PARTIAL1 ||
                    aggs.get(i).getMode() == GenericUDAFEvaluator.Mode.PARTIAL2) {
                isOutputPartials[i] = true;
            } else {
                isOutputPartials[i] = false;
            }
        }
        return isOutputPartials;
    }

    private String[] getExprFromStructField(List<? extends StructField> structFields) {
        List<String> expressions = new ArrayList<>();
        for (StructField structField : structFields) {
            expressions.add(TypeUtils.buildExpression(
                    ((PrimitiveObjectInspector) structField.getFieldObjectInspector()).getTypeInfo(),
                    structField.getFieldID()));
        }
        return expressions.toArray(new String[0]);
    }

    private String[][] getTwoDimenExprFromStructField(List<? extends StructField> structFields) {
        List<String[]> expressions = new ArrayList<>();
        for (StructField structField : structFields) {
            List<String> expr = new ArrayList<>();
            expr.add(TypeUtils.buildExpression(
                    ((PrimitiveObjectInspector) structField.getFieldObjectInspector()).getTypeInfo(),
                    structField.getFieldID() + numKeys));
            expressions.add(expr.toArray(new String[0]));
        }
        return expressions.toArray(new String[0][0]);
    }

    private DataType[][] getTwoDimenOutputDataType(List<? extends StructField> structFields) {
        List<DataType[]> twoDimenDataTypes = new ArrayList<>();
        // TODO Confirming the Multidimensional array processing flow
        for (StructField structField : structFields) {
            List<DataType> dataTypes = new ArrayList<>();
            PrimitiveTypeInfo typeInfo = ((PrimitiveObjectInspector) structField.getFieldObjectInspector()).getTypeInfo();
            DataType dataType = buildInputDataType(typeInfo);
            dataTypes.add(dataType);
            twoDimenDataTypes.add(dataTypes.toArray(new DataType[0]));
        }
        return twoDimenDataTypes.toArray(new DataType[0][0]);
    }

    private FunctionType[] getFunctionTypeFromAggs(ArrayList<AggregationDesc> aggs) {
        List<FunctionType> functionTypes = new ArrayList<>();
        for (AggregationDesc agg : aggs) {
            // For AggFun count(*)
            if (agg.getGenericUDAFName().equals("count") && agg.getParameters().size() == 0) {
                functionTypes.add(OMNI_AGGREGATION_TYPE_COUNT_ALL);
            } else {
                functionTypes.add(TypeUtils.getAggFunctionTypeFromName(agg));
            }
        }
        return functionTypes.toArray(new FunctionType[0]);
    }

    private List<DataType> getListDataTypeFromStructField(List<? extends StructField> fieldRefs) {
        List<DataType> dataTypes = new ArrayList<>();
        for (int i = 0; i < fieldRefs.size(); i++) {
            if (fieldRefs.get(i).getFieldObjectInspector() instanceof PrimitiveObjectInspector) {
                PrimitiveTypeInfo typeInfo = ((PrimitiveObjectInspector) fieldRefs.get(i).getFieldObjectInspector()).getTypeInfo();
                dataTypes.add(buildInputDataType(typeInfo));
            }
        }
        return dataTypes;
    }

    private DataType[] getDataTypeFromStructField(List<? extends StructField> fieldRefs) {
        List<DataType> dataTypes = new ArrayList<>();
        for (ExprNodeEvaluator keyField : keyFields) {
            if (keyField instanceof ExprNodeConstantEvaluator && !groupingSetsPresent) {
                dataTypes.add(buildInputDataType(keyField.getExpr().getTypeInfo()));
            }
        }
        dataTypes.addAll(getListDataTypeFromStructField(fieldRefs));
        if (groupingSetsPresent) {
            dataTypes.add(LongDataType.LONG);
        }
        for (List<ExprNodeEvaluator> aggChannelField : aggChannelFields) {
            for (ExprNodeEvaluator aggField : aggChannelField) {
                if (aggField instanceof ExprNodeConstantEvaluator) {
                    dataTypes.add(buildInputDataType(aggField.getExpr().getTypeInfo()));
                }
            }
        }
        return dataTypes.toArray(new DataType[0]);
    }

    private DataType[] getDataTypeFromStructField(List<? extends StructField> keyStructFieldRefs, List<? extends StructField> valueStructFieldRefs) {
        List<DataType> dataTypes = new ArrayList<>();
        dataTypes.addAll(getListDataTypeFromStructField(keyStructFieldRefs));
        dataTypes.addAll(getListDataTypeFromStructField(valueStructFieldRefs));
        return dataTypes.toArray(new DataType[0]);
    }

    private String[] getExprFromExprNode(List<ExprNodeEvaluator> nodes, boolean isAggChannel) {
        int columnOffset = 0;
        if (isAggChannel && !constantColumnIds.isEmpty()) {
            columnOffset = numKeys;
        }
        List<String> expressions = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i) instanceof ExprNodeGenericFuncEvaluator) {
                expressions.add(ExpressionUtils.build((ExprNodeGenericFuncDesc) nodes.get(i).getExpr(), inputObjInspectors[0]).toString());
            } else if (nodes.get(i) instanceof ExprNodeColumnEvaluator) {
                expressions.add(TypeUtils.buildExpression(nodes.get(i).getExpr().getTypeInfo(), columnOffset +
                        getFieldIdFromFieldName(((ExprNodeColumnEvaluator) nodes.get(i)).getExpr().getColumn())));
            } else if (nodes.get(i) instanceof ExprNodeConstantEvaluator) {
                if (groupingSetsPresent) {
                    expressions.add(TypeUtils.buildExpression(nodes.get(i).getExpr().getTypeInfo(), allStructFieldRefs.size()));
                } else {
                    int columnId = i + columnOffset;
                    expressions.add(TypeUtils.buildExpression(nodes.get(i).getExpr().getTypeInfo(), columnId));
                    constantColumnIds.add(columnId);
                    constantEvaluators.add((ExprNodeConstantEvaluator) nodes.get(i));
                }
            } else {
                throw new IllegalArgumentException("not support ExprNode:" + nodes.get(i).getClass().getSimpleName());
            }
        }
        return expressions.toArray(new String[0]);
    }

    private String[][] getTwoDimenExprFromExprNode(List<List<ExprNodeEvaluator>> nodes) {
        List<String[]> expressions = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i++) {
            if (!nodes.get(i).isEmpty()) {
                expressions.add(getExprFromExprNode(nodes.get(i), true));
            }
        }
        return expressions.toArray(new String[0][0]);
    }

    private int getFieldIdFromFieldName(String name) {
        StructField structField = ((StructObjectInspector) inputObjInspectors[0]).getStructFieldRef(name);
        return structField.getFieldID();
    }

    private Vec expandVec(Vec vec, long mask) {
        int rowCount = vec.getSize();
        int groupingSetSize = groupingSets.size();
        Vec newVec = VecFactory.createFlatVec(rowCount * groupingSetSize, vec.getType());
        Vec flatVec = vec;
        if (vec instanceof DictionaryVec) {
            flatVec = ((DictionaryVec) vec).expandDictionary();
        }
        for (int i = 0; i < groupingSetSize; i++) {
            if ((groupingSets.get(i) & mask) == 0) {
                DataType.DataTypeId dataTypeId = vec.getType().getId();
                newVec.setNulls(i * rowCount, vec.getValuesNulls(0, rowCount), 0, rowCount);
                switch (dataTypeId) {
                    case OMNI_INT:
                    case OMNI_DATE32:
                        ((IntVec) newVec).put(((IntVec) flatVec).get(0, rowCount), i * rowCount, 0, rowCount);
                        break;
                    case OMNI_LONG:
                    case OMNI_DATE64:
                    case OMNI_DECIMAL64:
                        ((LongVec) newVec).put(((LongVec) flatVec).get(0, rowCount), i * rowCount, 0, rowCount);
                        break;
                    case OMNI_DOUBLE:
                        ((DoubleVec) newVec).put(((DoubleVec) flatVec).get(0, rowCount), i * rowCount, 0, rowCount);
                        break;
                    case OMNI_BOOLEAN:
                        ((BooleanVec) newVec).put(((BooleanVec) flatVec).get(0, rowCount), i * rowCount, 0, rowCount);
                        break;
                    case OMNI_SHORT:
                        ((ShortVec) newVec).put(((ShortVec) flatVec).get(0, rowCount), i * rowCount, 0, rowCount);
                        break;
                    case OMNI_DECIMAL128:
                        long[] values = ((Decimal128Vec) flatVec).get(0, rowCount);
                        ((Decimal128Vec) newVec).put(values, i * rowCount, 0, values.length);
                        break;
                    case OMNI_VARCHAR:
                    case OMNI_CHAR:
                        ((VarcharVec) newVec).put(i * rowCount, ((VarcharVec) flatVec).get(0, rowCount), 0,
                                ((VarcharVec) flatVec).getValueOffset(0, rowCount), 0, rowCount);
                        break;
                    default:
                        throw new RuntimeException("Not support dataType, dataTypeId: " + dataTypeId);
                }
            } else {
                boolean[] nulls = new boolean[rowCount];
                Arrays.fill(nulls, true);
                newVec.setNulls(i * rowCount, nulls, 0, rowCount);
            }
        }
        return newVec;
    }

    private Set getAggChannels(List<List<ExprNodeEvaluator>> nodes) {
        Set aggChannels = new HashSet();
        for (List<ExprNodeEvaluator> node : nodes) {
            if (!node.isEmpty()) {
                for (ExprNodeEvaluator exprNodeEvaluator : node) {
                    if (exprNodeEvaluator instanceof ExprNodeColumnEvaluator) {
                        aggChannels.add(getFieldIdFromFieldName(((ExprNodeColumnEvaluator) exprNodeEvaluator).getExpr().getColumn()));
                    }
                }
            }
        }
        return aggChannels;
    }

    private VecBatch expandVecBatch(VecBatch vecBatch) {
        int originalKeyCount = numKeys - 1;
        int vecCount = vecBatch.getVectorCount();
        int rowCount = vecBatch.getRowCount();
        int groupingSetSize = groupingSets.size();
        Vec[] vecs = new Vec[vecCount + 1];

        Set aggChannels = getAggChannels(aggChannelFields);
        for (int keyIndex = 0, i = 0; i < vecCount; i++) {
            if (aggChannels.contains(i)) {
                vecs[i] = expandVec(vecBatch.getVector(i), 0);
            } else {
                vecs[i] = expandVec(vecBatch.getVector(i), 1L << (originalKeyCount - keyIndex - 1));
                keyIndex++;
            }
        }
        LongVec groupingIdVector = new LongVec(rowCount * groupingSetSize);
        for (int i = 0; i < groupingSetSize; i++) {
            long[] groupingArr = new long[rowCount];
            Arrays.fill(groupingArr, groupingSets.get(i));
            groupingIdVector.put(groupingArr, i * rowCount, 0, rowCount);
        }
        vecs[vecCount] = groupingIdVector;
        return new VecBatch(vecs);
    }

    private Vec createConstantVec(ExprNodeConstantEvaluator exprNodeConstantEvaluator, int rowCount) {
        DataType dataType = buildInputDataType(exprNodeConstantEvaluator.getExpr().getTypeInfo());
        Vec newVec = VecFactory.createFlatVec(rowCount, dataType);
        DataType.DataTypeId dataTypeId = dataType.getId();
        for (int i = 0; i < rowCount; i++) {
            switch (dataTypeId) {
                case OMNI_INT:
                case OMNI_DATE32:
                    ((IntVec) newVec).set(i, (int) exprNodeConstantEvaluator.getExpr().getValue());
                    break;
                case OMNI_LONG:
                case OMNI_DATE64:
                case OMNI_DECIMAL64:
                    ((LongVec) newVec).set(i, (long) exprNodeConstantEvaluator.getExpr().getValue());
                    break;
                case OMNI_DOUBLE:
                    ((DoubleVec) newVec).set(i, (double) exprNodeConstantEvaluator.getExpr().getValue());
                    break;
                case OMNI_BOOLEAN:
                    ((BooleanVec) newVec).set(i, (boolean) exprNodeConstantEvaluator.getExpr().getValue());
                    break;
                case OMNI_SHORT:
                    ((ShortVec) newVec).set(i, (short) exprNodeConstantEvaluator.getExpr().getValue());
                    break;
                case OMNI_DECIMAL128:
                    ((Decimal128Vec) newVec).set(i, (long[]) exprNodeConstantEvaluator.getExpr().getValue());
                    break;
                case OMNI_VARCHAR:
                case OMNI_CHAR:
                    ((VarcharVec) newVec).set(i, ((String) exprNodeConstantEvaluator.getExpr().getValue()).getBytes());
                    break;
                default:
                    throw new RuntimeException("Not support dataType, dataTypeId: " + dataTypeId);
            }
        }
        return newVec;
    }

    private VecBatch createConstVecBatch(VecBatch vecBatch) {
        int newVecBatchCount = vecBatch.getVectorCount() + constantColumnIds.size();
        Vec[] vecs = new Vec[newVecBatchCount];
        int constantColumnIndex = 0;
        for (int i = 0; i < newVecBatchCount; i++) {
            if (constantColumnIds.contains(i)) {
                vecs[i] = constantVec[constantColumnIndex].slice(0, vecBatch.getRowCount());
                constantColumnIndex++;
            } else {
                vecs[i] = vecBatch.getVector(i - numKeys);
            }
        }
        return new VecBatch(vecs);
    }

    @Override
    public void process(Object row, int tag) throws HiveException {
        VecBatch input = (VecBatch) row;
        firstRow = false;
        if (groupingSetsPresent) {
            input = expandVecBatch(input);
        }
        if (!constantColumnIds.isEmpty()) {
            input = createConstVecBatch(input);
        }
        this.omniOperator.addInput(input);
    }

    @Override
    public String getName() {
        return "OMNI-GROUPBY";
    }

    @Override
    public OperatorType getType() {
        return OperatorType.GROUPBY;
    }

    private VecBatch removeVector(VecBatch vecBatch, int vecIndex) {
        int vecCount = vecBatch.getVectorCount();
        if (vecIndex >= vecCount || vecIndex < 0) {
            throw new IllegalArgumentException("The vecIndex exceeds the vecBatch size. vecCount: " + vecCount + ", vecIndex: " + vecIndex);
        }
        Vec[] vecs = new Vec[vecCount - 1];
        for (int i = 0, j = 0; j < vecCount; j++) {
            if (j == vecIndex) {
                vecBatch.getVector(j).close();
            } else {
                vecs[i] = vecBatch.getVector(j);
                i++;
            }
        }
        return new VecBatch(vecs);
    }

    private VecBatch createVecBatch(int pos) {
        // This VecBatch has only one row of data, and each column is NULL.
        List<? extends StructField> structFields = ((StandardStructObjectInspector) outputObjInspector).getAllStructFieldRefs();
        int vectorCount = structFields.size();
        Vec[] vecs = new Vec[vectorCount];
        for (int i = 0; i < vectorCount; i++) {
            ObjectInspector fieldObjectInspector = structFields.get(i).getFieldObjectInspector();
            vecs[i] = VecFactory.createFlatVec(1, buildInputDataType(((PrimitiveObjectInspector) fieldObjectInspector).getTypeInfo()));
            if (i == pos && pos < outputKeyLength) {
                ((LongVec) vecs[i]).set(0, (1L << pos) - 1);
            } else {
                vecs[i].setNull(0);
            }
        }
        return new VecBatch(vecs, 1);
    }

    @Override
    public VectorizationContext getOutputVectorizationContext() {
        return vOutContext;
    }

    @Override
    protected void closeOp(boolean abort) throws HiveException {
        if (!abort) {
            // If there is no grouping key and no row came to this operator
            if (firstRow && GroupByOperator.shouldEmitSummaryRow(conf)) {
                firstRow = false;
                int pos = conf.getGroupingSetPosition();
                VecBatch vecBatch = createVecBatch(pos);
                forward(vecBatch, outputObjInspector);
            } else {
                Iterator<VecBatch> output = this.omniOperator.getOutput();
                while (output.hasNext()) {
                    VecBatch next = output.next();
                    if (outputObjInspector instanceof StandardStructObjectInspector
                            && next.getVectorCount() != ((StandardStructObjectInspector) outputObjInspector).getAllStructFieldRefs().size()) {
                        next = removeVector(next, numKeys - 1);
                    }
                    forward(next, outputObjInspector);
                }
            }
        }
        for (Vec vec : constantVec) {
            vec.close();
        }
        omniHashAggregationWithExprOperatorFactory.close();
        omniOperator.close();
        super.closeOp(abort);
    }
}
