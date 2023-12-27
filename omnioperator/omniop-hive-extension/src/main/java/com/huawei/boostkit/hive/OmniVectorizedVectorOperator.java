
package com.huawei.boostkit.hive;

import static com.huawei.boostkit.hive.OmniExecuteWithHookContext.OMNI_OPERATOR;
import static com.huawei.boostkit.hive.OmniVectorOperator.convertLazyToJavaInspector;
import static com.huawei.boostkit.hive.cache.VectorCache.BATCH;
import static com.huawei.boostkit.hive.converter.Decimal64VecConverter.isConvertedDecimal64;
import static com.huawei.boostkit.hive.converter.VecConverter.CONVERTER_MAP;
import static org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch.DEFAULT_SIZE;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;

import com.huawei.boostkit.hive.cache.BytesColumnCache;
import com.huawei.boostkit.hive.cache.ColumnCache;
import com.huawei.boostkit.hive.cache.DecimalColumnCache;
import com.huawei.boostkit.hive.cache.DoubleColumnCache;
import com.huawei.boostkit.hive.cache.LongColumnCache;
import com.huawei.boostkit.hive.cache.VecBufferCache;
import com.huawei.boostkit.hive.converter.Decimal64VecConverter;
import com.huawei.boostkit.hive.converter.VecConverter;
import com.huawei.boostkit.hive.shuffle.OmniVecBatchSerDe;
import com.huawei.boostkit.hive.shuffle.VecSerdeBody;

import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapJoinBaseOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContextRegion;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OmniVectorizedVectorOperator extends OmniHiveOperator<OmniVectorDesc> {
    private boolean isToVector;

    private VectorizationContext vectorizationContext;

    private transient VecConverter[] converters;

    private int rowCount;

    private transient VectorizedRowBatch vectorizedRowBatch;

    private transient VectorizedRowBatchCtx rbCtx;

    private transient PrimitiveObjectInspector[] projectedColumnInspectors;

    private transient List<Integer> projectedColumns;

    private transient ColumnCache[] columnCaches;

    private transient VecBufferCache vecBufferCache;

    private transient int keyFieldNum;

    private transient boolean isVecBatchSerDe;

    public OmniVectorizedVectorOperator() {
        super();
    }

    public OmniVectorizedVectorOperator(CompilationOpContext ctx) {
        super(ctx);
    }

    public OmniVectorizedVectorOperator(CompilationOpContext ctx, OmniVectorDesc conf,
            VectorizationContext vectorizationContext, VectorizedRowBatchCtx rbCtx) {
        super(ctx);
        this.conf = conf;
        this.isToVector = conf.getIsToVector();
        this.vectorizationContext = vectorizationContext;
        this.rbCtx = rbCtx;
    }

    p

    @Override
    protected void initializeOp(Configuration hconf) throws HiveException {
        if (OMNI_OPERATOR.contains(OperatorType.REDUCESINK) && parentOperators.isEmpty()) {
            rebuildVectoriedInspector();
            ReduceWork reduceWork = Utilities.getReduceWork(hconf);
            if (reduceWork != null && reduceWork.getKeyDesc().getProperties().get(SERIALIZATION_LIB)
                    .equals(OmniVecBatchSerDe.class.getName())) {
                isVecBatchSerDe = true;
            }
        }
        super.initializeOp(hconf);
        if (!isToVector && !OMNI_OPERATOR.contains(OperatorType.TABLESCAN) && checkParentTableScan()) {
            rebuildTableScanInspector(hconf);
        }
        StructObjectInspector bigTableInspector;
        if (parentOperators.isEmpty() && childOperators.get(0).getType().equals(OperatorType.MAPJOIN)
                && ((MapJoinDesc) childOperators.get(0).getConf()).isDynamicPartitionHashJoin()) {
            rebuildMapjoinInspector();
            int posBigTable = ((MapJoinDesc) childOperators.get(0).getConf()).getPosBigTable();
            bigTableInspector = (StructObjectInspector) inputObjInspectors[posBigTable];
        } else {
            bigTableInspector = (StructObjectInspector) outputObjInspector;
        }
        if (!parentOperators.isEmpty() && parentOperators.get(0).getType() != null
                && parentOperators.get(0).getType().equals(OperatorType.HASHTABLEDUMMY)) {
            return;
        }
        rowCount = 0;
        projectedColumns = new ArrayList<>();
        rbCtx = getVectorizedRowBatchCtx(hconf);
        vectorizedRowBatch = rbCtx.createVectorizedRowBatch();
        for (int i = 0; i < vectorizationContext.getProjectionColumnNames().size(); i++) {
            if (!vectorizationContext.getProjectionColumnNames().get(i).equals("ROW__ID")
                    && vectorizedRowBatch.cols[vectorizationContext.getProjectedColumns().get(i)] != null) {
                projectedColumns.add(vectorizationContext.getProjectedColumns().get(i));
            }
        }
        converters = new VecConverter[projectedColumns.size()];
        projectedColumnInspectors = new PrimitiveObjectInspector[projectedColumns.size()];
        vectorizedRowBatch.projectionSize = projectedColumns.size();
        vectorizedRowBatch.projectedColumns = projectedColumns.stream().mapToInt(Integer::valueOf).toArray();
        List<ObjectInspector> inspectors = new ArrayList<>();
        converters = bigTableInspector.getAllStructFieldRefs().stream()
                .filter(field -> !field.getFieldName().equals("row__id")).map(field -> {
                    PrimitiveTypeInfo primitiveTypeInfo = ((PrimitiveObjectInspector) field.getFieldObjectInspector())
                            .getTypeInfo();
                    inspectors.add(field.getFieldObjectInspector());
                    if (isConvertedDecimal64(field.getFieldName(), vectorizationContext)) {
                        return new Decimal64VecConverter();
                    }
                    return CONVERTER_MAP.get(primitiveTypeInfo.getPrimitiveCategory());
                }).toArray(VecConverter[]::new);
        projectedColumnInspectors = inspectors.toArray(new PrimitiveObjectInspector[0]);
        columnCaches = bigTableInspector.getAllStructFieldRefs().stream()
                .filter(field -> !field.getFieldName().equals("row__id")).map(field -> getColumnCache(field))
                .toArray(ColumnCache[]::new);
        if (isVecBatchSerDe) {
            List<TypeInfo> typeInfos = Arrays.stream(projectedColumnInspectors)
                    .map(inspector -> inspector.getTypeInfo()).collect(Collectors.toList());
            vecBufferCache = new VecBufferCache(columnCaches.length, typeInfos);
        }
    }

    private boolean checkParentTableScan() {
        if (parentOperators.isEmpty()) {
            return false;
        }
        if (parentOperators.get(0).getType().equals(OperatorType.TABLESCAN)) {
            return true;
        }
        if (parentOperators.get(0).getType().equals(OperatorType.FILTER)
                && parentOperators.get(0).getParentOperators().get(0).getType() != null
                && parentOperators.get(0).getParentOperators().get(0).getType().equals(OperatorType.TABLESCAN)) {
            return true;
        }
        return false;
    }

    private void rebuildMapjoinInspector() throws HiveException {
        int posBigTable = ((MapJoinDesc) childOperators.get(0).getConf()).getPosBigTable();
        ObjectInspector fieldObjectInspector = ((StructObjectInspector) inputObjInspectors[posBigTable])
                .getAllStructFieldRefs().get(0).getFieldObjectInspector();
        if (fieldObjectInspector instanceof StructObjectInspector) {
            keyFieldNum = ((StructObjectInspector) ((StructObjectInspector) inputObjInspectors[posBigTable])
                    .getAllStructFieldRefs().get(0).getFieldObjectInspector()).getAllStructFieldRefs().size();
            for (int i = 0; i < inputObjInspectors.length; i++) {
                if (i == posBigTable && !OMNI_OPERATOR.contains(OperatorType.REDUCESINK)) {
                    continue;
                }
                StructObjectInspector structObjectInspector = (StructObjectInspector) inputObjInspectors[i];
                inputObjInspectors[i] = Utilities.constructVectorizedReduceRowOI(
                        (StructObjectInspector) structObjectInspector.getAllStructFieldRefs().get(0)
                                .getFieldObjectInspector(),
                        (StructObjectInspector) structObjectInspector.getAllStructFieldRefs().get(1)
                                .getFieldObjectInspector());
            }
        } else {
            keyFieldNum = 1;
        }
        List<String> fieldNames = new ArrayList<>();
        for (int i = 0; i < inputObjInspectors.length; i++) {
            fieldNames.add(String.valueOf(i));
        }
        outputObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                Arrays.asList(inputObjInspectors));
    }

    private void rebuildTableScanInspector(Configuration hconf) {
        MapWork mapWork = Utilities.getMapWork(hconf);
        TableScanOperator tableScanOperator = (TableScanOperator) mapWork.getAliasToWork()
                .get(mapWork.getAliases().get(0));
        Set<String> partColumnNames = mapWork.getAliasToPartnInfo().get(tableScanOperator.getConf().getAlias())
                .getPartSpec().keySet();
        List<StructField> neededFields = new ArrayList<>();
        Set<Integer> neededColumnIDs = new HashSet<>(tableScanOperator.getNeededColumnIDs());
        List<? extends StructField> allStructFieldRefs = ((StandardStructObjectInspector) inputObjInspectors[0])
                .getAllStructFieldRefs();
        for (int i = 0; i < allStructFieldRefs.size(); i++) {
            if (neededColumnIDs.contains(i) || partColumnNames.contains(allStructFieldRefs.get(i).getFieldName())) {
                neededFields.add(allStructFieldRefs.get(i));
            }
        }
        outputObjInspector = convertLazyToJavaInspector(neededFields);
    }

    private void rebuildVectoriedInspector() throws HiveException {
        if (inputObjInspectors.length > 1) {
            return;
        }
        StandardStructObjectInspector structObjectInspector = (StandardStructObjectInspector) inputObjInspectors[0];
        if (structObjectInspector.getOriginalColumnNames().contains("KEY")) {
            keyFieldNum = ((StandardStructObjectInspector) structObjectInspector.getAllStructFieldRefs().get(0)
                    .getFieldObjectInspector()).getAllStructFieldRefs().size();
            expandInputInspector(0);
        }
        outputObjInspector = inputObjInspectors[0];
    }

    private void expandInputInspector(int index) throws HiveException {
        StructObjectInspector structObjectInspector = (StructObjectInspector) inputObjInspectors[index];
        inputObjInspectors[index] = Utilities.constructVectorizedReduceRowOI(
                (StructObjectInspector) structObjectInspector.getAllStructFieldRefs().get(0).getFieldObjectInspector(),
                (StructObjectInspector) structObjectInspector.getAllStructFieldRefs().get(1).getFieldObjectInspector());
    }

    private Object getColumnCache(StructField field) {
        PrimitiveTypeInfo primitiveTypeInfo = ((PrimitiveObjectInspector) field.getFieldObjectInspector())
                .getTypeInfo();
        if (isConvertedDecimal64(field.getFieldName(), vectorizationContext)) {
            return new LongColumnCache();
        }
        switch (primitiveTypeInfo.getPrimitiveCategory()) {
            case LONG:
            case INT:
            case TIMESTAMP:
            case DATE:
            case SHORT:
            case BYTE:
            case BOOLEAN:
                return new LongColumnCache();
            case DECIMAL:
                return new DecimalColumnCache();
            case VARCHAR:
            case CHAR:
            case STRING:
                return new BytesColumnCache();
            case FLOAT:
            case DOUBLE:
                return new DoubleColumnCache();
            default:
                return null;
        }
    }

    private VectorizedRowBatchCtx getVectorizedRowBatchCtx(Configuration hconf) throws HiveException {
        VectorizedRowBatchCtx vectorizedRowBatchCtx = findChangedCtx(parentOperators);
        if (vectorizedRowBatchCtx == null) {
            if (HiveConf.getBoolVar(hconf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED)
                    && Utilities.getPlanPath(hconf) != null) {
                MapWork mapWork = Utilities.getMapWork(hconf);
                if (mapWork != null) {
                    vectorizedRowBatchCtx = mapWork.getVectorizedRowBatchCtx();
                }
            }
        }
        if (vectorizedRowBatchCtx == null) {
            ReduceWork reduceWork = Utilities.getReduceWork(hconf);
            vectorizedRowBatchCtx = reduceWork.getVectorizedRowBatchCtx();
        }
        return vectorizedRowBatchCtx;
    }

    private VectorizedRowBatchCtx findChangedCtx(List<Operator<? extends OperatorDesc>> parentOperators)
            throws HiveException {
        for (Operator parent : parentOperators) {
            if (parent.getType() != null && parent.getType().equals(OperatorType.GROUPBY)) {
                VectorizationContext parentContext = ((VectorizationContextRegion) parent)
                        .getOutputVectorizationContext();
                GroupByDesc groupByDesc = (GroupByDesc) parent.getConf();
                DataTypePhysicalVariation[] rowDataTypePhysicalVariations = new DataTypePhysicalVariation[parentContext
                        .getInitialColumnNames().size()];
                for (int i = 0; i < rowDataTypePhysicalVariations.length; i++) {
                    rowDataTypePhysicalVariations[i] = parentContext.getDataTypePhysicalVariation(i);
                }
                return new VectorizedRowBatchCtx(groupByDesc.getOutputColumnNames().toArray(new String[0]),
                        parentContext.getInitialTypeInfos(), rowDataTypePhysicalVariations, /* dataColumnNums */ null,
                        /* partitionColumnCount */ 0, /* virtualColumnCount */ 0, /* neededVirtualColumns */ null,
                        parentContext.getScratchColumnTypeNames(),
                        parentContext.getScratchDataTypePhysicalVariations());
            } else if (parent instanceof VectorMapJoinBaseOperator
                    || (parent instanceof OmniMapJoinOperator && ((OmniMapJoinOperator) parent).isChangedCtx())) {
                VectorizationContext parentContext = ((VectorizationContextRegion) parent)
                        .getOutputVectorizationContext();
                VectorizedRowBatchCtx vectorizedRowBatchCtx = new VectorizedRowBatchCtx();
                vectorizedRowBatchCtx.init((StructObjectInspector) parent.getOutputObjInspector(),
                        parentContext.getScratchColumnTypeNames(),
                        parentContext.getScratchDataTypePhysicalVariations());
                return vectorizedRowBatchCtx;
            }
            VectorizedRowBatchCtx next = findChangedCtx(parent.getParentOperators());
            if (next != null) {
                return next;
            }
        }
        return null;
    }

    @Override
    public void process(Object row, int tag) throws HiveException {
        if (!(row instanceof VecBatch) && !isToVector || row instanceof VecBatch && isToVector) {
            throw new HiveException("isToVector is not right");
        }
        if (isToVector) {
            if (isVecBatchSerDe) {
                dealVecBatchSerDeData((List<Object>) row);
                return;
            }
            VectorizedRowBatch vectorizedRowBatch = (VectorizedRowBatch) row;
            if (vectorizedRowBatch.size == 0) {
                return;
            }
            for (int i = 0; i < projectedColumns.size(); i++) {
                converters[i].setValueFromColumnVector(vectorizedRowBatch, projectedColumns.get(i), columnCaches[i], i,
                        rowCount, projectedColumnInspectors[i].getTypeInfo());
            }
            rowCount = rowCount + vectorizedRowBatch.size;
            if (rowCount < BATCH - DEFAULT_SIZE) {
                return;
            }
            forwardNext();
            rowCount = 0;
        } else {
            VecBatch vecBatch = (VecBatch) row;
            Vec[] vecs = vecBatch.getVectors();
            int num = vecBatch.getRowCount() / DEFAULT_SIZE;
            for (int j = 0; j < num; j++) {
                for (int i = 0; i < vecs.length; i++) {
                    vectorizedRowBatch.cols[vectorizedRowBatch.projectedColumns[i]] = converters[i]
                            .getColumnVectorFromOmniVec(vecs[i], j * DEFAULT_SIZE, (j + 1) * DEFAULT_SIZE,
                                    projectedColumnInspectors[i]);
                }
                vectorizedRowBatch.size = DEFAULT_SIZE;
                forward(vectorizedRowBatch, null);
            }
            if (num * DEFAULT_SIZE < vecBatch.getRowCount()) {
                for (int i = 0; i < vecs.length; i++) {
                    vectorizedRowBatch.cols[vectorizedRowBatch.projectedColumns[i]] = converters[i]
                            .getColumnVectorFromOmniVec(vecs[i], num * DEFAULT_SIZE, vecBatch.getRowCount(),
                                    projectedColumnInspectors[i]);
                }
                vectorizedRowBatch.size = vecBatch.getRowCount() - num * DEFAULT_SIZE;
                forward(vectorizedRowBatch, null);
            }
            vecBatch.releaseAllVectors();
            vecBatch.close();
        }
    }

    private void dealVecBatchSerDeData(List<Object> input) throws HiveException {
        if (input.get(0) != null) {
            vecBufferCache.addVecSerdeBody((VecSerdeBody[]) input.get(0), rowCount, 0);
        }
        if (input.get(1) != null) {
            vecBufferCache.addVecSerdeBody((VecSerdeBody[]) input.get(1), rowCount, keyFieldNum);
        }
        ++rowCount;
        if (rowCount < BATCH) {
            return;
        }
        forwardNext();
        rowCount = 0;
        return;
    }

    protected void forwardNext() throws HiveException {
        if (vecBufferCache != null) {
            Vec[] valueVecBatchCache = vecBufferCache.getValueVecBatchCache(rowCount);
            forward(new VecBatch(valueVecBatchCache, rowCount), 0);
            return;
        }
        Vec[] vecs = new Vec[projectedColumns.size()];
        IntStream.range(0, projectedColumns.size()).forEach(i -> {
            vecs[i] = converters[i].toOmniVec(columnCaches[i], rowCount, projectedColumnInspectors[i].getTypeInfo());
        });
        for (ColumnCache columnCache : columnCaches) {
            columnCache.reset();
        }
        VecBatch vecBatch = new VecBatch(vecs, rowCount);
        forward(vecBatch, 0);
    }

    @Override
    public String getName() {
        return OmniVectorOperator.getOperatorName();
    }

    @Override
    public OperatorType getType() {
        return null;
    }

    @Override
    public void close(boolean abort) throws HiveException {
        // here to process the remaining data in the cache
        if (rowCount > 0) {
            forwardNext();
            rowCount = 0;
        }
        super.close(abort);
    }
}