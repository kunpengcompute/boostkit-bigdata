
package com.huawei.boostkit.hive;

import static com.huawei.boostkit.hive.cache.VectorCache.BATCH;

import com.huawei.boostkit.hive.cache.VecBufferCache;
import com.huawei.boostkit.hive.cache.VectorCache;
import com.huawei.boostkit.hive.expression.TypeUtils;
import com.huawei.boostkit.hive.shuffle.VecSerdeBody;

import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.sort.OmniSortOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.tez.RecordSource;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class OmniVectorWithSortOperator extends OmniHiveOperator<OmniVectorDesc> {
    public transient VectorCache[] vectorCache;

    public transient VecBufferCache[] vecBufferCaches;

    private transient List<StructField>[] flatFields;

    private transient RecordSource[] source;

    private transient boolean[] fetchDone;

    private transient boolean allFetchDone;

    private int[] rowCount;

    private transient StructObjectInspector[] soi;
    private transient List<StructField>[] fields;
    private transient int[] keyFiledNum;

    private transient OmniSortOperatorFactory[] sortOperatorFactories;

    private transient OmniOperator[] sortOperators;

    public transient Iterator<VecBatch>[] outputs;

    private int posBigTable;

    public OmniVectorWithSortOperator() {
        super();
    }

    public OmniVectorWithSortOperator(CompilationOpContext ctx, OmniVectorDesc conf) {
        super(ctx);
        this.conf = conf;
    }

    @Override
    protected void initializeOp(Configuration hconf) throws HiveException {
        super.initializeOp(hconf);
        source = ((TezContext) MapredContext.get()).getRecordSources();
        vecBufferCaches = new VecBufferCache[inputObjInspectors.length];
        flatFields = new List[inputObjInspectors.length];
        fetchDone = new boolean[inputObjInspectors.length];
        rowCount = new int[inputObjInspectors.length];
        soi = new StructObjectInspector[inputObjInspectors.length];
        fields = new List[inputObjInspectors.length];
        keyFiledNum = new int[inputObjInspectors.length];
        outputs = new Iterator[inputObjInspectors.length];

        for (int i = 0; i < inputObjInspectors.length; i++) {
            soi[i] = (StructObjectInspector) inputObjInspectors[i];
        }
        for (int i = 0; i < soi.length; i++) {
            fields[i] = soi[i].getAllStructFieldRefs().stream().map(field -> (StructField) field)
                    .collect(Collectors.toList());
            keyFiledNum[i] = ((StructObjectInspector) fields[i].get(0).getFieldObjectInspector())
                    .getAllStructFieldRefs().size();
            flatFields[i] = fields[i].stream().flatMap(
                    field -> ((StructObjectInspector) field.getFieldObjectInspector()).getAllStructFieldRefs().stream())
                    .collect(Collectors.toList());
            List<TypeInfo> typeInfos = flatFields[i].stream()
                    .map(field -> ((PrimitiveObjectInspector) field.getFieldObjectInspector()).getTypeInfo())
                    .collect(Collectors.toList());
            vecBufferCaches[i] = new VecBufferCache(flatFields[i].size(), typeInfos);
        }
        List<String> fieldNames = new ArrayList<>();
        for (int i = 0; i < inputObjInspectors.length; i++) {
            fieldNames.add(String.valueOf(i));
        }
        outputObjInspector = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, Arrays.asList(soi));
        generateSortOperator();
        posBigTable = ((OmniMergeJoinOperator) childOperators.get(0)).getPosBigTable();
        allFetchDone = false;
    }

    private void generateSortOperator() {
        sortOperatorFactories = new OmniSortOperatorFactory[inputObjInspectors.length];
        sortOperators = new OmniOperator[inputObjInspectors.length];
        for (int i = 0; i < sortOperators.length; i++) {
            DataType[] inputTypes = new DataType[flatFields[i].size()];
            int[] outputCols = new int[flatFields[i].size()];
            for (int j = 0; j < flatFields[i].size(); j++) {
                PrimitiveTypeInfo typeInfo = ((PrimitiveObjectInspector) flatFields[i].get(j).getFieldObjectInspector())
                        .getTypeInfo();
                inputTypes[j] = TypeUtils.buildInputDataType(typeInfo);
                outputCols[j] = j;
            }
            String[] sortColumns = new String[keyFiledNum[i]];
            for (int j = 0; j < keyFiledNum[i]; j++) {
                sortColumns[j] = "#" + j;
            }
            int[] sortAscendings = new int[keyFiledNum[i]];
            int[] sortNullFirsts = new int[keyFiledNum[i]];
            Arrays.fill(sortAscendings, 1);
            Arrays.fill(sortNullFirsts, 1);
            sortOperatorFactories[i] = new OmniSortOperatorFactory(inputTypes, outputCols, sortColumns, sortAscendings,
                    sortNullFirsts);
            sortOperators[i] = sortOperatorFactories[i].createOperator();
        }
    }

    @Override
    public void process(Object row, int tag) throws HiveException {
        if (tag == posBigTable && !allFetchDone) {
            for (int i = 0; i < source.length; i++) {
                if (i == posBigTable) {
                    continue;
                }
                while (!fetchDone[i]) {
                    fetchDone[i] = !source[i].pushRecord();
                }
            }
            allFetchDone = true;
        }
        for (int i = 0; i < fields[tag].size(); i++) {
            Object structFieldData = soi[tag].getStructFieldData(row, fields[tag].get(i));
            VecSerdeBody[] vecSerdeBodies = (VecSerdeBody[]) structFieldData;
            if (vecSerdeBodies == null) {
                continue;
            }
            vecBufferCaches[tag].addVecSerdeBody(vecSerdeBodies, rowCount[tag], i == 1 ? keyFiledNum[tag] : 0);
        }
        rowCount[tag]++;
        if (rowCount[tag] < BATCH) {
            return;
        }
        this.sortOperators[tag]
                .addInput(new VecBatch(vecBufferCaches[tag].getValueVecBatchCache(rowCount[tag]), rowCount[tag]));
        rowCount[tag] = 0;
    }

    @Override
    public void close(boolean abort) throws HiveException {
        for (int i = 0; i < rowCount.length; i++) {
            if (rowCount[i] > 0) {
                this.sortOperators[i]
                        .addInput(new VecBatch(vecBufferCaches[i].getValueVecBatchCache(rowCount[i]), rowCount[i]));
                rowCount[i] = 0;
            }
        }
        for (int i = 0; i < sortOperators.length; i++) {
            outputs[i] = sortOperators[i].getOutput();
        }
        VecBatch vecBatch = outputs[posBigTable].next();
        if (vecBatch == null) {
            return;
        }
        forward(outputs[posBigTable].next(), posBigTable);
        super.close(abort);
    }

    public void pushRecord(int tag) throws HiveException {
        forward(outputs[tag].next(), tag);
    }

    @Override
    public String getName() {
        return OmniVectorOperator.getOperatorName();
    }

    public static String getOperatorName() {
        return "OMNI-VEC";
    }

    @Override
    public OperatorType getType() {
        return null;
    }
}