
package com.huawei.boostkit.hive;

import static com.huawei.boostkit.hive.expression.TypeUtils.checkOmniJsonWhiteList;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.operator.config.OverflowConfig;
import nova.hetu.omniruntime.operator.config.SpillConfig;
import nova.hetu.omniruntime.operator.project.OmniProjectOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class OmniSelectOperator extends OmniHiveOperator<OmniSelectDesc> implements Serializable {
    private static final long serialVersionUID = 1L;
    private final transient Logger LOG = LoggerFactory.getLogger(getClass().getName());

    private transient OmniProjectOperatorFactory projectOperatorFactory;

    private transient OmniOperator omniOperator;

    private transient boolean isSelectStarNoCompute = false;

    private transient boolean needSliceVector = false;

    private transient Iterator<VecBatch> output;

    private static Cache<Object, Object> cache = CacheBuilder.newBuilder().concurrencyLevel(8).initialCapacity(10)
            .maximumSize(100).recordStats().removalListener(notification -> {
                ((OmniProjectOperatorFactory) notification.getValue()).close();
            }).build();

    private static boolean addedCloseThread;

    public OmniSelectOperator() {
        super();
    }

    public OmniSelectOperator(CompilationOpContext ctx) {
        super(ctx);
    }

    public OmniSelectOperator(CompilationOpContext ctx, SelectDesc conf) {
        super(ctx);
        this.conf = new OmniSelectDesc(conf);
    }

    private boolean checkNoCompute() {
        List<ExprNodeDesc> colList = this.conf.getColList();
        List<? extends StructField> allStructFieldRefs = ((StructObjectInspector) inputObjInspectors[0])
                .getAllStructFieldRefs();
        if (colList.size() != allStructFieldRefs.size()) {
            return false;
        }
        for (int i = 0; i < colList.size(); i++) {
            ExprNodeDesc nodeDesc = colList.get(i);
            if (!(nodeDesc instanceof ExprNodeColumnDesc)) {
                return false;
            }
            StructField structFieldRef = ((StructObjectInspector) inputObjInspectors[0])
                    .getStructFieldRef(nodeDesc.getExprString());
            int fieldId = structFieldRef.getFieldID();
            if (fieldId != i) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected void initializeOp(Configuration hconf) throws HiveException {
        super.initializeOp(hconf);
        checkIfHasBrotherOperators();
        String queryId = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVEQUERYID);
        String cacheKey = queryId + "SELECT_" + this.getOperatorId() + "_container";
        Operator parent = parentOperators.get(0);
        if (parent instanceof OmniVectorOperator && ((OmniVectorOperator) parent).isKeyValue()) {
            inputObjInspectors[0] = expandInspector(inputObjInspectors[0]);
        }
        List<ExprNodeDesc> colList = conf.getColList();

        List<ObjectInspector> colInspector = new ArrayList<>();
        List<Integer> dataTypes = new ArrayList<>();
        List<Integer> colVals = new ArrayList<>();
        List<TypeInfo> typeInfos = new ArrayList<>();

        for (ExprNodeDesc exprNodeDesc : colList) {
            prepareExpression(colInspector, dataTypes, colVals, typeInfos, exprNodeDesc);
        }
        this.outputObjInspector = ObjectInspectorFactory
                .getStandardStructObjectInspector(this.conf.getOutputColumnNames(), colInspector);
        OmniProjectOperatorFactory omniProjectOperatorFactory = (OmniProjectOperatorFactory) cache
                .getIfPresent(cacheKey);
        if (omniProjectOperatorFactory != null) {
            this.projectOperatorFactory = omniProjectOperatorFactory;
            this.omniOperator = this.projectOperatorFactory.createOperator();
            return;
        }
        if (checkNoCompute()) {
            isSelectStarNoCompute = true;
            return;
        }
        String[] expressions = new String[typeInfos.size()];
        for (int i = 0; i < dataTypes.size(); i++) {
            ExprNodeDesc exprNodeDesc = colList.get(i);
            if (exprNodeDesc instanceof ExprNodeGenericFuncDesc) {
                expressions[i] = ExpressionUtils.build((ExprNodeGenericFuncDesc) colList.get(i), inputObjInspectors[0])
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

        List<? extends StructField> allStructFieldRefs = ((StructObjectInspector) inputObjInspectors[0])
                .getAllStructFieldRefs();
        DataType[] inputTypes = new DataType[allStructFieldRefs.size()];
        for (int i = 0; i < allStructFieldRefs.size(); i++) {
            if (allStructFieldRefs.get(i).getFieldObjectInspector() instanceof PrimitiveObjectInspector) {
                PrimitiveTypeInfo typeInfo = ((PrimitiveObjectInspector) allStructFieldRefs.get(i)
                        .getFieldObjectInspector()).getTypeInfo();
                inputTypes[i] = TypeUtils.buildInputDataType(typeInfo);
            }
        }
        this.projectOperatorFactory = new OmniProjectOperatorFactory(expressions, inputTypes, 1, new OperatorConfig(
                SpillConfig.NONE, new OverflowConfig(OverflowConfig.OverflowConfigId.OVERFLOW_CONFIG_NULL), true));
        this.omniOperator = this.projectOperatorFactory.createOperator();
        cache.put(cacheKey, this.projectOperatorFactory);
        if (!addedCloseThread) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                cache.invalidateAll();
            }));
            addedCloseThread = true;
        }
    }

    private void prepareExpression(List<ObjectInspector> colInspector, List<Integer> dataTypes, List<Integer> colVals,
            List<TypeInfo> typeInfos, ExprNodeDesc nodeDesc) {
        ObjectInspector inspector;
        Integer fieldId;
        if (nodeDesc instanceof ExprNodeColumnDesc) {
            StructField structFieldRef = ((StructObjectInspector) inputObjInspectors[0])
                    .getStructFieldRef(nodeDesc.getExprString());
            inspector = structFieldRef.getFieldObjectInspector();
            fieldId = structFieldRef.getFieldID();
        } else if (nodeDesc instanceof ExprNodeGenericFuncDesc) {
            TypeInfo typeinfo = createTypeinfo((ExprNodeGenericFuncDesc) nodeDesc);
            inspector = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(typeinfo);
            fieldId = null;
        } else {
            TypeInfo typeInfo = nodeDesc.getTypeInfo();
            if (typeInfo instanceof CharTypeInfo || typeInfo instanceof VarcharTypeInfo) {
                int length = Optional.ofNullable(TypeUtils.getCharWidth(nodeDesc)).orElse(2000);
                VarcharTypeInfo varcharTypeInfo = new VarcharTypeInfo(length);
                inspector = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(varcharTypeInfo);
            } else {
                inspector = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(typeInfo);
            }
            // todo select constant. note: find way to get length
            // int length =
            // Optional.ofNullable(TypeUtils.getCharWidth(nodeDesc)).orElse(2000);
            // VarcharTypeInfo varcharTypeInfo = new VarcharTypeInfo(length);
            // inspector =
            // TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(varcharTypeInfo);
            fieldId = null;
        }
        colInspector.add(inspector);
        dataTypes.add(TypeUtils.convertHiveTypeToOmniType(nodeDesc.getTypeInfo()));
        typeInfos.add(nodeDesc.getTypeInfo());
        colVals.add(fieldId);
    }

    private void checkIfHasBrotherOperators() {
        List<Operator<? extends OperatorDesc>> parentOperators = this.getParentOperators();
        List<Operator<? extends OperatorDesc>> childOperators = parentOperators.get(0).getChildOperators();
        if (childOperators.get(childOperators.size() - 1) != this) {
            needSliceVector = true;
        }
    }

    @Override
    public void process(Object row, int tag) throws HiveException {
        if (isSelectStarNoCompute) {
            forward(row, inputObjInspectors[tag]);
            return;
        }
        VecBatch input = (VecBatch) row;
        if (needSliceVector) {
            Vec[] vectors = input.getVectors();
            Vec[] copyVectors = new Vec[vectors.length];
            for (int i = 0; i < vectors.length; i++) {
                copyVectors[i] = vectors[i].slice(0, vectors[i].getSize());
            }
            VecBatch copyVecBatch = new VecBatch(copyVectors, input.getRowCount());
            this.omniOperator.addInput(copyVecBatch);
            output = this.omniOperator.getOutput();
            while (output.hasNext()) {
                forward(output.next(), outputObjInspector);
            }
            return;
        }
        this.omniOperator.addInput(input);
        output = this.omniOperator.getOutput();
        while (output.hasNext()) {
            forward(output.next(), outputObjInspector);
        }
    }

    @Override
    public String getName() {
        return "OmniSelectOperator";
    }

    @Override
    public OperatorType getType() {
        return OperatorType.SELECT;
    }

    @Override
    protected void closeOp(boolean abort) throws HiveException {
        if (projectOperatorFactory != null) {
            projectOperatorFactory.close();
        }
        if (omniOperator != null) {
            omniOperator.close();
        }
        output = null;
        super.closeOp(abort);
    }

    private TypeInfo createTypeinfo(ExprNodeGenericFuncDesc nodeDesc) {
        TypeInfo typeInfo = nodeDesc.getTypeInfo();
        if (typeInfo instanceof PrimitiveTypeInfo && typeInfo.getTypeName().equals("string")) {
            return new VarcharTypeInfo(TypeUtils.calculateVarcharLength(nodeDesc));
        }
        return typeInfo;
    }

    private ObjectInspector expandInspector(ObjectInspector inspector) {
        List<StructField> fields = new ArrayList<>();
        List<? extends StructField> keyValueFields = ((StructObjectInspector) inspector).getAllStructFieldRefs();
        List<? extends StructField> keyFields = ((StructObjectInspector) keyValueFields.get(0)
                .getFieldObjectInspector()).getAllStructFieldRefs();
        List<? extends StructField> valueFields = ((StructObjectInspector) keyValueFields.get(1)
                .getFieldObjectInspector()).getAllStructFieldRefs();
        List<String> fieldNames = keyFields.stream().map(filed -> "key." + filed.getFieldName())
                .collect(Collectors.toList());
        fieldNames.addAll(
                valueFields.stream().map(field -> "value." + field.getFieldName()).collect(Collectors.toList()));
        fields.addAll(keyFields);
        fields.addAll(valueFields);
        List<ObjectInspector> fieldInspectors = fields.stream().map(StructField::getFieldObjectInspector)
                .collect(Collectors.toList());
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldInspectors);
    }
}