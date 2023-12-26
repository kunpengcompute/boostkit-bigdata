
package com.huawei.boostkit.hive;

import com.huawei.boostkit.hive.expression.TypeUtils;

import com.google.common.primitives.Ints;
import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.constants.OmniWindowFrameBoundType;
import nova.hetu.omniruntime.constants.OmniWindowFrameType;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.operator.config.OverflowConfig;
import nova.hetu.omniruntime.operator.window.OmniWindowOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.PTFDeserializer;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.plan.ptf.OrderExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFunctionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowTableFunctionDef;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OmniPTFOperator extends OmniHiveOperator<OmniPTFDesc> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OmniPTFOperator.class.getName());
    private transient OmniWindowOperatorFactory omniWindowOperatorFactory;
    private transient OmniOperator omniOperator;
    private transient List<? extends StructField> allStructFieldRefs;
    boolean isMapOperator;

    private transient WindowTableFunctionDef windowTableFunctionDef;
    transient Configuration hiveConf;

    public OmniPTFOperator() {
        super();
    }

    public OmniPTFOperator(CompilationOpContext ctx) {
        super(ctx);
    }

    public OmniPTFOperator(CompilationOpContext ctx, PTFDesc conf) {
        super(ctx);
        this.conf = new OmniPTFDesc(conf);
    }

    @Override
    protected void initializeOp(Configuration jobConf) throws HiveException {
        super.initializeOp(jobConf);
        hiveConf = jobConf;
        isMapOperator = conf.isMapSide();
        reconstructQueryDef(hiveConf);
        windowTableFunctionDef = (WindowTableFunctionDef) conf.getFuncDef();
        List<WindowFunctionDef> windowFunctions = windowTableFunctionDef.getWindowFunctions();
        List<OrderExpressionDef> windowOrderExpressions = windowTableFunctionDef.getOrder().getExpressions();

        if (isMapOperator) {
            PartitionedTableFunctionDef tDef = conf.getStartOfChain();
            outputObjInspector = tDef.getRawInputShape().getOI();
        } else {
            outputObjInspector = conf.getFuncDef().getOutputShape().getOI();
        }

        allStructFieldRefs = ((StandardStructObjectInspector) inputObjInspectors[0]).getAllStructFieldRefs();

        DataType[] sourceTypes = getExprFromStructField(allStructFieldRefs);
        int[] outputChannels = getOutputChannels((StandardStructObjectInspector) inputObjInspectors[0],
                (StandardStructObjectInspector) outputObjInspector);
        FunctionType[] windowFunction = getWindowFunctionType(windowFunctions);
        int[] partitionChannels = getChannels(windowTableFunctionDef.getPartition().getExpressions());
        int[] preGroupedChannels = {};
        int[] sortChannels = getChannels(windowOrderExpressions);
        int[] sortOrder = getSortOrder(windowOrderExpressions);
        int[] sortNullFirsts = getSortNullFirsts(windowOrderExpressions);
        int preSortedChannelPrefix = 0;
        int expectedPositions = 10000;
        int[] argumentKeys = getWindowArgumentKeys(windowFunctions);
        DataType[] windowFunctionReturnType = getWindowFunctionReturnType(windowFunctions);
        OmniWindowFrameType[] windowFrameTypes = getWindowFrameType(windowFunctions);
        OmniWindowFrameBoundType[] windowFrameStartTypes = getWindowFrameStartTypes(windowFunctions);
        int[] windowFrameStartChannels = {-1};
        OmniWindowFrameBoundType[] windowFrameEndTypes = getWindowFrameEndTypes(windowFunctions);
        int[] windowFrameEndChannels = {-1};
        OverflowConfig overflowConfig = new OverflowConfig(OverflowConfig.OverflowConfigId.OVERFLOW_CONFIG_NULL);
        OperatorConfig operatorConfig = new OperatorConfig(overflowConfig);
        this.omniWindowOperatorFactory = new OmniWindowOperatorFactory(sourceTypes, outputChannels, windowFunction,
                partitionChannels, preGroupedChannels, sortChannels, sortOrder, sortNullFirsts, preSortedChannelPrefix,
                expectedPositions, argumentKeys, windowFunctionReturnType, windowFrameTypes, windowFrameStartTypes,
                windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
        this.omniOperator = omniWindowOperatorFactory.createOperator();
    }

    private DataType[] getExprFromStructField(List<? extends StructField> structFields) {
        return structFields.stream()
                .map(structField -> TypeUtils.buildInputDataType(
                        ((PrimitiveObjectInspector) structField.getFieldObjectInspector()).getTypeInfo()))
                .toArray(DataType[]::new);
    }

    private FunctionType[] getWindowFunctionType(List<WindowFunctionDef> windowFunctionDefs) {
        return windowFunctionDefs.stream()
                .map(windowFunctionDef -> TypeUtils.getWindowFunctionType(windowFunctionDef))
                .toArray(FunctionType[]::new);
    }

    private int getFieldIdFromFieldName(String name) {
        StructField structFieldRef = ((StructObjectInspector) inputObjInspectors[0]).getStructFieldRef(name);
        return structFieldRef.getFieldID();
    }

    private int[] getChannels(List<? extends PTFExpressionDef> ptfExpressionDefs) {
        List<Integer> channels = new ArrayList();
        for (PTFExpressionDef ptfExpressionDef : ptfExpressionDefs) {
            ExprNodeDesc exprNode = ptfExpressionDef.getExprNode();
            if (exprNode instanceof ExprNodeColumnDesc) {
                channels.add(getFieldIdFromFieldName(((ExprNodeColumnDesc) exprNode).getColumn()));
            } else if (exprNode instanceof ExprNodeConstantDesc) {
                // ExprNodeConstantDesc indicates that there is no partition column.
                // Therefore, we do not need to obtain column information.
            } else {
                throw new IllegalArgumentException("not support ExprNode: " + exprNode.getClass().getSimpleName());
            }
        }
        return Ints.toArray(channels);
    }

    private OmniWindowFrameType[] getWindowFrameType(List<WindowFunctionDef> windowFunctionDefs) {
        return windowFunctionDefs.stream().map(
                windowFunctionDef -> TypeUtils.getWindowFrameType(windowFunctionDef.getWindowFrame().getWindowType()))
                .toArray(OmniWindowFrameType[]::new);
    }

    private OmniWindowFrameBoundType[] getWindowFrameStartTypes(List<WindowFunctionDef> windowFunctionDefs) {
        return windowFunctionDefs.stream()
                .map(windowFunctionDef -> TypeUtils
                        .getWindowFrameBoundType(windowFunctionDef.getWindowFrame().getStart().getDirection()))
                .toArray(OmniWindowFrameBoundType[]::new);
    }

    private OmniWindowFrameBoundType[] getWindowFrameEndTypes(List<WindowFunctionDef> windowFunctionDefs) {
        return windowFunctionDefs.stream()
                .map(windowFunctionDef -> TypeUtils
                        .getWindowFrameBoundType(windowFunctionDef.getWindowFrame().getEnd().getDirection()))
                .toArray(OmniWindowFrameBoundType[]::new);
    }

    private int[] getOutputChannels(StandardStructObjectInspector inputOI, StandardStructObjectInspector outputOI) {
        List<Integer> outputChannels = new ArrayList<>();
        List<? extends StructField> inputField = inputOI.getAllStructFieldRefs();
        List<? extends StructField> outputField = outputOI.getAllStructFieldRefs();

        for (int i = 0; i < inputField.size(); i++) {
            for (int j = 0; j < outputField.size(); j++) {
                if (inputField.get(i).getFieldName().equals(outputField.get(j).getFieldName())) {
                    outputChannels.add(inputField.get(i).getFieldID());
                }
            }
        }

        return outputChannels.stream().mapToInt(Integer::valueOf).toArray();
    }

    private int[] getSortOrder(List<OrderExpressionDef> orderExpressionDefs) {
        return (orderExpressionDefs.stream()
                .mapToInt(orderExpressionDef -> TypeUtils.getWindowSortType(orderExpressionDef.getOrder()))).toArray();
    }

    private int[] getSortNullFirsts(List<OrderExpressionDef> orderExpressionDefs) {
        return (orderExpressionDefs.stream()
                .mapToInt(orderExpressionDef -> TypeUtils.getSortNullFirst(orderExpressionDef.getNullOrder())))
                .toArray();
    }

    private DataType[] getWindowFunctionReturnType(List<WindowFunctionDef> windowFunctionDefs) {
        List<DataType> dataTypes = new ArrayList<>();
        for (WindowFunctionDef windowFunctionDef : windowFunctionDefs) {
            ObjectInspector oi = windowFunctionDef.getOI();
            if (oi instanceof PrimitiveObjectInspector) {
                dataTypes.add(TypeUtils.buildInputDataType(((PrimitiveObjectInspector) oi).getTypeInfo()));
            }
            if (oi instanceof StandardListObjectInspector) {
                dataTypes.add(
                        TypeUtils.buildInputDataType(((WritableIntObjectInspector) (((StandardListObjectInspector) oi)
                        .getListElementObjectInspector())).getTypeInfo()));
            }
        }
        return dataTypes.toArray(new DataType[0]);
    }

    private int[] getWindowArgumentKeys(List<WindowFunctionDef> windowFunctionDefs) {
        int[] argumentChannels = new int[windowFunctionDefs.size()];
        for (int i = 0; i < windowFunctionDefs.size(); i++) {
            List<PTFExpressionDef> args = windowFunctionDefs.get(i).getArgs();
            if (args == null) {
                argumentChannels[i] = -1;
            } else if (args.size() == 1) {
                argumentChannels[i] = Integer
                        .valueOf(getFieldIdFromFieldName(((ExprNodeColumnDesc) args.get(0).getExprNode()).getColumn()));
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported! WindowFunctionDefinition.getArgumentChannels() bigger than 1: " + args.size());
            }
        }
        return argumentChannels;
    }

    // because omniruntime always puts build table output col in the tail
    // but hive is according to inputInspectors' order
    // so need to reorder output col
    private VecBatch reorderVecs(VecBatch vecBatch) {
        Vec[] newVecs = new Vec[vecBatch.getVectors().length];

        int outputOISize = ((StandardStructObjectInspector) outputObjInspector).getAllStructFieldRefs().size();
        int inputOISize = ((StandardStructObjectInspector) inputObjInspectors[0]).getAllStructFieldRefs().size();

        int srcPos = 0;
        int destPos = outputOISize - inputOISize;
        int len = inputOISize;
        System.arraycopy(vecBatch.getVectors(), srcPos, newVecs, destPos, len);

        srcPos = inputOISize;
        destPos = 0;
        len = outputOISize - inputOISize;
        System.arraycopy(vecBatch.getVectors(), srcPos, newVecs, destPos, len);

        return new VecBatch(newVecs, vecBatch.getRowCount());
    }

    /**
     * Initialize the visitor to use the QueryDefDeserializer Use the order
     * defined in QueryDefWalker to visit the QueryDef
     *
     * @param hiveConf
     * @throws HiveException
     * */
    protected void reconstructQueryDef(Configuration hiveConf) throws HiveException {
        PTFDeserializer ds = new PTFDeserializer(conf, (StructObjectInspector) inputObjInspectors[0], hiveConf);
        ds.initializePTFChain(conf.getFuncDef());
    }

    @Override
    public void process(Object row, int tag) throws HiveException {
        VecBatch input = (VecBatch) row;
        this.omniOperator.addInput(input);
    }

    @Override
    public String getName() {
        return "OMNI-PTF";
    }

    @Override
    public OperatorType getType() {
        return OperatorType.PTF;
    }

    @Override
    protected void closeOp(boolean abort) throws HiveException {
        super.closeOp(abort);
        Iterator<VecBatch> output = this.omniOperator.getOutput();
        while (output.hasNext()) {
            VecBatch next = output.next();
            next = reorderVecs(next);
            forward(next, outputObjInspector);
        }
        omniWindowOperatorFactory.close();
        omniOperator.close();
    }
}
