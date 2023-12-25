package com.huawei.boostkit.hive;

import com.huawei.boostkit.hive.expression.BaseExpression;
import com.huawei.boostkit.hive.expression.ExpressionUtils;
import com.huawei.boostkit.hive.expression.TypeUtils;

import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.operator.config.OverflowConfig;
import nova.hetu.omniruntime.operator.config.SpillConfig;
import nova.hetu.omniruntime.operator.filter.OmniFilterAndProjectOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static nova.hetu.omniruntime.operator.config.OverflowConfig.OverflowConfigId.OVERFLOW_CONFIG_NULL;

public class OmniFilterOperator extends OmniHiveOperator<FilterDesc> implements Serializable {
    private transient OmniFilterAndProjectOperatorFactory filterAndProjectOperatorFactory;

    private transient OmniOperator omniOperator;
    private transient Iterator<VecBatch> output;

    public OmniFilterOperator(CompilationOpContext ctx) {
        super(ctx);
    }

    public OmniFilterOperator(CompilationOpContext ctx, FilterDesc conf) {
        super(ctx);
        this.conf = conf;
    }

    @Override
    protected void initializeOp(Configuration hconf) throws HiveException {
        super.initializeOp(hconf);
        ExprNodeGenericFuncDesc predicate = (ExprNodeGenericFuncDesc) conf.getPredicate();
        BaseExpression root = ExpressionUtils.build(predicate, inputObjInspectors[0]);
        List<? extends StructField> allStructFieldRefs = ((StructObjectInspector) inputObjInspectors[0]).getAllStructFieldRefs();
        DataType[] inputTypes = new DataType[allStructFieldRefs.size()];
        String[] projections = new String[allStructFieldRefs.size()];
        for (int i = 0; i < allStructFieldRefs.size(); i++) {
            if (allStructFieldRefs.get(i).getFieldObjectInspector() instanceof PrimitiveObjectInspector) {
                PrimitiveTypeInfo typeInfo = ((PrimitiveObjectInspector) allStructFieldRefs.get(i).getFieldObjectInspector()).getTypeInfo();
                int omniType = TypeUtils.convertHiveTypeToOmniType(typeInfo);
                inputTypes[i] = TypeUtils.buildINputDataType(typeInfo);
                projections[i] = TypeUtils.buildExpression(typeInfo, omniType, i);
            }
        }
        this.filterAndProjectOperatorFactory = new OmniFilterAndProjectOperatorFactory(root.toString(), inputTypes,
                Arrays.asList(projections), 1, new OperatorConfig(SpillConfig.NONE,
                new OverflowConfig(OVERFLOW_CONFIG_NULL), true));
        this.omniOperator = this.filterAndProjectOperatorFactory.createOperator();
    }

    @Override
    public void process(Object row, int tag) throws HiveException {
        VecBatch input = (VecBatch) row;
        this.omniOperator.addInput(input);
        output = this.omniOperator.getOutput();
        while (output.hasNext()) {
            forward(output.next(), null);
        }
    }

    @Override
    public String getName() {
        return "OmniFilterOperator";
    }

    @Override
    public OperatorType getType() {
        return OperatorType.FILTER;
    }

    @Override
    protected void closeOp(boolean abort) throws HiveException {
        filterAndProjectOperatorFactory.close();
        omniOperator.close();
        output = null;
        super.closeOp(abort);
    }
}