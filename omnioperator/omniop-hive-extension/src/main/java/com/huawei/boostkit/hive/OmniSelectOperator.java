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
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class OmniSelectOperator extends OmniHiveOperator<OmniSelectDesc> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static Cache<Object, Object> cache = CacheBuilder.newBuilder().concurrencyLevel(8).initialCapacity(10)
            .maximumSize(100).recordStats().removalListener(notification -> {
                ((OmniProjectOperatorFactory) notification.getValue()).close();
            }).build();

    private static boolean isAddedCloseThread;

    private final transient Logger LOG = LoggerFactory.getLogger(getClass().getName());
    private transient OmniProjectOperatorFactory projectOperatorFactory;

    private transient OmniOperator omniOperator;

    private transient boolean isSelectStarNoCompute = false;

    private transient boolean isNeedSliceVector = false;

    private transient Iterator<VecBatch> output;

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
        Operator parent = parentOperators.get(0);
        if (parent instanceof OmniVectorOperator && ((OmniVectorOperator) parent).isKeyValue()) {
            inputObjInspectors[0] = OperatorUtils.expandInspector(inputObjInspectors[0]);
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
        String cacheKey = Arrays.toString(expressions) + Arrays.toString(inputTypes);
        OmniProjectOperatorFactory omniProjectOperatorFactory = (OmniProjectOperatorFactory) cache
                .getIfPresent(cacheKey);
        if (omniProjectOperatorFactory != null) {
            this.projectOperatorFactory = omniProjectOperatorFactory;
            this.omniOperator = this.projectOperatorFactory.createOperator();
            return;
        }
        this.projectOperatorFactory = new OmniProjectOperatorFactory(expressions, inputTypes, 1, new OperatorConfig(
                SpillConfig.NONE, new OverflowConfig(OverflowConfig.OverflowConfigId.OVERFLOW_CONFIG_NULL), true));
        this.omniOperator = this.projectOperatorFactory.createOperator();
        cache.put(cacheKey, this.projectOperatorFactory);
        if (!isAddedCloseThread) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                cache.invalidateAll();
            }));
            isAddedCloseThread = true;
        }
    }

    protected void prepareExpression(List<ObjectInspector> colInspector, List<Integer> dataTypes, List<Integer> colVals,
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

    protected void checkIfHasBrotherOperators() {
        List<Operator<? extends OperatorDesc>> parentOperators = this.getParentOperators();
        List<Operator<? extends OperatorDesc>> childOperators = parentOperators.get(0).getChildOperators();
        if (childOperators.get(childOperators.size() - 1) != this) {
            isNeedSliceVector = true;
        }
    }

    @Override
    public void process(Object row, int tag) throws HiveException {
        if (isSelectStarNoCompute) {
            forward(row, inputObjInspectors[tag]);
            return;
        }
        VecBatch input = (VecBatch) row;
        if (isNeedSliceVector) {
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
        return "OMNI_SEL";
    }

    @Override
    public OperatorType getType() {
        return OperatorType.SELECT;
    }

    @Override
    protected void closeOp(boolean isAbort) throws HiveException {
        if (projectOperatorFactory != null) {
            projectOperatorFactory.close();
        }
        if (omniOperator != null) {
            omniOperator.close();
        }
        output = null;
        super.closeOp(isAbort);
    }

    private TypeInfo createTypeinfo(ExprNodeGenericFuncDesc nodeDesc) {
        TypeInfo typeInfo = nodeDesc.getTypeInfo();
        if (typeInfo instanceof PrimitiveTypeInfo && typeInfo.getTypeName().equals("string")) {
            return new VarcharTypeInfo(TypeUtils.calculateVarcharLength(nodeDesc));
        }
        return typeInfo;
    }

    public static boolean isInvalidSelectColumn(List<ExprNodeDesc> colList) {
        for (ExprNodeDesc exprNodeDesc : colList) {
            if (exprNodeDesc instanceof ExprNodeConstantDesc
                    && exprNodeDesc.getTypeInfo().getTypeName().equals("smallint")) {
                return true;
            }
        }
        return false;
    }
}