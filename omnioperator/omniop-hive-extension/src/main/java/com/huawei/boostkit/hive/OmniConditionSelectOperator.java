/*
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
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
import com.huawei.boostkit.hive.expression.ReferenceFactor;
import com.huawei.boostkit.hive.expression.TypeUtils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.config.OperatorConfig;
import nova.hetu.omniruntime.operator.config.OverflowConfig;
import nova.hetu.omniruntime.operator.config.SpillConfig;
import nova.hetu.omniruntime.operator.filter.OmniFilterAndProjectOperatorFactory;
import nova.hetu.omniruntime.operator.project.OmniProjectOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class OmniConditionSelectOperator extends OmniSelectOperator {
    private static final long serialVersionUID = 1L;
    private static Cache<Object, Object> cache = CacheBuilder.newBuilder().concurrencyLevel(8).initialCapacity(10)
            .maximumSize(100).recordStats().removalListener(notification -> {
                ((OmniProjectOperatorFactory) notification.getValue()).close();
            }).build();

    private static boolean isAddedCloseThread;

    private final transient Logger LOG = LoggerFactory.getLogger(getClass().getName());
    private transient OmniFilterAndProjectOperatorFactory omniFilterAndProjectOperatorFactory;

    private transient OmniOperator omniOperator;

    private transient boolean isNeedSliceVector = false;

    private transient Iterator<VecBatch> output;

    public OmniConditionSelectOperator() {
        super();
    }

    public OmniConditionSelectOperator(CompilationOpContext ctx) {
        super(ctx);
    }

    public OmniConditionSelectOperator(CompilationOpContext ctx, SelectDesc conf) {
        super(ctx);
        this.conf = new OmniConditionSelectDesc(conf);
    }

    @Override
    protected void initializeOp(Configuration hconf) throws HiveException {
        super.initializeOp(hconf);
        ExprNodeDesc predicate = ((OmniFilterDesc) otherConfs.get(0)).getPredicate();
        BaseExpression root;
        if (predicate instanceof ExprNodeGenericFuncDesc) {
            root = ExpressionUtils.build((ExprNodeGenericFuncDesc) predicate, inputObjInspectors[0]);
        } else if (predicate instanceof ExprNodeColumnDesc) {
            root = ExpressionUtils.wrapNotNullExpression(
                    (ReferenceFactor) ExpressionUtils.createReferenceNode(predicate, inputObjInspectors[0]));
        } else {
            root = ExpressionUtils.createLiteralNode(predicate);
        }
        List<? extends StructField> allStructFieldRefs =
                ((StructObjectInspector) inputObjInspectors[0]).getAllStructFieldRefs();
        DataType[] inputTypes = new DataType[allStructFieldRefs.size()];
        String[] projections = new String[allStructFieldRefs.size()];
        for (int i = 0; i < allStructFieldRefs.size(); i++) {
            if (allStructFieldRefs.get(i).getFieldObjectInspector() instanceof PrimitiveObjectInspector) {
                PrimitiveTypeInfo typeInfo = (
                        (PrimitiveObjectInspector) allStructFieldRefs.get(i).getFieldObjectInspector()).getTypeInfo();
                int omniType = TypeUtils.convertHiveTypeToOmniType(typeInfo);
                inputTypes[i] = TypeUtils.buildInputDataType(typeInfo);
                projections[i] = TypeUtils.buildExpression(typeInfo, omniType, i);
            }
        }

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

        String cacheKey = root.toString() + Arrays.toString(expressions) + Arrays.toString(inputTypes);
        OmniFilterAndProjectOperatorFactory omniFilterAndProjectOperatorFactory = (OmniFilterAndProjectOperatorFactory) cache
                .getIfPresent(cacheKey);
        if (omniFilterAndProjectOperatorFactory != null) {
            this.omniFilterAndProjectOperatorFactory = omniFilterAndProjectOperatorFactory;
            this.omniOperator = this.omniFilterAndProjectOperatorFactory.createOperator();
            return;
        }
        this.omniFilterAndProjectOperatorFactory = new OmniFilterAndProjectOperatorFactory(root.toString(), inputTypes,
                Arrays.asList(expressions), 1, new OperatorConfig(SpillConfig.NONE,
                new OverflowConfig(OverflowConfig.OverflowConfigId.OVERFLOW_CONFIG_NULL), true));
        this.omniOperator = this.omniFilterAndProjectOperatorFactory.createOperator();
        cache.put(cacheKey, this.omniFilterAndProjectOperatorFactory);
        if (!isAddedCloseThread) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                cache.invalidateAll();
            }));
            isAddedCloseThread = true;
        }
    }

    @Override
    public void process(Object row, int tag) throws HiveException {
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
        return "OMNI_CON_SEL";
    }

    @Override
    protected void closeOp(boolean isAbort) throws HiveException {
        if (omniFilterAndProjectOperatorFactory != null) {
            omniFilterAndProjectOperatorFactory.close();
        }
        if (omniOperator != null) {
            omniOperator.close();
        }
        output = null;
        super.closeOp(isAbort);
    }
}
