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

import static nova.hetu.omniruntime.operator.config.OverflowConfig.OverflowConfigId.OVERFLOW_CONFIG_NULL;

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
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
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

public class OmniFilterOperator extends OmniHiveOperator<OmniFilterDesc> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static Cache<Object, Object> cache = CacheBuilder.newBuilder().concurrencyLevel(8).initialCapacity(10)
            .maximumSize(100).recordStats().removalListener(notification -> {
                ((OmniFilterAndProjectOperatorFactory) notification.getValue()).close();
            }).build();
    private static boolean hasAddedCloseThread = false;

    private transient OmniFilterAndProjectOperatorFactory filterAndProjectOperatorFactory;
    private transient OmniOperator omniOperator;
    private transient Iterator<VecBatch> output;
    public OmniFilterOperator() {
        super();
    }

    public OmniFilterOperator(CompilationOpContext ctx) {
        super(ctx);
    }

    public OmniFilterOperator(CompilationOpContext ctx, FilterDesc conf) {
        super(ctx);
        this.conf = new OmniFilterDesc(conf);
    }

    @Override
    protected void initializeOp(Configuration hconf) throws HiveException {
        super.initializeOp(hconf);
        ExprNodeDesc predicate = conf.getPredicate();
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

        String cacheKey = root.toString() + Arrays.toString(inputTypes) + Arrays.toString(projections);
        OmniFilterAndProjectOperatorFactory omniFilterAndProjectOperatorFactory =
                (OmniFilterAndProjectOperatorFactory) cache.getIfPresent(cacheKey);
        if (omniFilterAndProjectOperatorFactory != null) {
            this.filterAndProjectOperatorFactory = omniFilterAndProjectOperatorFactory;
            this.omniOperator = this.filterAndProjectOperatorFactory.createOperator();
            return;
        }
        this.filterAndProjectOperatorFactory = new OmniFilterAndProjectOperatorFactory(root.toString(), inputTypes,
                Arrays.asList(projections), 1, new OperatorConfig(SpillConfig.NONE,
                new OverflowConfig(OVERFLOW_CONFIG_NULL), true));
        this.omniOperator = this.filterAndProjectOperatorFactory.createOperator();

        cache.put(cacheKey, this.filterAndProjectOperatorFactory);
        if (!hasAddedCloseThread) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> cache.invalidateAll()));
            hasAddedCloseThread = true;
        }
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
        return "OMNI_FIL";
    }

    @Override
    public OperatorType getType() {
        return OperatorType.FILTER;
    }

    @Override
    protected void closeOp(boolean abort) throws HiveException {
        if (filterAndProjectOperatorFactory != null) {
            filterAndProjectOperatorFactory.close();
        }
        if (omniOperator != null) {
            omniOperator.close();
        }
        output = null;
        super.closeOp(abort);
    }
}