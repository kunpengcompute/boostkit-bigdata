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

import com.huawei.boostkit.hive.expression.TypeUtils;

import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.sort.OmniSortOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class OmniSortOperator extends OmniHiveOperator<OmniSortDesc> implements Serializable {
    private static final long serialVersionUID = 1L;

    private transient OmniSortOperatorFactory sortOperatorFactory;

    private transient OmniOperator omniOperator;

    public OmniSortOperator() {
        super();
    }

    public OmniSortOperator(CompilationOpContext ctx) {
        super(ctx);
    }

    public OmniSortOperator(CompilationOpContext ctx, GroupByDesc groupByDesc) {
        super(ctx);
        this.conf = new OmniSortDesc(groupByDesc);
    }

    @Override
    protected void initializeOp(Configuration hconf) throws HiveException {
        super.initializeOp(hconf);
        List<? extends StructField> allStructFieldRefs = ((StructObjectInspector) inputObjInspectors[0])
                .getAllStructFieldRefs();
        DataType[] inputTypes = new DataType[allStructFieldRefs.size()];
        int[] outputCols = new int[allStructFieldRefs.size()];
        for (int i = 0; i < allStructFieldRefs.size(); i++) {
            if (allStructFieldRefs.get(i).getFieldObjectInspector() instanceof PrimitiveObjectInspector) {
                PrimitiveTypeInfo typeInfo = ((PrimitiveObjectInspector) allStructFieldRefs.get(i)
                        .getFieldObjectInspector()).getTypeInfo();
                inputTypes[i] = TypeUtils.buildInputDataType(typeInfo);
                outputCols[i] = i;
            }
        }
        String[] sortColumns = new String[conf.getSortCol().length];
        for (int i = 0; i < conf.getSortCol().length; i++) {
            sortColumns[i] = "#" + i;
        }
        int[] sortAscendings = new int[conf.getSortCol().length];
        int[] sortNullFirsts = new int[conf.getSortCol().length];
        Arrays.fill(sortAscendings, 1);
        Arrays.fill(sortNullFirsts, 1);
        this.sortOperatorFactory = new OmniSortOperatorFactory(inputTypes, outputCols, sortColumns, sortAscendings,
                sortNullFirsts);
        this.omniOperator = this.sortOperatorFactory.createOperator();
    }

    @Override
    public void process(Object row, int tag) throws HiveException {
        VecBatch input = (VecBatch) row;
        this.omniOperator.addInput(input);
    }

    @Override
    public String getName() {
        return "OMNI_SORT";
    }

    @Override
    public OperatorType getType() {
        return null;
    }

    @Override
    protected void closeOp(boolean abort) throws HiveException {
        Iterator<VecBatch> output = this.omniOperator.getOutput();
        while (output.hasNext()) {
            forward(output.next(), outputObjInspector);
        }
        if (sortOperatorFactory != null) {
            sortOperatorFactory.close();
        }
        if (omniOperator != null) {
            omniOperator.close();
        }
        super.closeOp(abort);
    }
}