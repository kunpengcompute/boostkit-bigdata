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

import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.CommonMergeJoinDesc;

import java.util.Queue;

public class OmniMergeJoinWithSortOperator extends OmniMergeJoinOperator {
    private transient OmniVectorWithSortOperator omniVectorWithSortOperator;

    /**
     * Kryo ctor.
     */
    protected OmniMergeJoinWithSortOperator() {
        super();
    }

    public OmniMergeJoinWithSortOperator(CompilationOpContext ctx) {
        super(ctx);
    }

    public OmniMergeJoinWithSortOperator(CompilationOpContext ctx, CommonMergeJoinDesc commonMergeJoinDesc) {
        super(ctx, commonMergeJoinDesc);
    }

    @Override
    // If mergeJoinOperator has 3 tables, first join table0 and table1, and output
    // all columns of table0 and table1.
    // Then use the output to join table2, and output required columns.
    protected void initializeOp(Configuration hconf) throws HiveException {
        super.initializeOp(hconf);
        omniVectorWithSortOperator = (OmniVectorWithSortOperator) parentOperators.get(0);
    }

    @Override
    protected void processOmniSmj(int opIndex, int dataIndex, Queue<VecBatch>[] data, OmniOperator[] operators,
                                  int controlCode, DataType[][] types) throws HiveException {
        if (!data[opIndex].isEmpty()) {
            while (flowControlCode[opIndex] == controlCode && resCode[opIndex] == RES_INIT
                    && !data[opIndex].isEmpty()) {
                setStatus(operators[opIndex].addInput(data[opIndex].poll()), opIndex);
            }
            return;
        }
        if (opIndex == dataIndex && opIndex > 0 && flowControlCode[opIndex - 1] == SCAN_FINISH) {
            setStatus(operators[opIndex].addInput(createEofVecBatch(types[opIndex])), opIndex);
            return;
        }

        if (omniVectorWithSortOperator.outputs[dataIndex].hasNext()) {
            while (flowControlCode[opIndex] == controlCode && resCode[opIndex] == RES_INIT
                    && omniVectorWithSortOperator.outputs[dataIndex].hasNext()) {
                omniVectorWithSortOperator.pushRecord(dataIndex);
                if (!data[opIndex].isEmpty()) {
                    setStatus(operators[opIndex].addInput(data[opIndex].poll()), opIndex);
                }
            }
        } else if (!omniVectorWithSortOperator.outputs[dataIndex].hasNext()) {
            setStatus(operators[opIndex].addInput(createEofVecBatch(types[opIndex])), opIndex);
        }
    }
}
