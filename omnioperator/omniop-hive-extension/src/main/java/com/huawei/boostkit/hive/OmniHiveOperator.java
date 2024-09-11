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

import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public abstract class OmniHiveOperator<T extends OperatorDesc> extends Operator<T> {
    protected List<Object> otherConfs = new ArrayList<>();
    private static String currentWork;

    public OmniHiveOperator() {
        super();
    }

    public OmniHiveOperator(CompilationOpContext ctx) {
        super(ctx);
    }

    public List<Object> getOtherConfs() {return otherConfs;}

    public void addOtherConfs(Object otherConf) {
        otherConfs.add(otherConf);
    }

    @Override
    protected void forward(Object row, ObjectInspector rowInspector) throws HiveException {
        VecBatch vecBatch = null;
        VecBatch[] vecBatches = new VecBatch[childOperatorsArray.length];
        if (row instanceof VecBatch) {
            vecBatch = (VecBatch) row;
            vecBatches[0] = vecBatch;
            this.runTimeNumRows += vecBatch.getRowCount();
            if (childOperatorsArray.length > 1) {
                for (int i = 1; i < vecBatches.length; i++) {
                    if (!childOperatorsArray[i].getDone()) {
                        vecBatches[i] = copyVecBatch(vecBatch);
                    }
                }
            }
        }
        if (getDone()) {
            if (vecBatch != null) {
                vecBatch.releaseAllVectors();
                ;
                vecBatch.close();
            }
            return;
        }
        int childrenDone = 0;
        for (int i = 0; i < childOperatorsArray.length; i++) {
            Operator<? extends OperatorDesc> o = childOperatorsArray[i];
            if (o.getDone()) {
                childrenDone++;
            } else {
                if (vecBatch != null) {
                    o.process(vecBatches[i], childOperatorsTag[i]);
                } else {
                    o.process(row, childOperatorsTag[i]);
                }
            }
        }
        // if all children are done, this operator is also done
        if (childrenDone != 0 && childrenDone == childOperatorsArray.length) {
            setDone(true);
            if (vecBatch != null) {
                vecBatch.releaseAllVectors();
                vecBatch.close();
            }
        }
    }

    protected void forward(VecBatch vecBatch, int tag) throws HiveException {
        this.runTimeNumRows += vecBatch.getRowCount();
        if (getDone()) {
            vecBatch.releaseAllVectors();
            vecBatch.close();
            return;
        }
        int childrenDone = 0;
        for (int i = 0; i < childOperatorsArray.length; i++) {
            Operator<? extends OperatorDesc> o = childOperatorsArray[i];
            if (o.getDone()) {
                childrenDone++;
            } else {
                o.process(vecBatch, tag);
            }
        }
        // if all children are done, this operator is also done
        if (childrenDone != 0 && childrenDone == childOperatorsArray.length) {
            setDone(true);
            setAllParentsDone();
            vecBatch.releaseAllVectors();
            vecBatch.close();
        }
    }

    private void setAllParentsDone() {
        Queue<Operator> parents = new LinkedList<>(this.getParentOperators());
        while (!parents.isEmpty()) {
            Operator current = parents.poll();
            for (Object child : current.getChildOperators()) {
                if (!((Operator) child).getDone()) {
                    return;
                }
            }
            if (current instanceof OmniHiveOperator) {
                ((OmniHiveOperator) current).publicSetDone(true);
            } else if (current instanceof OmniMergeJoinOperator) {
                ((OmniMergeJoinOperator) current).publicSetDone(true);
            } else if (current instanceof OmniMapJoinOperator) {
                ((OmniMapJoinOperator) current).publicSetDone(true);
            } else {
                return;
            }
            for (Object parent : current.getParentOperators()) {
                parents.offer((Operator) parent);
            }
        }
    }

    public static VecBatch copyVecBatch(VecBatch vecBatch) {
        Vec[] vectors = vecBatch.getVectors();
        Vec[] copyVectors = new Vec[vectors.length];
        for (int i = 0; i < vectors.length; i++) {
            copyVectors[i] = vectors[i].slice(0, vectors[i].getSize());
        }
        return new VecBatch(copyVectors, vecBatch.getRowCount());
    }

    @Override
    public void startGroup() throws HiveException {
    }

    @Override
    public void endGroup() throws HiveException {
    }

    public void publicSetDone(boolean done) {
        this.done = done;
    }
}