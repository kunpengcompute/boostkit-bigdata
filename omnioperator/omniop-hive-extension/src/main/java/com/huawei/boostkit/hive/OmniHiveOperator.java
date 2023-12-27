package com.huawei.boostkit.hive;

import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public abstract class OmniHiveOperator<T extends OperatorDesc> extends Operator<T> {
    private static String currentWork;

    public OmniHiveOperator() {
        super();
    }

    public OmniHiveOperator(CompilationOpContext ctx) {
        super(ctx);
    }

    @Override
    protected void forward(Object row, ObjectInspector rowInspector) throws HiveException {
        VecBatch vecBatch = null;
        VecBatch[] vecBatches = new VecBatch[childOperatorsArray.length];
        if (row instanceof VecBatch) {
            vecBatch = (VecBatch) row;
            vecBatches[0] = vecBatch;
            this.runTimeNumRows += vecBatch.getRowCount();
            for (int i = 1; i < childOperatorsArray.length; i++) {
                if (!childOperatorsArray[i].getDone()) {
                    vecBatches[i] = copyVecBatch(vecBatch);
                }
            }
        }
        if (getDone()) {
            if (vecBatch != null) {
                vecBatch.releaseAllVectors();
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
            vecBatch.releaseAllVectors();
            vecBatch.close();
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
        return;
    }

    @Override
    public void endGroup() throws HiveException {
        return;
    }
}