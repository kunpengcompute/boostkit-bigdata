
package com.huawei.boostkit.hive.reader;

import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VecBatchWrapper extends VectorizedRowBatch {
    private VecBatch vecBatch;

    public VecBatchWrapper() {
        super(0);
    }

    public VecBatch getVecBatch() {
        return vecBatch;
    }

    public void setVecBatch(VecBatch vecBatch) {
        this.vecBatch = vecBatch;
    }

    public void write(DataOutput var1) throws IOException {
    }

    public void readFields(DataInput var1) throws IOException {
    }
}
