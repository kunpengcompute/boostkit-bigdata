
package com.huawei.boostkit.hive;

import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.CommonMergeJoinDesc;

import java.util.Queue;

public class OmniMergeJoinOperator extends OmniJoinOperator {
    protected int posBigTable;
    private OmniVectorOperator omniVectorOperator;

    /** Kryo ctor. */
    protected OmniMergeJoinOperator() {
        super();
    }

    public OmniMergeJoinOperator(CompilationOpContext ctx) {
        super(ctx);
    }

    public OmniMergeJoinOperator(CompilationOpContext ctx, CommonMergeJoinDesc commonMergeJoinDesc) {
        super(ctx);
        this.conf = new OmniMergeJoinDesc(commonMergeJoinDesc);
        this.posBigTable = commonMergeJoinDesc.getPosBigTable();
    }

    @Override
    // If mergeJoinOperator has 3 tables, first join table0 and table1, and output
    // all columns of table0 and table1.
    // Then use the output to join table2, and output required columns.
    protected void initializeOp(Configuration hconf) throws HiveException {
        super.initializeOp(hconf);
        sources = ((TezContext) MapredContext.get()).getRecordSources();
        if (parentOperators.get(0) instanceof OmniVectorOperator) {
            omniVectorOperator = (OmniVectorOperator) parentOperators.get(0);
        }
    }

    @Override
    public void process(Object row, int tag) throws HiveException {
        VecBatch input = (VecBatch) row;
        if (flowControlCode[flowControlCode.length - 1] == SCAN_FINISH) {
            input.releaseAllVectors();
            input.close();
            setDone(true);
            return;
        }
        if (tag == 0 && flowControlCode[0] != SCAN_FINISH) {
            streamData[0].offer(input);
        } else if (tag >= 1 && flowControlCode[tag - 1] != SCAN_FINISH) {
            bufferData[tag - 1].offer(input);
        } else {
            input.releaseAllVectors();
            input.close();
            return;
        }
        if (tag == posBigTable) {
            processOmni(0, 1);
            for (int opIndex = 1; opIndex < streamFactories.length; opIndex++) {
                if (!streamData[opIndex].isEmpty()) {
                    processOmni(opIndex, opIndex + 1);
                }
            }
        }
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

        if (!fetchDone[dataIndex] && posBigTable != dataIndex) {
            while (flowControlCode[opIndex] == controlCode && resCode[opIndex] == RES_INIT && !fetchDone[dataIndex]) {
                fetchDone[dataIndex] = !sources[dataIndex].pushRecord();
                if (!data[opIndex].isEmpty()) {
                    setStatus(operators[opIndex].addInput(data[opIndex].poll()), opIndex);
                }
            }
        } else if (omniVectorOperator.getRowCount()[dataIndex] > 0 && posBigTable != dataIndex) {
            omniVectorOperator.pushRestData(dataIndex);
            if (!data[opIndex].isEmpty()) {
                setStatus(operators[opIndex].addInput(data[opIndex].poll()), opIndex);
            }
        } else if (omniVectorOperator.getRowCount()[dataIndex] == 0) {
            setStatus(operators[opIndex].addInput(createEofVecBatch(types[opIndex])), opIndex);
        }
    }

    public int getPosBigTable() {
        return posBigTable;
    }

    @Override
    public void startGroup() throws HiveException {
    }

    @Override
    public void endGroup() throws HiveException {
    }
}