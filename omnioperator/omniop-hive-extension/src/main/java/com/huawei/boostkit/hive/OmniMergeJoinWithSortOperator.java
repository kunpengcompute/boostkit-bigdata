
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

    /** Kryo ctor. */
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
    // all colunmns of table 0 and table1.
    // Then use the output to join table2, and output required columns.
    protected void initializeOp(Configuration hconf) throws HiveException {
        super.initializeOp(hconf);
        omniVectorWithSortOperator = (OmniVectorWithSortOperator) parentOperators.get(0);
    }

    @Override
    protected void processOmniSmj(int opIndex, int dataIndex, Queue<VecBatch>[] data, OmniOperator[] operators,
                                  int controlCode, DataType[][] types) throws HiveException{
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
