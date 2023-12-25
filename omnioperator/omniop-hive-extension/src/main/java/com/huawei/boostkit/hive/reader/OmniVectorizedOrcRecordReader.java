
package com.huawei.boostkit.hive.reader;

import static com.huawei.boostkit.hive.cache.VectorCache.BATCH;
import static com.huawei.boostkit.hive.converer.VecConverter.CONVERTER_MAP;

import com.huawei.boostkit.hive.converer.VecConverter;

import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;

import java.io.IOException;
import java.util.Arrays;

public class OmniVectorizedOrcRecordReader extends OmniOrcRecordReader {
    private VecConverter[] partColumnConverters;
    private final Vec[] withPartCol;
    private final Object[] partitionValues;

    public OmniVectorizedOrcRecordReader(Configuration conf, FileSplit split) throws IOException {
        super(conf, split);
        VectorizedRowBatchCtx rbCtx = Utilities.getVectorizedRowBatchCtx(conf);
        int partitionColumnCount = rbCtx.getPartitionColumnCount();
        if (partitionColumnCount > 0) {
            partColumnConverters = new VecConverter[partitionColumnCount];
            for (int i = 0; i < rbCtx.getPartitionColumnCount(); i++) {
                TypeInfo partColTypeInfo = rbCtx.getRowColumnTypeInfos()[rbCtx.getDataColumnCount() + i];
                if (partColTypeInfo instanceof PrimitiveTypeInfo) {
                    partColumnConverters[i] = CONVERTER_MAP
                            .get(((PrimitiveTypeInfo) partColTypeInfo).getPrimitiveCatagory());
                }
            }
            partitionValues = new Object[partitionColumnCount];
            withPartCol = new Vec[vecs.length + partitionColumnCount];
            VectorizedRowBatchCtx.getPartitionValues(rbCtx, conf, split, partitionValues);
        } else {
            partitionValues = null;
            withPartCol = null;
        }
    }

    @Override
    public boolean next(NullWritable key, VecBatchWrapper value) throws IOException {
        int batchSize = BATCH;
        if (tableScanOp != null && tableScanOp.getDone()) {
            return false;
        }
        if (included.size() == 0) {
            batchSize = (int) recordReader.getNumberOfRowsJava();
        } else {
            batchSize = recordReader.next(vecs);
        }
        if (batchSize == 0) {
            return false;
        }
        if (partitionValues != null) {
            for (int i = 0; i < partitionValues.length; i++) {
                Object[] partValue = new Object[batchSize];
                Arrays.fill(partValue, partColumnConverters[i].calculateValue(partitionValues[i]));
                Vec partVec = partColumnConverters[i].toOmniVec(partValue, batchSize());
                withPartCol[vecs.length + i] = partVec;
            }
            System.arraycopy(vecs, 0, withPartCol, 0, vecs.length);
            value.setVecBatch(new VecBatch(withPartCol, batchSize));
            return true;
        }
        value.setVecBatch(new VecBatch(vecs, batchSize));
        return true;
    }
}