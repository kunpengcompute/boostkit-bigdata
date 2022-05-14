package org.apache.hadoop.hive.ql.omnidata.reader;

import com.huawei.boostkit.omnidata.model.datasource.DataSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * OmniDataOrcRecordReader
 */
public class OmniDataOrcRecordReader implements RecordReader {
    static final Logger LOG = LoggerFactory.getLogger(OmniDataOrcRecordReader.class);

    private OmniDataAdapter dataAdapter;

    public OmniDataOrcRecordReader(Configuration conf, FileSplit fileSplit, DataSource dataSource,
                                   NdpPredicateInfo ndpPredicateInfo) {
        this.dataAdapter = new OmniDataAdapter(dataSource, conf, fileSplit, ndpPredicateInfo);
    }

    private boolean ensureBatch() {
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public long getRowNumber() {
        return 0;
    }

    @Override
    public float getProgress() throws IOException {
        return 0;
    }

    @Override
    public boolean hasNext() {
        return ensureBatch();
    }

    @Override
    public void seekToRow(long row) {
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public Object next(Object previous) throws IOException {
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public boolean nextBatch(VectorizedRowBatch theirBatch) {
        boolean ret = false;
        try {
            ret = dataAdapter.nextBatchFromOmniData(theirBatch);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    @Override
    public void close() throws IOException {
    }

}