package org.apache.hadoop.hive.ql.omnidata.reader;

import com.huawei.boostkit.omnidata.model.datasource.DataSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;
import org.apache.hadoop.mapred.FileSplit;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * OmniDataReader for agg optimization
 *
 * @since 2022-03-07
 */
public class OmniDataReader implements Callable<List<VectorizedRowBatch>> {

    private DataSource dataSource;

    private Configuration conf;

    private FileSplit fileSplit;

    private NdpPredicateInfo ndpPredicateInfo;

    public OmniDataReader(DataSource dataSource, Configuration conf, FileSplit fileSplit,
                          NdpPredicateInfo ndpPredicateInfo) {
        this.dataSource = dataSource;
        this.conf = conf;
        this.fileSplit = fileSplit;
        this.ndpPredicateInfo = ndpPredicateInfo;
    }

    @Override
    public List<VectorizedRowBatch> call() throws UnknownHostException {
        OmniDataAdapter omniDataAdapter = new OmniDataAdapter(dataSource, conf, fileSplit, ndpPredicateInfo);
        Queue<ColumnVector[]> pages = omniDataAdapter.getBatchFromOmniData();
        return getVectorizedRowBatch(pages);
    }

    private List<VectorizedRowBatch> getVectorizedRowBatch(Queue<ColumnVector[]> pages) {
        List<VectorizedRowBatch> rowBatches = new ArrayList<>();
        if (!pages.isEmpty()) {
            ColumnVector[] columnVectors = pages.poll();
            int channelCount = columnVectors.length;
            int positionCount = columnVectors[0].isNull.length;
            VectorizedRowBatch rowBatch = new VectorizedRowBatch(channelCount, positionCount);
            System.arraycopy(columnVectors, 0, rowBatch.cols, 0, channelCount);
            rowBatches.add(rowBatch);
        }
        return rowBatches;
    }
}