package org.apache.hadoop.hive.ql.omnidata.reader;

import com.huawei.boostkit.omnidata.model.datasource.DataSource;
import com.huawei.boostkit.omnidata.model.datasource.hdfs.HdfsParquetDataSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.FileMetadataCache;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * OmniDataParquetRecordReader
 */
public class OmniDataParquetRecordReader extends VectorizedParquetRecordReader {
    static final Logger LOG = LoggerFactory.getLogger(OmniDataParquetRecordReader.class);

    private OmniDataAdapter dataAdapter;

    public OmniDataParquetRecordReader(InputSplit oldInputSplit, JobConf conf, FileMetadataCache metadataCache,
                                       DataCache dataCache, Configuration cacheConf, NdpPredicateInfo ndpPredicateInfo) {
        super(oldInputSplit, conf, metadataCache, dataCache, cacheConf);

        String path = ((FileSplit) oldInputSplit).getPath().toString();
        long start = ((FileSplit) oldInputSplit).getStart();
        long length = ((FileSplit) oldInputSplit).getLength();
        DataSource dataSource = new HdfsParquetDataSource(path, start, length, false);

        this.dataAdapter = new OmniDataAdapter(dataSource, conf, (FileSplit) oldInputSplit, ndpPredicateInfo);
    }

    @Override
    public boolean next(NullWritable nullWritable, VectorizedRowBatch vectorizedRowBatch) throws IOException {
        if (fileSchema == null) {
            return false;
        } else {
            return dataAdapter.nextBatchFromOmniData(vectorizedRowBatch);
        }
    }
}
