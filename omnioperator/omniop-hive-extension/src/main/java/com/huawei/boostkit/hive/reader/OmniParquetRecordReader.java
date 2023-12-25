
package com.huawei.boostkit.hive.reader;

import static com.huawei.boostkit.hive.cache.VectorCache.BATCH;

import com.huawei.boostkit.spark.jni.ParquetColumnarBatchJniReader;

import com.google.common.base.Strings;

import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordReader;
import org.apache.hadoop.hive.ql.io.parquet.ProjectionPusher;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetFilterPredicateConverter;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterCompat;
import org.apache.parquet.filter2.statisticslevel.StatisticsFilter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetaData;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OmniParquetRecordReader
        implements RecordReader<NullWritable, VecBatchWrapper>, StatsProvidingRecordReader {
    public static final Logger LOG = LoggerFactory.getLogger(OmniParquetRecordReader.class);

    private ParquetColumnarBatchJniReader recordReader;
    private Vec[] vecs;
    private final long splitLen; //for getPos()
    private boolean skipTimestampConversion = false;
    private SerDeStats serDeStats;
    private JobConf jobConf;
    private ProjectionPusher projectionPusher;

    private FilterCompat.Filter filter;
    private boolean isFilterPredicate = false;
    private ParquetMetadata fileFooter;

    OmniParquetRecordReader(InputSplit oldSplit, JobConf oldJobConf) throws IOException {
        this.splitLen = oldSplit.getLength();
        this.serDeStats = new SerDeStats();
        this.projectionPusher = new ProjectionPusher();
        jobConf = oldJobConf;
        this.fileFooter = ParquetFileReader.readFooter(oldJobConf, ((FileSplit) oldSplit).getPath());
        ParquetInputSplit split = getSplit(oldSplit, jobConf);
        // create a TaskInputOutputContext
        Configuration conf = jobConf;
        if (skipTimestampConversion
                ^ HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION)) {
            conf = new JobConf(oldJobConf);
            HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION,
                    skipTimestampConversion);
        }
        this.isFilterPredicate = filter instanceof FilterCompat.FilterPredicateCompat;
        this.recordReader = new ParquetColumnarBatchJniReader();
        // PushDown rowGroups and columns indices for native reader.
        List<Integer> rowGroupIndices = getFilteredBlocks(split.getStart(), split.getEnd());
        List<Integer> columnIndices = ColumnProjectionUtils.getReadColumnIDs(conf);
        String ugi = UserGroupInformation.getCurrentUser.toString();
        this.recordReader.initializeReaderJava(split.getPath().toString(), BATCH, rowGroupIndices, columnIndices, ugi);
        this.vecs = new Vec[columnIndices.size()];
    }

    protected ParquetInputSplit getSplit(final org.apache.hadoop.mapred.InputSplit oldSplit, final JobConf conf)
            throws IOException {
        ParquetInputSplit split;
        if (oldSplit instanceof FileSplit) {
            final Path finalPath = ((FileSplit) oldSplit).getPath();
            final List<BlockMetaData> blocks = fileFooter.getBlocks();
            final FileMetaData fileMetaData = fileFooter.getFileMetaData();

            this.filter = setFilter(jobConf, fileMetaData.getSchema());
            final ReadSupport.ReadContext readContext = new DataWritableReadSupport()
                    .init(new InitContext(jobConf, null, fileMetaData.getSchema()));

            // Compute stats
            for (BlockMetaData bmd : blocks) {
                serDeStats.setRowCount(serDeStats.getRowCount() + bmd.getRowCount());
                serDeStats.setRawDataSize(serDeStats.getRawDataSize() + bmd.getTotalByteSize());
            }
            final List<BlockMetaData> splitGroup = new Arraylist<BlockMetaData>();
            final long splitStart = ((FileSplit) oldSplit).getStart();
            final long splitLength = ((FileSplit) oldSplit).getLength();
            for (final BlockMetaData block : blocks) {
                final long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (firstDataPage >= splitStart && firstDataPage < splitStart + splitLength) {
                    splitGroup.add(block);
                }
            }
            if (splitGroup.isEmpty()) {
                LOG.warn("Skipping split, could not find row group in: " + oldSplit);
                return null;
            }
            if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION)) {
                skipTimestampConversion = !Strings.nullToEmpty(fileMetaData.getCreateBy()).startsWith("parquet-mr");
            }
            split = new ParquetInputSplit(finalPath, splitStart, splitLength, oldSplit.getLocations(), splitGroup,
                    fileMetaData.getSchema().toString(), fileMetaData.getSchema().toString(),
                    fileMetaData.getKeyValueMetaData(), readContext.getReadSupportMetadata());
            return split;
        } else {
            throw new IllegalArgumentException("Unknown split type: " + oldSplit);
        }
    }

    private FilterCompat.Filter setFilter(final JobConf conf, MessageType schema) {
        SearchArgument sarg = ConvertAstToSearchArg.createFromConf(conf);
        if (sarg == null) {
            return null;
        }
        // Create the Parquet FilterPredicate without including columns that do not
        // exist
        // on the schema (such as partition columns).
        FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema);
        if (p != null) {
            // Filter may have sensitive information. Do not send to debug.
            LOG.debug("PARQUET predicate push down generated.");
            ParquetInputFormat.setFilterPredicate(conf, p);
            return FilterCompat.get(p);
        } else {
            // Filter may have sensitive information. Do not send to debug.
            LOG.debug("No PARQUET predicate push down is generated.");
            return null;
        }
    }

    private List<Integer> getFilteredBlocks(long start, long end) throws IOException {
        List<Integer> res = new ArrayList<>();
        List<BlockMetaData> blocks = fileFooter.getBlocks();
        for (int i = 0; i < blocks.size(); i++) {
            BlockMetaData block = blocks.get(i);
            long totalSize = 0;
            long startIndex = block.getStartingPos();
            for (ColumnChunkMetaData col : blocks.getColumns()) {
                totalSize += col.getTotalSize();
            }
            long midPoint = startIndex + totalSize / 2;
            if (midPoint >= start && midPoint < end) {
                if (isFilterPredicate) {
                    boolean drop = StatisticsFilter.canDrop(
                            ((FilterCompat.FilterPredicateCompat) filter).getFilterPredicate(), block.getColumns());
                    if (!drop) {
                        res.add(i);
                    }
                } else {
                    res.add(i);
                }
            }
        }
        return res;
    }

    @Override
    public boolean next(NullWiritable key, VecBatchWrapper value) throws IOException {
        int batchSize == recordReader.next(vecs);
        if (batchSize == 0) {
            return false;
        }
        value.setVecBatch(new VecBatch(vecs, batchSize));
        return true;
    }

    @Override
    public NullWritable createKey() {
        return NullWritable.get();
    }

    @Override
    public VecBatchWrapper createValue() {
        return new VecBatchWrapper();
    }

    @Override
    public long getPos() throws IOException {
        return (long) (splitLen * getProgress());
    }

    @Override
    public void close() throws IOEXception {
        if (recordReader != null) {
            recordReader.close();
            recordReader = null;
        }
    }

    @Override
    public float getProgress() throws IOException {
        return 0.0f;
    }

    @Override
    public SerDeStats getStats() {
        return serDeStats;
    }
}