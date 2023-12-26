
package com.huawei.boostkit.hive.reader;

import static com.huawei.boostkit.hive.cache.VectorCache.BATCH;
import static org.apache.hadoop.hive.ql.io.orc.OrcInputFormat.getDesiredRowTypeDescr;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR;

import com.huawei.boostkit.spark.jni.OrcColumnarBatchJniReader;

import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordReader;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hive.com.esotericsoftware.kryo.Kryo;
import org.apache.hive.com.esotericsoftware.kryo.io.Input;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class OmniOrcRecordReader implements RecordReader<NullWritable, VecBatchWrapper>, StatsProvidingRecordReader {
    protected OrcColumnarBatchJniReader recordReader;
    protected Vec[] vecs;
    protected final long offset;
    protected final long length;
    protected float progress = 0.0f;
    protected final SerDeStats stats;
    protected List<Integer> included;
    protected Operator tableScanOp;

    OmniOrcRecordReader(Configuration conf, FileSplit split) throws IOException {
        TypeDescription schema = getDesiredRowTypeDescr(conf, false, Integer.MAX_VALUE);
        OrcConf.MAPRED_INPUT_SCHEMA.setString(conf, schema.toString());
        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf)
                .maxLength(OrcConf.MAX_FILE_LENGTH.getLong(conf)).filesystem(split.getPath().getFileSystem(conf));
        org.apache.orc.Reader.Options options = buildOptions(conf, split.getStart(), split.getLength());
        included = getReadColumnIDs(conf);
        TypeDescription requiredSchema = getRequiredSchema(schema);
        options.schema(requiredSchema);
        options.include(null);
        recordReader = new OrcColumnarBatchJniReader();
        recordReader.initializeReaderJava(split.getPath().toString(), readerOptions);
        recordReader.initializeRecordReaderJava(options);
        recordReader.initBatchJava(BATCH);
        vecs = new Vec[included.size()];
        this.offset = split.getStart();
        this.length = split.getLength();
        this.stats = new SerDeStats();
        Utilities.getMapWork(conf).getAliasToWork().values().forEach(op -> {
            if (op.getType().equals(OperatorType.TABLESCAN)) {
                this.tableScanOp = op;
            }
        });
    }

    private List<Integer> getReadColumnIDs(Configuration conf) {
        String skips = conf.get(READ_COLUMN_IDS_CONF_STR, "");
        return Arrays.stream(skips.split(",")).map(Integer::parseInt).distinct().collect(Collectors.toList());
    }

    private org.apache.orc.Reader.Options buildOptions(Configuration conf, long start, long length) {
        org.apache.orc.Reader.Options options = (new org.apache.orc.Reader.Options(conf)).range(start, length)
                .useZeroCopy(OrcConf.USE_ZEROCOPY.getBoolean(conf))
                .skipCorruptRecords(OrcConf.SKIP_CORRUPT_DATA.getBoolean(conf))
                .tolerateMissingSchema(OrcConf.TOLERATE_MISSING_SCHEMA.getBoolean(conf));
        String kryoSarg = OrcConf.KRYO_SARG.getString(conf);
        String sargColumns = OrcConf.SARG_COLUMNS.getString(conf);
        if (kryoSarg != null && sargColumns != null) {
            byte[] sargBytes = Base64.decodeBase64(kryoSarg);
            SearchArgument sarg = (SearchArgument) (new Kryo()).readObject(new Input(sargBytes),
                    SearchArgumentImpl.class);
            options.searchArgument(sarg, sargColumns.split(","));
            sarg.getExpression().toString();
        }
        return options;
    }

    private TypeDescription getRequiredSchema(TypeDescription schema) {
        Set<Integer> requiredIds = new HashSet<>(included);
        TypeDescription result = TypeDescription.createStruct();
        for (int i = 0; i < schema.getFieldNames().size(); i++) {
            if (requiredIds.contains(i)) {
                result.addField(schema.getFieldNames().get(i), schema.getChildren().get(i));
            }
        }
        return result;
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
        return offset + (long) (progress * length);
    }

    @Override
    public void close() throws IOException {
        if (recordReader != null) {
            recordReader.close();
            recordReader = null;
        }
    }

    @Override
    public float getProgress() throws IOException {
        return progress;
    }

    @Override
    public SerDeStats getStats() {
        return stats;
    }
}