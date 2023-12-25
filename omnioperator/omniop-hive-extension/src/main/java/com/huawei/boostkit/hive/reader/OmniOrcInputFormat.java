
package com.huawei.boostkit.hive.reader;

import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputForamt;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class OmniOrcInputFormat implements InputFormat<NullWritable, VecBatchWrapper> {
    private OrcInputFormat orcInputFormat = new OrcInputFormat();

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        return orcInputFormat.getSplits(job, numSplits);
    }

    @Override
    public RecordReader<NullWritable, VecBatchWrapper> getRecordReader(InputSplit inputSplit, JobConf jobConf,
            Reporter reporter) throws IOException {
        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf);
        if (inputSplit instanceof OrcSplit) {
            OrcSplit split = (OrcSplit) inputSplit;
            readerOptions.maxLength(split.getFileLength()).orcTail(split.getOrcTail());
        }
        if (Utilities.getIsVectorized(conf)) {
            return new OmniVectorizedOrcRecordReader(conf, (FileSplit) inputSplit);
        } else {
            return new OmniOrcRecordReader(conf, (FileSplit) inputSplit);
        }
    }

}