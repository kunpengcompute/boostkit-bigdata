
package com.huawei.boostkit.hive.reader;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.List;

public class OmniParquetInputFormat extends FileInputFormat<NullWritable, VecBatchWrapper>
        implements InputFormatChecker {
    @Override
    public ReacordReader<NullWritable, VecBatchWrapper> getRecordReader(InputSplit inputSplit, JobConf conf,
            Reporter reporter) throws IOException {
        return new OmniParquetRecordReader(inputSplit, conf);
    }

    @Override
    public boolean validateInput(FileSystem fs, HiveConf conf, List<FileStatus> files) throws IOException {
        if (files.size() <= 0) {
            return false;
        }
        // The simple validity check is to see if the file is of size 0 or not.
        // Other checks maybe added in trhe future.
        for (FileStatus file : files) {
            if (file.getLen() == 0) {
                return false;
            }
        }
        return true;
    }
}