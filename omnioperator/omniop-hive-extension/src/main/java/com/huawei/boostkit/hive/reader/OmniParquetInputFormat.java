/*
 * Copyright (C) 2023-2024. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.hive.reader;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.List;

public class OmniParquetInputFormat extends FileInputFormat<NullWritable, VecBatchWrapper>
        implements InputFormatChecker {
    @Override
    public RecordReader<NullWritable, VecBatchWrapper> getRecordReader(InputSplit inputSplit, JobConf conf,
                                                                       Reporter reporter) throws IOException {
        if (Utilities.getIsVectorized(conf)) {
            return new OmniVectorizedParquetRecordReader(inputSplit, conf);
        } else {
            return new OmniParquetRecordReader(inputSplit, conf);
        }
    }

    @Override
    public boolean validateInput(FileSystem fs, HiveConf conf, List<FileStatus> files) throws IOException {
        if (files.size() <= 0) {
            return false;
        }
        // The simple validity check is to see if the file is of size 0 or not.
        // Other checks maybe added in the future.
        for (FileStatus file : files) {
            if (file.getLen() == 0) {
                return false;
            }
        }
        return true;
    }
}
