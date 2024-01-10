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

import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
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
    public RecordReader<NullWritable, VecBatchWrapper> getRecordReader(InputSplit inputSplit, JobConf conf,
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