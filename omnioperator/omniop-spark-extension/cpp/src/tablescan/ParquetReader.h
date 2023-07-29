/**
 * Copyright (C) 2020-2023. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SPARK_THESTRAL_PLUGIN_PARQUETREADER_H
#define SPARK_THESTRAL_PLUGIN_PARQUETREADER_H

#include <map>
#include <jni.h>
#include <vector/vector_common.h>
#include <type/data_type.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/util/checked_cast.h>
#include <parquet/arrow/reader.h>
#include <arrow/record_batch.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/type_fwd.h>
#include <arrow/dataset/file_base.h>
#include <arrow/type_fwd.h>
#include <arrow/util/checked_cast.h>

namespace spark::reader {
    class ParquetReader {
    public:
        ParquetReader() {}

        arrow::Status InitRecordReader(std::string& path, int64_t capacity,
            const std::vector<int>& row_group_indices, const std::vector<int>& column_indices, std::string& ugi);

        arrow::Status ReadNextBatch(std::shared_ptr<arrow::RecordBatch> *batch);

        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;

        std::shared_ptr<arrow::RecordBatchReader> rb_reader;
    };

    class Filesystem {
    public:
        Filesystem() {}

        /**
         * File system holds the hdfs client, which should outlive the RecordBatchReader.
         */
        std::shared_ptr<arrow::fs::FileSystem> filesys_ptr;
    };

    std::string GetFileSystemKey(std::string& path, std::string& ugi);

    Filesystem* GetFileSystemPtr(std::string& path, std::string& ugi);

    int CopyToOmniVec(std::shared_ptr<arrow::DataType> vcType, int &omniTypeId, uint64_t &omniVecId,
        std::shared_ptr<arrow::Array> array);

    std::pair<int64_t*, int64_t*> TransferToOmniVecs(std::shared_ptr<arrow::RecordBatch> batch);
}
#endif // SPARK_THESTRAL_PLUGIN_PARQUETREADER_H