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

#include <vector/vector_common.h>
#include <arrow/filesystem/filesystem.h>
#include "ParquetColumnReader.h"

namespace spark::reader {

    class OmniRecordBatchReader {
    public:
        OmniRecordBatchReader(std::function<arrow::Status(std::vector<omniruntime::vec::BaseVector*> &batch, long *batchRowSize)> batches)
            : batches_(std::move(batches)) {}

        ~OmniRecordBatchReader() {}

        Status ReadNext(std::vector<omniruntime::vec::BaseVector*> &out, long *batchRowSize) {
            return batches_(out, batchRowSize);
        }

    private:
        std::function<arrow::Status(std::vector<omniruntime::vec::BaseVector*> &batch, long *batchRowSize)> batches_;
    };


    class ParquetReader {
    public:
        ParquetReader() {}

        arrow::Status InitRecordReader(std::string& path, int64_t capacity,
            const std::vector<int>& row_group_indices, const std::vector<int>& column_indices, std::string& ugi);

        arrow::Status ReadNextBatch(std::vector<omniruntime::vec::BaseVector*> &batch, long *batchRowSize);

        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;

        std::unique_ptr<OmniRecordBatchReader> rb_reader;

        std::vector<std::shared_ptr<ParquetColumnReader>> columnReaders;

        arrow::MemoryPool* pool = arrow::default_memory_pool();

    private:
        arrow::Status GetRecordBatchReader(const std::vector<int> &row_group_indices, const std::vector<int> &column_indices);

        arrow::Status GetFieldReaders(const std::vector<int> &row_group_indices, const std::vector<int> &column_indices,
            std::vector<std::shared_ptr<ParquetColumnReader>>* out, std::shared_ptr<::arrow::Schema>* out_schema);

        arrow::Status GetFieldReader(int i, const std::shared_ptr<std::unordered_set<int>>& included_leaves,
            const std::vector<int> &row_group_indices, std::unique_ptr<ParquetColumnReader>* out);

        arrow::Status GetReader(const parquet::arrow::SchemaField &field, const std::shared_ptr<arrow::Field> &arrow_field,
            const std::shared_ptr<parquet::arrow::ReaderContext> &ctx, std::unique_ptr<ParquetColumnReader>* out);

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

    Filesystem* GetFileSystemPtr(std::string& path, std::string& ugi, arrow::Status &status);
}
#endif // SPARK_THESTRAL_PLUGIN_PARQUETREADER_H