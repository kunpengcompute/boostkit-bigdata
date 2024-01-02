/**
 * Copyright (C) 2023-2023. Huawei Technologies Co., Ltd. All rights reserved.
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

#ifndef SPARK_PARQUET_COLUMN_READER_H
#define SPARK_PARQUET_COLUMN_READER_H

#include "ParquetTypedRecordReader.h"
#include <parquet/column_reader.h>
#include <parquet/arrow/reader_internal.h>

namespace spark::reader {
    class ParquetColumnReader {
    public:
        ParquetColumnReader(std::shared_ptr<::parquet::arrow::ReaderContext> ctx, std::shared_ptr<::arrow::Field> field,
                std::unique_ptr<::parquet::arrow::FileColumnIterator> input, ::parquet::internal::LevelInfo leaf_info)
            : ctx_(std::move(ctx)),
                field_(std::move(field)),
                input_(std::move(input)),
                descr_(input_->descr()) {
            record_reader_ = MakeRecordReader(descr_, leaf_info, ctx_->pool,
                field_->type()->id() == ::arrow::Type::DICTIONARY, field_->type());
            NextRowGroup();
        }

        ::arrow::Status NextBatch(int64_t batch_size, omniruntime::vec::BaseVector** out);

        ::arrow::Status LoadBatch(int64_t records_to_read, omniruntime::vec::BaseVector** out);

        const std::shared_ptr<::arrow::Field> field() {
            return field_;
        }

    private:
        void NextRowGroup();

        std::shared_ptr<::parquet::arrow::ReaderContext> ctx_;
        std::shared_ptr<::arrow::Field> field_;
        std::unique_ptr<::parquet::arrow::FileColumnIterator> input_;
        const ::parquet::ColumnDescriptor* descr_;
        std::shared_ptr<OmniRecordReader> record_reader_;
    };
}
#endif // SPARK_PARQUET_COLUMN_READER_H