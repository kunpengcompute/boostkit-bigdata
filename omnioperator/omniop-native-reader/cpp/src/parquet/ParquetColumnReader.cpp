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

#include "ParquetColumnReader.h"

using namespace omniruntime::vec;

namespace omniruntime::reader {

Status ParquetColumnReader::NextBatch(int64_t batch_size, BaseVector** out)
{
    RETURN_NOT_OK(LoadBatch(batch_size, out));
    return Status::OK();
}

Status ParquetColumnReader::LoadBatch(int64_t records_to_read, BaseVector** out)
{
    BEGIN_PARQUET_CATCH_EXCEPTIONS
    record_reader_->Reset();
    record_reader_->Reserve(records_to_read);
    while (records_to_read > 0) {
        if (!record_reader_->HasMoreData()) {
            break;
        }
        int64_t records_read = record_reader_->ReadRecords(records_to_read);
        records_to_read -= records_read;
        if (records_read == 0) {
            NextRowGroup();
        }
    }

    *out = record_reader_->GetBaseVec();
    if (*out == nullptr) {
        return Status::Invalid("Parquet Read OmniVector is nullptr!");
    }
    return Status::OK();
    END_PARQUET_CATCH_EXCEPTIONS
}

void ParquetColumnReader::NextRowGroup()
{
    std::unique_ptr<parquet::PageReader> page_reader = input_->NextChunk();
    record_reader_->SetPageReader(std::move(page_reader));
}

}