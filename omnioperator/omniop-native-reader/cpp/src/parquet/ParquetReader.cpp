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

#include <unordered_map>
#include "jni/jni_common.h"
#include "ParquetReader.h"
#include "arrowadapter/FileSystemAdapter.h"
#include "common/UriInfo.h"

using namespace arrow;
using namespace parquet::arrow;
using namespace omniruntime::reader;
using namespace arrow::internal;

static std::mutex mutex_;
static std::unordered_map<std::string, Filesystem*> restore_filesysptr;
static constexpr int32_t LOCAL_FILE_PREFIX = 5;
static const std::string LOCAL_FILE = "file:";
static const std::string HDFS_FILE = "hdfs:";

// the ugi is UserGroupInformation
std::string omniruntime::reader::GetFileSystemKey(std::string& path, std::string& ugi)
{
    // if the local file, all the files are the same key "file:"
    std::string result = ugi;

    // if the hdfs file, only get the ip and port just like the ugi + ip + Port as key
    if (path.substr(0, LOCAL_FILE_PREFIX) == HDFS_FILE) {
        auto mid = path.find(":", LOCAL_FILE_PREFIX);
        auto end = path.find("/", mid);
        std::string s1 = path.substr(LOCAL_FILE_PREFIX, mid - LOCAL_FILE_PREFIX);
        std::string s2 = path.substr(mid + 1, end - (mid + 1));
        result += s1 + ":" + s2;
        return result;
    }

    // if the local file, get the ugi + "file" as the key
    if (path.substr(0, LOCAL_FILE_PREFIX) == LOCAL_FILE) {
        // process the Path "file://" head, the arrow could not read the head
        path = path.substr(LOCAL_FILE_PREFIX);
        result += "file:";
        return result;
    }

    // if not the local, not the hdfs, get the ugi + Path as the key
    result += path;
    return result;
}

Filesystem* omniruntime::reader::GetFileSystemPtr(UriInfo &uri, std::string &ugi, arrow::Status &status)
{
    std::string fullPath = uri.ToString();
    auto key = GetFileSystemKey(fullPath, ugi);

    // if not find key, create the filesystem ptr
    auto iter = restore_filesysptr.find(key);
    if (iter == restore_filesysptr.end()) {
        Filesystem* fs = new Filesystem();
        auto result = arrow_adapter::FileSystemFromUriOrPath(uri);
        status = result.status();
        if (!status.ok()) {
          return nullptr;
        }
        fs->filesys_ptr = std::move(result).ValueUnsafe();
        restore_filesysptr[key] = fs;
    }

    return restore_filesysptr[key];
}

Status ParquetReader::InitRecordReader(UriInfo &uri, int64_t capacity,
    const std::vector<int>& row_group_indices, const std::vector<int>& column_indices,
    std::string& ugi)
{
    // Configure reader settings
    auto reader_properties = parquet::ReaderProperties(pool);

    // Configure Arrow-specific reader settings
    auto arrow_reader_properties = parquet::ArrowReaderProperties();
    arrow_reader_properties.set_batch_size(capacity);

    // Get the file from filesystem
    Status result;
    mutex_.lock();
    Filesystem* fs = GetFileSystemPtr(uri, ugi, result);
    mutex_.unlock();
    if (fs == nullptr || fs->filesys_ptr == nullptr) {
      return Status::IOError(result);
    }
    std::string path = uri.ToString();
    ARROW_ASSIGN_OR_RAISE(auto file, fs->filesys_ptr->OpenInputFile(path));

    FileReaderBuilder reader_builder;
    ARROW_RETURN_NOT_OK(reader_builder.Open(file, reader_properties));
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_properties);

    ARROW_ASSIGN_OR_RAISE(arrow_reader, reader_builder.Build());
    ARROW_RETURN_NOT_OK(GetRecordBatchReader(row_group_indices, column_indices));
    return arrow::Status::OK();
}

Status ParquetReader::ReadNextBatch(std::vector<omniruntime::vec::BaseVector*> &batch, long *batchRowSize)
{
    ARROW_RETURN_NOT_OK(rb_reader->ReadNext(batch, batchRowSize));
    return arrow::Status::OK();
}

Status ParquetReader::GetRecordBatchReader(const std::vector<int> &row_group_indices,
    const std::vector<int> &column_indices)
{
    std::shared_ptr<::arrow::Schema> batch_schema;
    RETURN_NOT_OK(GetFieldReaders(row_group_indices, column_indices, &columnReaders, &batch_schema));

    int64_t num_rows = 0;
    for(int row_group : row_group_indices) {
        num_rows += arrow_reader->parquet_reader()->metadata()->RowGroup(row_group)->num_rows();
    }
    // Use lambda function to generate BaseVectors
    auto batches = [num_rows, this](std::vector<omniruntime::vec::BaseVector*> &batch,
        long *batchRowSize) mutable -> Status {
            int64_t read_size = std::min(arrow_reader->properties().batch_size(), num_rows);
            num_rows -= read_size;
            *batchRowSize = read_size;

            if (columnReaders.empty() || read_size <= 0) {
                return Status::OK();
            }

            try {
                for (uint64_t i = 0; i < columnReaders.size(); i++) {
                    RETURN_NOT_OK(columnReaders[i]->NextBatch(read_size, &batch[i]));
                }
            } catch (const std::exception &e) {
                return Status::Invalid(e.what());
            }

            // Check BaseVector
            for (const auto& column : batch) {
                if (column == nullptr) {
                    return Status::Invalid("BaseVector should not be nullptr after reading");
                }
            }

            return Status::OK();
    };

    rb_reader = std::make_unique<OmniRecordBatchReader>(std::move(batches));
    return Status::OK();
}

std::shared_ptr<std::unordered_set<int>> VectorToSharedSet(const std::vector<int> &values) {
    std::shared_ptr<std::unordered_set<int>> result(new std::unordered_set<int>());
    result->insert(values.begin(), values.end());
    return result;
}

Status ParquetReader::GetFieldReaders(const std::vector<int> &row_group_indices, const std::vector<int> &column_indices,
    std::vector<std::shared_ptr<ParquetColumnReader>>* out, std::shared_ptr<::arrow::Schema>* out_schema)
{
    // We only read schema fields which have columns indicated in the indices vector
    ARROW_ASSIGN_OR_RAISE(std::vector<int> field_indices, arrow_reader->manifest().GetFieldIndices(column_indices));
    auto included_leaves = VectorToSharedSet(column_indices);
    out->resize(field_indices.size());
    ::arrow::FieldVector out_fields(field_indices.size());

    for (size_t i = 0; i < out->size(); i++) {
        std::unique_ptr<ParquetColumnReader> reader;
        RETURN_NOT_OK(GetFieldReader(field_indices[i], included_leaves, row_group_indices, &reader));
        out_fields[i] = reader->field();
        out->at(i) = std::move(reader);
    }

    *out_schema = ::arrow::schema(std::move(out_fields), arrow_reader->manifest().schema_metadata);
    return Status::OK();
}

FileColumnIteratorFactory SomeRowGroupsFactory(std::vector<int> row_group_indices) {
    return [row_group_indices] (int i, parquet::ParquetFileReader* reader) {
        return new FileColumnIterator(i, reader, row_group_indices);
    };
}

Status ParquetReader::GetFieldReader(int i, const std::shared_ptr<std::unordered_set<int>>& included_leaves,
    const std::vector<int> &row_group_indices, std::unique_ptr<ParquetColumnReader>* out)
{
    if (ARROW_PREDICT_FALSE(i < 0 || static_cast<size_t>(i) >= arrow_reader->manifest().schema_fields.size())) {
        return Status::Invalid("Column index out of bounds (got ", i,
            ", should be between 0 and ", arrow_reader->manifest().schema_fields.size(), ")");
    }
    auto ctx = std::make_shared<ReaderContext>();
    ctx->reader = arrow_reader->parquet_reader();
    ctx->pool = pool;
    ctx->iterator_factory = SomeRowGroupsFactory(row_group_indices);
    ctx->filter_leaves = true;
    ctx->included_leaves = included_leaves;
    auto field = arrow_reader->manifest().schema_fields[i];
    return GetReader(field, field.field, ctx, out);
}

Status ParquetReader::GetReader(const SchemaField &field, const std::shared_ptr<Field> &arrow_field,
    const std::shared_ptr<ReaderContext> &ctx, std::unique_ptr<ParquetColumnReader> *out)
{
    BEGIN_PARQUET_CATCH_EXCEPTIONS

    auto type_id = arrow_field->type()->id();

    if (type_id == ::arrow::Type::EXTENSION) {
        return Status::Invalid("Unsupported type: ", arrow_field->ToString());
    }

    if (field.children.size() == 0) {
        if (!field.is_leaf()) {
            return Status::Invalid("Parquet non-leaf node has no children");
        }
        if (!ctx->IncludesLeaf(field.column_index)) {
            *out = nullptr;
            return Status::OK();
        }
        std::unique_ptr<FileColumnIterator> input(ctx->iterator_factory(field.column_index, ctx->reader));
        *out = std::make_unique<ParquetColumnReader>(ctx, arrow_field, std::move(input), field.level_info);
    } else {
        return Status::Invalid("Unsupported type: ", arrow_field->ToString());
    }
    return Status::OK();

    END_PARQUET_CATCH_EXCEPTIONS
}