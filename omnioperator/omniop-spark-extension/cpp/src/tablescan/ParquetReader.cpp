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

#include <iostream>
#include <string>
#include <arrow/filesystem/filesystem.h>
#include <type/decimal_operations.h>
#include "jni/jni_common.h"
#include "ParquetReader.h"

using namespace omniruntime::vec;
using namespace omniruntime::type;
using namespace arrow;
using namespace parquet::arrow;
using namespace arrow::compute;
using namespace spark::reader;

static std::mutex mutex_;
static std::map<std::string, Filesystem*> restore_filesysptr;
static constexpr int32_t PARQUET_MAX_DECIMAL64_DIGITS = 18;
static constexpr int32_t INT128_BYTES = 16;
static constexpr int32_t INT64_BYTES = 8;
static constexpr int32_t BYTE_BITS = 8;
static constexpr int32_t LOCAL_FILE_PREFIX = 5;
static constexpr int32_t READER_BUFFER_SIZE = 4096 * 4;
static const std::string LOCAL_FILE = "file:";
static const std::string HDFS_FILE = "hdfs:";

std::string spark::reader::GetFileSystemKey(std::string& path, std::string& ugi)
{
    // if the local file, all the files are the same key "file:"
    std::string result = ugi;

    // if the hdfs file, only get the ip and port just like the ugi + ip + port as key
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
        // process the path "file://" head, the arrow could not read the head
        path = path.substr(LOCAL_FILE_PREFIX);
        result += "file:";
        return result;
    }

    // if not the local, not the hdfs, get the ugi + path as the key
    result += path;
    return result;
}

Filesystem* spark::reader::GetFileSystemPtr(std::string& path, std::string& ugi)
{
    auto key = GetFileSystemKey(path, ugi);

    // if not find key, creadte the filesystem ptr
    auto iter = restore_filesysptr.find(key);
    if (iter == restore_filesysptr.end()) {
        Filesystem* fs = new Filesystem();
        fs->filesys_ptr = std::move(fs::FileSystemFromUriOrPath(path)).ValueUnsafe();
        restore_filesysptr[key] = fs;
    }

    return restore_filesysptr[key];
}

Status ParquetReader::InitRecordReader(std::string& filePath, int64_t capacity,
    const std::vector<int>& row_group_indices, const std::vector<int>& column_indices, std::string& ugi)
{
    arrow::MemoryPool* pool = default_memory_pool();

    // Configure reader settings
    auto reader_properties = parquet::ReaderProperties(pool);
    reader_properties.set_buffer_size(READER_BUFFER_SIZE);
    reader_properties.enable_buffered_stream();

    // Configure Arrow-specific reader settings
    auto arrow_reader_properties = parquet::ArrowReaderProperties();
    arrow_reader_properties.set_batch_size(capacity);

    // Get the file from filesystem
    mutex_.lock();
    Filesystem* fs = GetFileSystemPtr(filePath, ugi);
    mutex_.unlock();
    ARROW_ASSIGN_OR_RAISE(auto file, fs->filesys_ptr->OpenInputFile(filePath));

    FileReaderBuilder reader_builder;
    ARROW_RETURN_NOT_OK(reader_builder.Open(file, reader_properties));
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_properties);

    ARROW_ASSIGN_OR_RAISE(arrow_reader, reader_builder.Build());
    ARROW_RETURN_NOT_OK(arrow_reader->GetRecordBatchReader(row_group_indices, column_indices, &rb_reader));
    return arrow::Status::OK();
}

Status ParquetReader::ReadNextBatch(std::shared_ptr<RecordBatch> *batch)
{
    ARROW_RETURN_NOT_OK(rb_reader->ReadNext(batch));
    return arrow::Status::OK();
}

/**
 * For BooleanType, copy values one by one.
 */
uint64_t CopyBooleanType(std::shared_ptr<Array> array)
{
    arrow::BooleanArray *lvb = dynamic_cast<arrow::BooleanArray *>(array.get());
    auto numElements = lvb->length();
    auto originalVector = new Vector<bool>(numElements);
    for (int64_t i = 0; i < numElements; i++) {
        if (lvb->IsNull(i)) {
            originalVector->SetNull(i);
        } else {
            if (lvb->Value(i)) {
                originalVector->SetValue(i, true);
            } else {
                originalVector->SetValue(i, false);
            }
        }
    }
    return (uint64_t)originalVector;
}

/**
 * For int16/int32/int64/double type, copy values in batches and skip setNull if there is no nulls.
 */
template <DataTypeId TYPE_ID, typename PARQUET_TYPE> uint64_t CopyFixedWidth(std::shared_ptr<Array> array)
{
    using T = typename NativeType<TYPE_ID>::type;
    PARQUET_TYPE *lvb  = dynamic_cast<PARQUET_TYPE *>(array.get());
    auto numElements = lvb->length();
    auto values = lvb->raw_values();
    auto originalVector = new Vector<T>(numElements);
    // Check ColumnVectorBatch has null or not firstly
    if (lvb->null_count() != 0) {
        for (int64_t i = 0; i < numElements; i++) {
            if (lvb->IsNull(i)) {
                originalVector->SetNull(i);
            }
        }
    }
    originalVector->SetValues(0, values, numElements);
    return (uint64_t)originalVector;
}

uint64_t CopyVarWidth(std::shared_ptr<Array> array)
{
    auto lvb = dynamic_cast<StringArray *>(array.get());
    auto numElements = lvb->length();
    auto originalVector = new Vector<LargeStringContainer<std::string_view>>(numElements);
    for (int64_t i = 0; i < numElements; i++) {
        if (lvb->IsValid(i)) {
            auto data = lvb->GetView(i);
            originalVector->SetValue(i, data);
        } else {
            originalVector->SetNull(i);
        }
    }
    return (uint64_t)originalVector;
}

uint64_t CopyToOmniDecimal128Vec(std::shared_ptr<Array> array)
{
    auto lvb = dynamic_cast<arrow::Decimal128Array *>(array.get());
    auto numElements = lvb->length();
    auto originalVector = new Vector<omniruntime::type::Decimal128>(numElements);
    for (int64_t i = 0; i < numElements; i++) {
        if (lvb->IsValid(i)) {
            auto data = lvb->GetValue(i);
            __int128_t val;
            memcpy_s(&val, sizeof(val), data, INT128_BYTES);
            omniruntime::type::Decimal128 d128(val);
            originalVector->SetValue(i, d128);
        } else {
            originalVector->SetNull(i);
        }
    }
    return (uint64_t)originalVector;
}

uint64_t CopyToOmniDecimal64Vec(std::shared_ptr<Array> array)
{
    auto lvb = dynamic_cast<arrow::Decimal128Array *>(array.get());
    auto numElements = lvb->length();
    auto originalVector = new Vector<int64_t>(numElements);
    for (int64_t i = 0; i < numElements; i++) {
        if (lvb->IsValid(i)) {
            auto data = lvb->GetValue(i);
            int64_t val;
            memcpy_s(&val, sizeof(val), data, INT64_BYTES);
            originalVector->SetValue(i, val);
        } else {
            originalVector->SetNull(i);
        }
    }
    return (uint64_t)originalVector;
}

int spark::reader::CopyToOmniVec(std::shared_ptr<arrow::DataType> vcType, int &omniTypeId, uint64_t &omniVecId,
    std::shared_ptr<Array> array)
{
    switch (vcType->id()) {
        case arrow::Type::BOOL:
            omniTypeId = static_cast<jint>(OMNI_BOOLEAN);
            omniVecId = CopyBooleanType(array);
            break;
        case arrow::Type::INT16:
            omniTypeId = static_cast<jint>(OMNI_SHORT);
            omniVecId = CopyFixedWidth<OMNI_SHORT, arrow::Int16Array>(array);
            break;
        case arrow::Type::INT32:
            omniTypeId = static_cast<jint>(OMNI_INT);
            omniVecId = CopyFixedWidth<OMNI_INT, arrow::Int32Array>(array);
            break;
        case arrow::Type::DATE32:
            omniTypeId = static_cast<jint>(OMNI_DATE32);
            omniVecId = CopyFixedWidth<OMNI_INT, arrow::Date32Array>(array);
            break;
        case arrow::Type::INT64:
            omniTypeId = static_cast<jint>(OMNI_LONG);
            omniVecId = CopyFixedWidth<OMNI_LONG, arrow::Int64Array>(array);
            break;
        case arrow::Type::DATE64:
            omniTypeId = static_cast<jint>(OMNI_DATE64);
            omniVecId = CopyFixedWidth<OMNI_LONG, arrow::Date64Array>(array);
            break;
        case arrow::Type::DOUBLE:
            omniTypeId = static_cast<jint>(OMNI_DOUBLE);
            omniVecId = CopyFixedWidth<OMNI_DOUBLE, arrow::DoubleArray>(array);
            break;
        case arrow::Type::STRING:
            omniTypeId = static_cast<jint>(OMNI_VARCHAR);
            omniVecId = CopyVarWidth(array);
            break;
        case arrow::Type::DECIMAL128: {
            auto decimalType = static_cast<arrow::DecimalType *>(vcType.get());
            if (decimalType->precision() > PARQUET_MAX_DECIMAL64_DIGITS) {
                omniTypeId = static_cast<int>(OMNI_DECIMAL128);
                omniVecId = CopyToOmniDecimal128Vec(array);
            } else {
                omniTypeId = static_cast<int>(OMNI_DECIMAL64);
                omniVecId = CopyToOmniDecimal64Vec(array);
            }
            break;
        }
        default: {
            throw std::runtime_error("Native ColumnarFileScan Not support For This Type: " + vcType->id());
        }
    }
    return 1;
}

std::pair<int64_t*, int64_t*> spark::reader::TransferToOmniVecs(std::shared_ptr<RecordBatch> batch)
{
    int64_t num_columns = batch->num_columns();
    std::vector<std::shared_ptr<arrow::Field>> fields = batch->schema()->fields();
    auto vecTypes = new int64_t[num_columns];
    auto vecs = new int64_t[num_columns];
    for (int64_t colIdx = 0; colIdx < num_columns; colIdx++) {
        std::shared_ptr<Array> array = batch->column(colIdx);
        // One array in current batch
        std::shared_ptr<ArrayData> data = array->data();
        int omniTypeId = 0;
        uint64_t omniVecId = 0;
        spark::reader::CopyToOmniVec(data->type, omniTypeId, omniVecId, array);
        vecTypes[colIdx] = omniTypeId;
        vecs[colIdx] = omniVecId;
    }
    return std::make_pair(vecTypes, vecs);
}