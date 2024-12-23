/**
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

#include "ParquetWriter.h"
#include "ParquetReader.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/data.h"
#include <arrow/type.h>
#include <arrow/array.h>
#include <arrow/api.h>
#include "arrow/util/bitmap.h"
#include "arrow/chunked_array.h"
#include "arrow/buffer_builder.h"
#include "arrow/table.h"
#include "arrowadapter/FileSystemAdapter.h"
#include "common/UriInfo.h"
#include "jni/jni_common.h"
#include "parquet/arrow/reader.h"
#include "parquet/exception.h"
#include "parquet/properties.h"
#include <filesystem>
#include <mutex>
#include <unordered_map>
#include <iostream>
#include <codecvt>
#include <locale>
#include <exception>
#include <stdexcept>
#include <stdio.h>

using namespace arrow;
using namespace arrow::internal;
using namespace parquet::arrow;
using namespace omniruntime::writer;
using namespace omniruntime::reader;

static std::mutex mutex_;

namespace omniruntime::writer
{

    arrow::Status ParquetWriter::InitRecordWriter(UriInfo &uri, std::string &ugi)
    {
        parquet::WriterProperties::Builder writer_properties;
        parquet::ArrowWriterProperties::Builder arrow_writer_properties;

        arrow::Status result;
        mutex_.lock();
        Filesystem *fs = GetFileSystemPtr(uri, ugi, result);
        mutex_.unlock();
        if (fs == nullptr || fs->filesys_ptr == nullptr) {
            return arrow::Status::IOError(result);
        }

        std::string uriPath = uri.ToString();
        std::filesystem::path path(std::string(uri.Path()));
        auto res = std::filesystem::create_directories(path.parent_path());
        if (!res) {
          throw std::runtime_error("Create local directories fail");
        }
        std::shared_ptr<io::OutputStream> outputStream;
        ARROW_ASSIGN_OR_RAISE(outputStream, fs->filesys_ptr->OpenOutputStream(path));

        writer_properties.disable_dictionary();
        auto fileWriterResult = FileWriter::Open(
            *schema_, arrow::default_memory_pool(), outputStream,
            writer_properties.build(), parquet::default_arrow_writer_properties());
        if (!fileWriterResult.ok()) {
            std::cerr<<"Error opening file writer: "<<fileWriterResult.status().ToString()<<std::endl;
            return fileWriterResult.status();
        }
        ARROW_ASSIGN_OR_RAISE(arrow_writer, fileWriterResult);
        auto pool = arrow::default_memory_pool();
        return arrow::Status::OK();
    }

    std::shared_ptr<::arrow::ChunkedArray> buildBooleanChunk(DataTypeId typeId, BaseVector *baseVector,
                                                             bool isSplitWrite = false, long startPos  = 0,
                                                             long endPos = 0)
    {
        using T = typename NativeType<OMNI_BOOLEAN>::type;
        auto vector = (Vector<T> *)baseVector;

        if (!isSplitWrite) {
            startPos = 0;
            endPos = vector->GetSize();
        }

        int64_t vectorSize = endPos - startPos;
        bool values[vectorSize];
        int64_t index = 0;
        auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
        arrow::internal::Bitmap bitmap(bitmapBuffer, 0, vectorSize);
        bitmap.SetBitsTo(true);

        if (vector->HasNull()) {
            for (long j = startPos; j < endPos; j++) {
                if (vector->IsNull(j)) {
                    bitmap.SetBitTo(index, false);
                } else if(isSplitWrite) {
                    values[index] = vector->GetValue(j);
                }
                index++;
            }
        } else if (isSplitWrite) {
              for (long j = startPos; j < endPos; j++) {
                   values[index] = vector->GetValue(j);
                   index++;
              }
        }

        TypedBufferBuilder<bool> builder;
        builder.Resize(vectorSize);

        builder.Append(reinterpret_cast<uint8_t *>(isSplitWrite?values:vector->GetValues()), vectorSize);
        auto maybe_buffer = builder.Finish();
        std::shared_ptr<arrow::Buffer> databuffer = *maybe_buffer;

        std::vector<std::shared_ptr<Buffer>> buffers;
        buffers.emplace_back(bitmapBuffer);
        buffers.emplace_back(databuffer);

        auto booleanType = std::make_shared<arrow::BooleanType>();
        auto arrayData = arrow::ArrayData::Make(booleanType, vectorSize, buffers);

        std::vector<std::shared_ptr<arrow::Array>> arrayVector;
        auto booleanArray = std::make_shared<arrow::BooleanArray>(arrayData);
        arrayVector.emplace_back(booleanArray);

        return arrow::ChunkedArray::Make(arrayVector, booleanType).ValueOrDie();
    }

    template<DataTypeId Type_ID, typename ArrowType, typename ChunkType>
    std::shared_ptr<::arrow::ChunkedArray> buildChunk(BaseVector *baseVector,
                                                           bool isSplitWrite = false, long startPos  = 0,
                                                           long endPos = 0)
    {
        using T=typename NativeType<Type_ID>::type;
        auto vector =(Vector<T> *)baseVector;
        if (!isSplitWrite) {
            startPos = 0;
            endPos = vector->GetSize();
        }
        int64_t vectorSize = endPos - startPos;
        ChunkType values[vectorSize];
        int64_t index = 0;

        auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
        arrow::internal::Bitmap bitmap(bitmapBuffer, 0, vectorSize);
        bitmap.SetBitsTo(true);
        if (vector->HasNull()) {
            for (long j = startPos; j < endPos; j++) {
                if (vector->IsNull(j)) {
                    bitmap.SetBitTo(index, false);
                } else if (isSplitWrite) {
                    values[index] = vector->GetValue(j);
                }
                index++;
            }
        } else if (isSplitWrite) {
               for (long j = startPos; j < endPos; j++) {
                    values[index] = vector->GetValue(j);
                    index++;
               }
        }

        TypedBufferBuilder<ChunkType> builder;
        builder.Resize(vectorSize);
        builder.Append(isSplitWrite?values:vector->GetValues(), vectorSize);
        auto dataBuffer = *builder.Finish();
        std::vector<std::shared_ptr<Buffer>> buffers;
        buffers.emplace_back(bitmapBuffer);
        buffers.emplace_back(dataBuffer);

        auto arrowType = std::make_shared<ArrowType>();
        auto arrayData = arrow::ArrayData::Make(arrowType, vectorSize, buffers);
        std::vector<std::shared_ptr<arrow::Array>> arrayVector;
        auto arrowArray = std::make_shared<NumericArray<ArrowType>>(arrayData);
        arrayVector.emplace_back(arrowArray);
        return ChunkedArray::Make(arrayVector, arrowType).ValueOrDie();
    }

    std::shared_ptr<ChunkedArray> buildVarcharChunk(DataTypeId typeId, BaseVector *baseVector,
                                                    bool isSplitWrite = false, long startPos  = 0,
                                                    long endPos = 0)
    {
        auto vector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(baseVector);

        if (!isSplitWrite) {
            startPos = 0;
            endPos = vector->GetSize();
        }

        int64_t vectorSize = endPos - startPos;
        auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
        arrow::internal::Bitmap bitmap(bitmapBuffer, 0, vectorSize);
        bitmap.SetBitsTo(true);

        TypedBufferBuilder<int32_t> offsetsBuilder;
        TypedBufferBuilder<char> valuesBuilder;
        int32_t currentOffset = 0;
        offsetsBuilder.Append(0);
        valuesBuilder.Resize(vectorSize);

        int64_t index = 0;
        for (long j = startPos; j < endPos; j++) {
            if (vector->IsNull(j)) {
                bitmap.SetBitTo(index++, false);
            }
            index++;
            std::string strValue = std::string(vector->GetValue(j));
            size_t length = strValue.length();
            currentOffset += length;
            offsetsBuilder.Append(currentOffset);
            valuesBuilder.Append(strValue.data(), length);
        }

        auto offsetsBuffer = offsetsBuilder.Finish().ValueOrDie();
        auto valuesBuffer = valuesBuilder.Finish().ValueOrDie();

        std::vector<std::shared_ptr<Buffer>> buffers;

        buffers.emplace_back(bitmapBuffer);
        buffers.emplace_back(offsetsBuffer);
        buffers.emplace_back(valuesBuffer);

        auto utf8Type = std::make_shared<arrow::StringType>();
        auto arrayData = arrow::ArrayData::Make(utf8Type, vectorSize, buffers);

        std::vector<std::shared_ptr<Array>> arrayVector;
        auto stringArray = std::make_shared<StringArray>(arrayData);
        arrayVector.emplace_back(stringArray);

        return ChunkedArray::Make(arrayVector, utf8Type).ValueOrDie();
    }

    std::shared_ptr<arrow::ChunkedArray> buildDecimal64Chunk(DataTypeId typeId, BaseVector *baseVector, 
                                                            int precision, int scale, bool isSplitWrite = false, 
                                                            long startPos  = 0, long endPos = 0)
    {
        using T = typename NativeType<OMNI_DECIMAL64>::type;
        auto vector = (Vector<T> *)baseVector;

        if (!isSplitWrite) {
            startPos = 0;
            endPos = vector->GetSize();
        }
        int64_t vectorSize = endPos - startPos;
        auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
        arrow::internal::Bitmap bitmap(bitmapBuffer, 0, vectorSize);
        bitmap.SetBitsTo(true);
        BufferBuilder builder;
        builder.Resize(vectorSize);
        std::vector<arrow::Decimal128> decimalArray;

        int64_t index = 0;
        for (long j = startPos; j < endPos; j++) {
             BasicDecimal128 basicDecimal128(0, vector->GetValue(j));
             decimalArray.emplace_back(BasicDecimal128(basicDecimal128));
            if (vector->IsNull(j)) {
                bitmap.SetBitTo(index, false);
            }
            index++;
        }

        builder.Append(decimalArray.data(), decimalArray.size() * sizeof(arrow::Decimal128));
         auto dataBuffer = *builder.Finish();
        std::vector<std::shared_ptr<Buffer>> buffers;
        buffers.emplace_back(bitmapBuffer);
        buffers.emplace_back(dataBuffer);

        auto decimal128Type = std::make_shared<arrow::Decimal128Type>(precision, scale);
        auto arrayData = arrow::ArrayData::Make(decimal128Type, vectorSize, buffers);
        std::vector<std::shared_ptr<Array>> arrayVector;
        auto decimal128Array = std::make_shared<Decimal128Array>(arrayData);
        arrayVector.emplace_back(decimal128Array);
        return ChunkedArray::Make(arrayVector, decimal128Type).ValueOrDie();
    }

    std::shared_ptr<ChunkedArray> buildDecimal128Chunk(DataTypeId typeId, BaseVector *baseVector, 
                                                      int precision, int scale, bool isSplitWrite = false, 
                                                      long startPos  = 0, long endPos = 0)
    {
        using T = typename NativeType<OMNI_DECIMAL128>::type;
        auto vector = (Vector<T> *)baseVector;

        if (!isSplitWrite) {
            startPos = 0;
            endPos = vector->GetSize();
        }
        int64_t vectorSize = endPos - startPos;
        auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
        arrow::internal::Bitmap bitmap(bitmapBuffer, 0, vectorSize);
        bitmap.SetBitsTo(true);
        BufferBuilder builder;
        builder.Resize(vectorSize);
        std::vector<arrow::Decimal128> decimalArray;

        int64_t index = 0;
        for (long j = startPos; j < endPos; j++) {
            auto decimalValue = vector->GetValue(j);
            BasicDecimal128 basicDecimal128(vector->GetValue(j).HighBits(), vector->GetValue(j).LowBits());
            decimalArray.emplace_back(BasicDecimal128(basicDecimal128));
            if (vector->IsNull(j)) {
                bitmap.SetBitTo(index, false);
            }
            index++;
        }

        builder.Append(decimalArray.data(), decimalArray.size() * sizeof(arrow::Decimal128));
        auto dataBuffer = *builder.Finish();
        std::vector<std::shared_ptr<Buffer>> buffers;
        buffers.emplace_back(bitmapBuffer);
        buffers.emplace_back(dataBuffer);

        auto decimal128Type = std::make_shared<arrow::Decimal128Type>(precision, scale);
        auto arrayData = arrow::ArrayData::Make(decimal128Type, vectorSize, buffers);
        std::vector<std::shared_ptr<Array>> arrayVector;
        auto decimal128Array = std::make_shared<Decimal128Array>(arrayData);
        arrayVector.emplace_back(decimal128Array);
        return ChunkedArray::Make(arrayVector, decimal128Type).ValueOrDie();
    }

    void ParquetWriter::write(long *vecNativeId, int colNums,
                              const int *omniTypes,
                              const unsigned char *dataColumnsIds,
                              bool isSplitWrite, long startPos , long endPos)
    {
        std::vector<std::shared_ptr<::arrow::ChunkedArray>> chunks;
        int decimalIndex = 0;
        int precision = 0;
        int scale = 0;
        for (int i = 0; i < colNums; ++i) {
            if (!dataColumnsIds[i]) {
                continue;
            }

            auto vec = (BaseVector *)vecNativeId[i];
            auto typeId = static_cast<DataTypeId>(omniTypes[i]);
            switch (typeId) {
            case OMNI_BOOLEAN:
                chunks.emplace_back(buildBooleanChunk(typeId, vec, isSplitWrite, startPos, endPos));
                break;
            case OMNI_SHORT:
                chunks.emplace_back(buildChunk<OMNI_SHORT, arrow::Int16Type, int16_t>(vec, isSplitWrite, startPos, endPos));
                break;
            case OMNI_INT:
                chunks.emplace_back(buildChunk<OMNI_INT, arrow::Int32Type, int32_t>(vec, isSplitWrite, startPos, endPos));
                break;
            case OMNI_LONG:
                chunks.emplace_back(buildChunk<OMNI_LONG, arrow::Int64Type, int64_t>(vec, isSplitWrite, startPos, endPos));
                break;
            case OMNI_DATE32:
                chunks.emplace_back(buildChunk<OMNI_DATE32, arrow::Date32Type, int32_t>(vec, isSplitWrite, startPos, endPos));
                break;
            case OMNI_DATE64:
                chunks.emplace_back(buildChunk<OMNI_DATE64, arrow::Date64Type, int64_t>(vec, isSplitWrite, startPos, endPos));
                break;
            case OMNI_DOUBLE:
                chunks.emplace_back(buildChunk<OMNI_DOUBLE, arrow::DoubleType, double>(vec, isSplitWrite, startPos, endPos));
                break;
            case OMNI_VARCHAR:
                chunks.emplace_back(buildVarcharChunk(typeId, vec, isSplitWrite, startPos, endPos));
                break;
            case OMNI_DECIMAL64:
                precision = precisions[decimalIndex];
                scale = scales[decimalIndex];
                chunks.emplace_back(buildDecimal64Chunk(typeId, vec, precision, scale, isSplitWrite, startPos, endPos));
                decimalIndex++;
                break;
            case OMNI_DECIMAL128:
                precision = precisions[decimalIndex];
                scale = scales[decimalIndex];
                chunks.emplace_back(buildDecimal128Chunk(typeId, vec, precision, scale, isSplitWrite, startPos, endPos));
                decimalIndex++;
                break;
            default:
                throw std::runtime_error(
                    "Native columnar write not support for this type: " + std::to_string(typeId));
            }
        }
        auto numRows = chunks.empty() ? 0 : chunks[0]->length();

        auto table = arrow::Table::Make(schema_, std::move(chunks), numRows);
        if (!arrow_writer) {
            throw std::runtime_error("Arrow writer is not initialized");
        }
        PARQUET_THROW_NOT_OK(arrow_writer->WriteTable(*table));
        PARQUET_THROW_NOT_OK(arrow_writer->Close());
    }

} // namespace omniruntime::writer
