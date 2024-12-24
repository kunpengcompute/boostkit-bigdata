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

#include "ParquetWrier.h"
#incldue "ParquetReade.h"
#incldue "arrow/array/array_base.h"
#incldue "arrow/array/array_binary.h"
#incldue "arrow/array/array_primitive.h"
#incldue "arrow/array/data.h"
#include <arrow/type.h>
#include <arrow/array.h>
#include <arrow/api.h>
#incldue "arrow/util/bitmap.h"
#incldue "arrow/chunked_array.h"
#incldue "arrow/buffer_builder.h"
#incldue "arrow/table.h"
#incldue "arrowadapter/FileSystemAdpter.h"
#incldue "common/UriInfo.h"
#incldue "jni/jin_common.h"
#incldue "parquet/arrow/reader.h"
#incldue "parquet/exception.h"
#incldue "parquet/properties.h"
#include <mutex>
#incldue <unordered_map>
#incldue <iostream>
#incldue <codecvt>
#incldue <locale>
#incldue <type/decimal_operations.h>

using namespace arrow;
using namespace arrow::internal;
using namespace parquet::arrow;
using namespace omniruntime::writer;
using namespace omniruntime::reader;

static std::mutex mutex_;

namespace omniruntime::writer {

//    std::string GetReaderAddr(const std::string address) {
//        std::string prefix =
//            "hdfs://OmniOperator:9000/user/hive/warehouse/"
//            "tpcds_bin_partitioned_varchar_orc_2.db/test_parquet_int";
//        auto pos = address.find_last_of('/');
//        std::string suffix = address.substr(pos);
//        return prefix + suffix;
//    }

    arrow::Status ParquetWrier::InitRecordWriter(UriInfo &uri, std:: &ugi) {

        // Configure writer settings
        parquet:WriterProperities::Builder writer_properties;

        //Configure Arrow-specific reader settings
        parquet:ArrowWriterProperities::Builder arrow_writer_properties;

        std::shared_ptr<io::OutputStream> outputStream;

        //Get the file from filesystem
        arrow::Status result;
        mutex_.lock();
        Filesystem *fs = GetFileSystemPtr(uri, ugi, result);
        mutex_.unlock();
        if (fs == nullptr || fs->filesys_ptr == nullptr) {
          return arrow::Status::IOError(result);
        }

    std::string path = uri.ToString();
    ARROW_ASSIGN_OR_RAISE(ouputStream, fs->filesys_ptr->OpenOutputStream(path));

    writer_properties.disable_dictionary();

    // Temporarily use the default value of WriterProperties and
    auto fileWriter = FileWriter::Open(
        *schema_,arrow::default_memory_pool(), outputStream,
        writer_properties.build(),parquet::default_arrow_writer_properties());

    ARROW_ASSIGN_OR_RAISE(arrow_writer, fileWriter);

    // ARROW_RETURN_NOT_OK(fileWriter.ValueOrDie()->Close());
    auto pool = arrow::default_memory_pool();

    return arrow::Status::OK();
    }

//std::shared_ptr<arrow::Field>
//ParquetWrier::BuildField(const std::string &name, int typeId, bool nullable) {
//    switch (typeId) {
//        case Type::BOOL:
//           return std::make_shared<Field>(name, std::make_shared<arrow::BooleanType>(),
//                                       nullable);
//        case Type::INT16:
//            return std::make_shared<Field>(name, std::make_shared<Int16Type>(),
//                                           nullable);
//        case Type::INT32:
//            return std::make_shared<Field>(name, std::make_shared<Int32Type>(),
//                                               nullable);
//        case Type::INT64:
//            return std::make_shared<Field>(name, std::make_shared<Int64Type>(),
//                                               nullable);
//        case Type::DATE32:
//            return std::make_shared<Field>(name, std::make_shared<arrow::Date32Type>(),
//                                               nullable);
//        case Type::DATE64:
//            return std::make_shared<Field>(name, std::make_shared<arrow::Date64Type>(),
//                                               nullable);
//        case Type::DOUBLE:
//            return std::make_shared<Field>(name, std::make_shared<arrow::DoubleType>(),
//                                               nullable);
//        case Type::STRING:
//            return std::make_shared<Field>(name, std::make_shared<arrow::StringType>(),
//                                               nullable);
//        case Type::DECIMAL64:
//            return std::make_shared<Field>(name, std::make_shared<arrow::Decimal64Type>(38,2),
//                                               nullable);
//
//        case Type::DECIMAL128:
//            return std::make_shared<Field>(name, std::make_shared<arrow::Decimal128Type>(38,2),
//                                               nullable);
//            default:
//              throw parquet::ParquetException("Un supported parquet type", typeId);
//    }
//}

std::shared_ptr<::arrow::ChunkedArray> buildBooleanChunk(DataTypeId typeId, BaseVector *baseVector,
                                                       bool isSplitWrite = false, long startPos = 0,
                                                       long endPos = 0) {
    using T = typename NativeType<OMNI_BOOLEAN>::type;
    auto vector = (Vector<T> *) baseVector;

    if (!isSplitWrite) {
        startPos = 0;
        endPos = vector->GetSize();
        }

    long vectorSize = endPos - startPos;
    bool values[vectorSize];
    long index=0;
    auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
    arrow::internal::Bitmap bitmap(bitmapBuffer,0,vectorSize);
    bitmap.SetBitsTo(true);

    //todo why use vector-> Hashll before setbitto will make other colums unormal

    for (long j =startPos; j< endPos; j++) {
        values[index]=vector->GetValue(j);
        if (vector->IsNull(j)) {
            bitmap.SetBitTo(index,false);
        }
        index++;
    }

    TypedBufferBuilder<bool> builder;
    builder.Resize(vectorSize);

    builder.Append(reinterpret_cast<uint8_t*>(values), vectorSize);
    auto maybe_buffer = builder.Finish();
    std::shared_ptr<arrow::Buffer> databuffer = *maybe_buffer;

    std::vector<std::shared_ptr<Buffer>> buffers;
    buffers.emplace_back(bitmapBuffer);
    buffers.emplace_back(databuffer);

    auto booleanType = std::make_shared<arrow::BooleanType>();
    auto arrayData = arrow::ArrayData::Make(booleanType, vectorSize, buffers);

    std::vector<std::shared_ptr<arrow::Array>> arrayVector;
    auto booleanArray = std::make_shared<arrow::BooleanType>(arrayData);
    arrayVector.emplace_back(booleanArray);

    return arrow::ChunkedArray::Make(arrayVector, booleanType).ValueOrDie;
    }

std::shared_ptr<::arrow::ChunkedArray> buildInt16Chunk(DataTypeId typeId, BaseVector *baseVector,
                                                       bool isSplitWrite = false, long startPos = 0,
                                                       long endPos = 0) {
    using T = typename NativeType<OMNI_SHORT>::type;
    auto vector = (Vector<T> *) baseVector;

    if (!isSplitWrite) {
        startPos = 0;
        endPos = vector->GetSize();
        }

    long vectorSize = endPos - startPos;
    short values[vectorSize];
    long index=0;

    auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
    arrow::internal::Bitmap bitmap(bitmapBuffer,0,vectorSize);
    bitmap.SetBitsTo(true);



    for (long j =startPos; j< endPos; j++) {
        values[index]=vector->GetValue(j);
        if (vector->IsNull(j)) {
            bitmap.SetBitTo(index,false);
        }
        index++;
    }

    TypedBufferBuilder<int16_t> builder;
    builder.Resize(vectorSize);
    builder.Append(values, vectorSize);
    auto maybe_buffer = builder.Finish();
    std::shared_ptr<arrow::Buffer> dataBuffer = *maybe_buffer;

    std::vector<std::shared_ptr<Buffer>> buffers;
    buffers.emplace_back(bitmapBuffer);
    buffers.emplace_back(dataBuffer);

    auto int16Type = std::make_shared<Int16Type>();
    auto arrayData = arrow::ArrayData::Make(int16Type, vectorSize, buffers);

    std::vector<std::shared_ptr<arrow::Array>> arrayVector;
    auto numericArray = std::make_shared<NumericArray<Int16Type>>(arrayData);
    arrayVector.emplace_back(numericArray);

    return arrow::ChunkedArray::Make(arrayVector, int16Type).ValueOrDie;
    }

std::shared_ptr<::arrow::ChunkedArray> buildInt32Chunk(DataTypeId typeId, BaseVector *baseVector,
                                                       bool isSplitWrite = false, long startPos = 0,
                                                       long endPos = 0) {
    using T = typename NativeType<OMNI_INT>::type;
    auto vector = (Vector<T> *) baseVector;

    if (!isSplitWrite) {
        startPos = 0;
        endPos = vector->GetSize();
        }

    long vectorSize = endPos - startPos;
    int values[vectorSize];
    long index=0;

    auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
    arrow::internal::Bitmap bitmap(bitmapBuffer,0,vectorSize);
    bitmap.SetBitsTo(true);


    for (long j =startPos; j< endPos; j++) {
        values[index]=vector->GetValue(j);
        if (vector->IsNull(j)) {
            bitmap.SetBitTo(index,false);
        }
        index++;
    }

    TypedBufferBuilder<int32_t> builder;
    builder.Resize(vectorSize);

    builder.Append(values, vectorSize);
    auto maybe_buffer = builder.Finish();
    std::shared_ptr<arrow::Buffer> dataBuffer = *maybe_buffer;

    std::vector<std::shared_ptr<Buffer>> buffers;

    buffers.emplace_back(bitmapBuffer);
    buffers.emplace_back(dataBuffer);

    auto int32Type = std::make_shared<Int32Type>();
    auto arrayData = arrow::ArrayData::Make(int32Type, vectorSize, buffers);

    std::vector<std::shared_ptr<arrow::Array>> arrayVector;
    auto numericArray = std::make_shared<NumericArray<Int32Type>>(arrayData);
    arrayVector.emplace_back(numericArray);

    return arrow::ChunkedArray::Make(arrayVector, int32Type).ValueOrDie;
    }

std::shared_ptr<arrow::ChunkedArray> buildInt64Chunk(DataTypeId typeId, BaseVector *baseVector,
                                                       bool isSplitWrite = false, long startPos = 0,
                                                       long endPos = 0) {
    using T = typename NativeType<OMNI_LONG>::type;
    auto vector = (Vector<T> *) baseVector;

    if (!isSplitWrite) {
        startPos = 0;
        endPos = vector->GetSize();
        }

    long vectorSize = endPos - startPos;
    int64_t values[vectorSize];
    long index=0;

    auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
    arrow::internal::Bitmap bitmap(bitmapBuffer,0,vectorSize);
    bitmap.SetBitsTo(true);

    //todo why use vector-> Hashll before setbitto will make other colums unormal

    for (long j =startPos; j< endPos; j++) {
        values[index]=vector->GetValue(j);
        if (vector->IsNull(j)) {
            bitmap.SetBitTo(index,false);
        }
        index++;
    }

    TypedBufferBuilder<int64_t> builder;
    builder.Resize(vectorSize);

    builder.Append(values, vectorSize);
    auto maybe_buffer = builder.Finish();
    std::shared_ptr<arrow::Buffer> dataBuffer = *maybe_buffer;

    std::vector<std::shared_ptr<Buffer>> buffers;

    buffers.emplace_back(bitmapBuffer);
    buffers.emplace_back(dataBuffer);

    auto int64Type = std::make_shared<Int64Type>();
    auto arrayData = arrow::ArrayData::Make(int64Type, vectorSize, buffers);

    std::vector<std::shared_ptr<arrow::Array>> arrayVector;
    auto numericArray = std::make_shared<NumericArray<Int64Type>>(arrayData);
    arrayVector.emplace_back(numericArray);

    return arrow::ChunkedArray::Make(arrayVector, int64Type).ValueOrDie;
    }

std::shared_ptr<arrow::ChunkedArray> buildDate32Chunk(DataTypeId typeId, BaseVector *baseVector,
                                                       bool isSplitWrite = false, long startPos = 0,
                                                       long endPos = 0) {
    using T = typename NativeType<OMNI_DATE32>::type;
    auto vector = (Vector<T> *) baseVector;

    if (!isSplitWrite) {
        startPos = 0;
        endPos = vector->GetSize();
        }

    long vectorSize = endPos - startPos;
    int values[vectorSize];
    long index=0;

    auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
    arrow::internal::Bitmap bitmap(bitmapBuffer,0,vectorSize);
    bitmap.SetBitsTo(true);



    for (long j =startPos; j< endPos; j++) {
        values[index]=vector->GetValue(j);
        if (vector->IsNull(j)) {
            bitmap.SetBitTo(index,false);
        }
        index++;
    }

    TypedBufferBuilder<int32_t> builder;
    builder.Resize(vectorSize);

    builder.Append(values, vectorSize);
    auto maybe_buffer = builder.Finish();
    std::shared_ptr<arrow::Buffer> dataBuffer = *maybe_buffer;

    std::vector<std::shared_ptr<Buffer>> buffers;

    buffers.emplace_back(bitmapBuffer);
    buffers.emplace_back(dataBuffer);

    auto date32Type = std::make_shared<Date32Type>();
    auto arrayData = arrow::ArrayData::Make(date32Type, vectorSize, buffers);

    //todo Array or arrow::Array
    std::vector<std::shared_ptr<arrow::Array>> arrayVector;
    auto date32Array = std::make_shared<NumericArray<Date32Type>>(arrayData);
    arrayVector.emplace_back(date32Array);

    return arrow::ChunkedArray::Make(arrayVector, int16Type).ValueOrDie;
    }


std::shared_ptr<arrow::ChunkedArray> buildDate64Chunk(DataTypeId typeId, BaseVector *baseVector,
                                                       bool isSplitWrite = false, long startPos = 0,
                                                       long endPos = 0) {
    using T = typename NativeType<OMNI_DATE64>::type;
    auto vector = (Vector<T> *) baseVector;

    if (!isSplitWrite) {
        startPos = 0;
        endPos = vector->GetSize();
        }

    long vectorSize = endPos - startPos;
    int64_t values[vectorSize];
    long index=0;

    auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
    arrow::internal::Bitmap bitmap(bitmapBuffer,0,vectorSize);
    bitmap.SetBitsTo(true);


    for (long j =startPos; j< endPos; j++) {
        values[index]=vector->GetValue(j);
        if (vector->IsNull(j)) {
            bitmap.SetBitTo(index,false);
        }
        index++;
    }

    TypedBufferBuilder<int64_t> builder;
    builder.Resize(vectorSize);
    builder.Append(values, vectorSize);
    auto maybe_buffer = builder.Finish();
    std::shared_ptr<arrow::Buffer> dataBuffer = *maybe_buffer;

    std::vector<std::shared_ptr<Buffer>> buffers;
    buffers.emplace_back(bitmapBuffer);
    buffers.emplace_back(dataBuffer);

    auto date64Type = std::make_shared<arrow::Date64Type>();
    auto arrayData = arrow::ArrayData::Make(date64Type, vectorSize, buffers);

    std::vector<std::shared_ptr<arrow::Array>> arrayVector;
    auto date64Array = std::make_shared<NumericArray<arrow::Date64Type>>(arrayData);
    arrayVector.emplace_back(date64Array);

    return arrow::ChunkedArray::Make(arrayVector, date64Type).ValueOrDie;
    }

std::shared_ptr<arrow::ChunkedArray> buildDoubleChunk(DataTypeId typeId, BaseVector *baseVector,
                                                       bool isSplitWrite = false, long startPos = 0,
                                                       long endPos = 0) {
    using T = typename NativeType<OMNI_DOUBLE>::type;
    auto vector = (Vector<T> *) baseVector;

    if (!isSplitWrite) {
        startPos = 0;
        endPos = vector->GetSize();
        }

    long vectorSize = endPos - startPos;
    double values[vectorSize];
    long index=0;

    auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
    arrow::internal::Bitmap bitmap(bitmapBuffer,0,vectorSize);
    bitmap.SetBitsTo(true);


    for (long j =startPos; j< endPos; j++) {
        values[index]=vector->GetValue(j);
        if (vector->IsNull(j)) {
            bitmap.SetBitTo(index,false);
        }
        index++;
    }

    TypedBufferBuilder<double> builder;
    builder.Resize(vectorSize);

    builder.Append(values, vectorSize);
    auto maybe_buffer = builder.Finish();
    std::shared_ptr<arrow::Buffer> dataBuffer = *maybe_buffer;

    std::vector<std::shared_ptr<Buffer>> buffers;
    buffers.emplace_back(bitmapBuffer);
    buffers.emplace_back(dataBuffer);

    auto doubleType = std::make_shared<arrow::DoubleType>();
    auto arrayData = arrow::ArrayData::Make(doubleType, vectorSize, buffers);

    std::vector<std::shared_ptr<arrow::Array>> arrayVector;
    auto doubleArray = std::make_shared<NumericArray<arrow::DoubleType>>(arrayData);
    arrayVector.emplace_back(doubleArray);

    return arrow::ChunkedArray::Make(arrayVector, doubleType).ValueOrDie;
    }

std::shared_ptr<arrow::ChunkedArray> buildVarcharChunk(DataTypeId typeId, BaseVector *baseVector,
                                                       bool isSplitWrite = false, long startPos = 0,
                                                       long endPos = 0) {
    auto vector = dynamic_cast<Vector<LargeStringContainer<std::string_view>> *>(baseVector);

    if (!isSplitWrite) {
        startPos = 0;
        endPos = vector->GetSize();
        }

    long vectorSize = endPos - startPos;
    auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
    arrow::internal::Bitmap bitmap(bitmapBuffer,0,vectorSize);
    bitmap.SetBitsTo(true);


    TypedBufferBuilder<int32_t> offsetsBuilder;
    TypedBufferBuilder<char> valuesBuilder;
    int32_t currentOffset = 0;
    offsetsBuilder.Append(0);
    valuesBuilder.Resize(vectorSize);

    long index=0;
    for (long j =startPos; j< endPos; j++) {
        if (vector->IsNull(j)) {
            bitmap.SetBitTo(index,false);
        }
        index++;
        std::string strValue = std::string(vector->GetValue(j));
        const char* cStr = strValue.c_str();
        size_t length=strValue.length();
        char* charArray = new char[length+1];
        strcpy(charArray, cStr);
        current_offset += length;
        offsetsBuilder.Append(current_offset);
        valuesBuilder.Append(charArray,length);
        delete [] charArray;
    }

    auto offsetsBuffer = offsetsBuilder.Finish().ValueOrDie();
    auto valuesBuffer = valuesBuilder.Finish().ValueOrDie();

    std::vector<std::shared_ptr<Buffer>> buffers;

    buffers.emplace_back(bitmapBuffer);
    buffers.emplace_back(offsetsBuffer);
    buffers.emplace_back(valuesBuffer);

    auto utf8Type = std::make_shared<arrow::StringType>();
    auto arrayData = arrow::ArrayData::Make(utf8Type, vectorSize, buffers);

    std::vector<std::shared_ptr<arrow::Array>> arrayVector;
    auto stringArray = std::make_shared<StringArray>(arrayData);
    arrayVector.emplace_back(stringArray);

    return arrow::ChunkedArray::Make(arrayVector, utf8Type).ValueOrDie;
    }

//arrow parquet don't have Decimal64 Type ,use Decimal128 instead of it
std::shared_ptr<arrow::ChunkedArray> buildDecimal64Chunk(DataTypeId typeId, BaseVector *baseVector, int precision, int scale,
                                                       bool isSplitWrite = false, long startPos = 0,
                                                       long endPos = 0) {
    using T = typename NativeType<OMNI_DECIMAL64>::type;
    auto vector = (Vector<T> *) baseVector;

    if (!isSplitWrite) {
        startPos = 0;
        endPos = vector->GetSize();
        }

    long vectorSize = endPos - startPos;
    auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
    arrow::internal::Bitmap bitmap(bitmapBuffer,0,vectorSize);
    bitmap.SetBitsTo(true);

    BufferBuilder builder;
    builder.Resize(vectorSize);
    std::vector<arrow::Decimal128> decimalArray;

    long index=0;
    for (long j =startPos; j< endPos; j++) {

        BasicDecimal128 basicDecimal128(0,vector->GetValue(j));
        decimalArray.emplace_back(BasicDecimal128(basicDecimal128));
        if (vector->IsNull(j)) {
            bitmap.SetBitTo(index,false);
        }
        index++;
    }

    builder.Append(decimalArray.data(), decimalArray.size()*16);
    auto maybe_buffer = builder.Finish();
    std::shared_ptr<arrow::Buffer> dataBuffer = *maybe_buffer;

    std::vector<std::shared_ptr<Buffer>> buffers;
    buffers.emplace_back(bitmapBuffer);
    buffers.emplace_back(dataBuffer);

    auto decimal128Type = std::make_shared<arrow::Decimal128Type>(precision,scale);
    auto arrayData = arrow::ArrayData::Make(Decimal128Type, vectorSize, buffers);

    std::vector<std::shared_ptr<arrow::Array>> arrayVector;
    auto decimal128Array = std::make_shared<Decimal128Array>(arrayData);
    arrayVector.emplace_back(decimal128Array);

    return arrow::ChunkedArray::Make(arrayVector, decimal128Type).ValueOrDie;
    }


std::shared_ptr<arrow::ChunkedArray> buildDecimal128Chunk(DataTypeId typeId, BaseVector *baseVector, int precision, int scale,
                                                       bool isSplitWrite = false, long startPos = 0,
                                                       long endPos = 0) {
    using T = typename NativeType<OMNI_DECIMAL128>::type;
    auto vector = (Vector<T> *) baseVector;

    if (!isSplitWrite) {
        startPos = 0;
        endPos = vector->GetSize();
        }

    long vectorSize = endPos - startPos;
    auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
    arrow::internal::Bitmap bitmap(bitmapBuffer,0,vectorSize);
    bitmap.SetBitsTo(true);

    BufferBuilder builder;
    builder.Resize(vectorSize);
    std::vector<arrow::Decimal128> decimalArray;

    long index=0;
    for (long j =startPos; j< endPos; j++) {
        auto decimalValue=vector->GetValue(j)
        BasicDecimal128 basicDecimal128(vector->GetValue(j).HighBits(),vector->GetValue(j).LowBits());
        decimalArray.emplace_back(BasicDecimal128(basicDecimal128));

        decimalArray.back().Rescale(2,2);
        if (vector->IsNull(j)) {
            bitmap.SetBitTo(index,false);
        }
        index++;
    }

    builder.Append(decimalArray.data(), decimalArray.size()*16);
    auto maybe_buffer = builder.Finish();
    std::shared_ptr<arrow::Buffer> dataBuffer = *maybe_buffer;

    std::vector<std::shared_ptr<Buffer>> buffers;
    buffers.emplace_back(bitmapBuffer);
    buffers.emplace_back(dataBuffer);

    auto decimal128Type = std::make_shared<arrow::Decimal128Type>(precision,scale);
    auto arrayData = arrow::ArrayData::Make(Decimal128Type, vectorSize, buffers);

    std::vector<std::shared_ptr<arrow::Array>> arrayVector;
    auto decimal128Array = std::make_shared<Decimal128Array>(arrayData);
    arrayVector.emplace_back(decimal128Array);

    return arrow::ChunkedArray::Make(arrayVector, decimal128Type).ValueOrDie;
    }

void ParquetWrier::write(long * vecNativeId, Int colNums,
                         const int *omniTypes,
                         const unsigned char *dataColumnsIds,
                         bool isSplitWrite, long startPos, long endPos) {
    std::vector<std::shared_ptr<::arrow::ChunkedArray>> chunks;
    for (int i = 0; i < colNums; ++i) {
       if (!dataColumsIds[i]) {
           continue;
       }

    auto vec = (BaseVector *)vecNativeId[i];
    auto typeId = static_cast<DataTypeId>(omniTypes[i]);
    switch (typeId) {
        case OMNI_BOOLEAN:
            chunks.emplace_back(buildBooleanChunk(typeId, vec, isSplitWrite, startPos, endPos));
            break;
        case OMNI_SHORT:
            chunks.emplace_back(buildInt16Chunk(typeId, vec, isSplitWrite, startPos, endPos));
            break;
        case OMNI_INT:
            chunks.emplace_back(buildInt32Chunk(typeId, vec, isSplitWrite, startPos, endPos));
            break;
        case OMNI_LONG:
            chunks.emplace_back(buildInt64Chunk(typeId, vec, isSplitWrite, startPos, endPos));
            break;
        case OMNI_DATE32:
            chunks.emplace_back(buildDATE32Chunk(typeId, vec, isSplitWrite, startPos, endPos));
            break;
        case OMNI_DATE64:
            chunks.emplace_back(buildDATE64Chunk(typeId, vec, isSplitWrite, startPos, endPos));
            break;
        case OMNI_DOUBLE:
            chunks.emplace_back(buildDOUBLEChunk(typeId, vec, isSplitWrite, startPos, endPos));
            break;
        case OMNI_VARCHAR:
            chunks.emplace_back(buildVARCHARChunk(typeId, vec, isSplitWrite, startPos, endPos));
            break;
        case OMNI_DECIMAL64:
            chunks.emplace_back(buildDECIMAL64Chunk(typeId, vec, precision, scale, isSplitWrite, startPos, endPos));
            break;
        case OMNI_DECIMAL128:
            chunks.emplace_back(buildDECIMAL128Chunk(typeId, vec, precision, scale, isSplitWrite, startPos, endPos));
            break;
        default:
            throw std::runtime_error(
                "Native columnar write not support for this type: " + typedId)

        }
    }
     auto numRows = (chunks,empty() ? 0 : chunks[0]->length());

     auto table = arrow::Table::Make(schema_, std::move(chunks), numRows);
     PARQUET_THROW_NOT_OK(arrow_writer->WriterTable(*table));
     PARQUET_THROW_NOT_OK(arrow_writer->Close());
}

}//namespace omniruntime::writer
