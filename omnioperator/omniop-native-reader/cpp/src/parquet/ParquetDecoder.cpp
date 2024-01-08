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

#include "ParquetDecoder.h"

using namespace parquet::arrow;
using namespace parquet;
using namespace omniruntime::vec;

namespace omniruntime::reader {

    ParquetPlainBooleanDecoder::ParquetPlainBooleanDecoder(const ::parquet::ColumnDescriptor* descr)
        : ParquetDecoderImpl(descr, ::parquet::Encoding::PLAIN) {}

    void ParquetPlainBooleanDecoder::SetData(int num_values, const uint8_t* data, int len) {
        num_values_ = num_values;
        bit_reader_ = std::make_unique<::arrow::bit_util::BitReader>(data, len);
    }

    int ParquetPlainBooleanDecoder::Decode(uint8_t* buffer, int max_values) {
        max_values = std::min(max_values, num_values_);
        bool val;
        ::arrow::internal::BitmapWriter bit_writer(buffer, 0, max_values);
        for (int i = 0; i < max_values; ++i) {
            if (!bit_reader_->GetValue(1, &val)) {
                ParquetException::EofException();
            }
            if (val) {
                bit_writer.Set();
            }
            bit_writer.Next();
        }
        bit_writer.Finish();
        num_values_ -= max_values;
        return max_values;
    }

    int ParquetPlainBooleanDecoder::Decode(bool* buffer, int max_values) {
        max_values = std::min(max_values, num_values_);
        if (bit_reader_->GetBatch(1, buffer, max_values) != max_values) {
            ::parquet::ParquetException::EofException();
        }
        num_values_ -= max_values;
        return max_values;
    }

    template <>
    void ParquetDictDecoderImpl<::parquet::BooleanType>::SetDict(ParquetTypedDecoder<::parquet::BooleanType>* dictionary) {
        ParquetException::NYI("Dictionary encoding is not implemented for boolean values");
    }

    template <>
    void ParquetDictDecoderImpl<ByteArrayType>::SetDict(ParquetTypedDecoder<ByteArrayType>* dictionary) {
        DecodeDict(dictionary);

        auto dict_values = reinterpret_cast<ByteArray*>(dictionary_->mutable_data());

        int total_size = 0;
        for (int i = 0; i < dictionary_length_; ++i) {
            total_size += dict_values[i].len;
        }
        PARQUET_THROW_NOT_OK(byte_array_data_->Resize(total_size,
            /*shrink_to_fit=*/false));
        PARQUET_THROW_NOT_OK(
            byte_array_offsets_->Resize((dictionary_length_ + 1) * sizeof(int32_t),
                /*shrink_to_fit=*/false));

        int32_t offset = 0;
        uint8_t* bytes_data = byte_array_data_->mutable_data();
        int32_t* bytes_offsets =
              reinterpret_cast<int32_t*>(byte_array_offsets_->mutable_data());
        for (int i = 0; i < dictionary_length_; ++i) {
            memcpy(bytes_data + offset, dict_values[i].ptr, dict_values[i].len);
            bytes_offsets[i] = offset;
            dict_values[i].ptr = bytes_data + offset;
            offset += dict_values[i].len;
        }
        bytes_offsets[dictionary_length_] = offset;
    }

    template <>
    inline void ParquetDictDecoderImpl<FLBAType>::SetDict(ParquetTypedDecoder<FLBAType>* dictionary) {
        DecodeDict(dictionary);

        auto dict_values = reinterpret_cast<FLBA*>(dictionary_->mutable_data());

        int fixed_len = descr_->type_length();
        int total_size = dictionary_length_ * fixed_len;

        PARQUET_THROW_NOT_OK(byte_array_data_->Resize(total_size,
              /*shrink_to_fit=*/false));
        uint8_t* bytes_data = byte_array_data_->mutable_data();
        for (int32_t i = 0, offset = 0; i < dictionary_length_; ++i, offset += fixed_len) {
            memcpy(bytes_data + offset, dict_values[i].ptr, fixed_len);
            dict_values[i].ptr = bytes_data + offset;
        }
    }
}