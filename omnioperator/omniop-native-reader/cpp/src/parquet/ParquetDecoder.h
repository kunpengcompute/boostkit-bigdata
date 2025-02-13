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

#ifndef OMNI_RUNTIME_ENCODING_H
#define OMNI_RUNTIME_ENCODING_H

#include <vector/vector_common.h>
#include <parquet/arrow/reader.h>
#include <arrow/util/bitmap_writer.h>
#include <arrow/util/rle_encoding.h>
#include <arrow/status.h>
#include <parquet/encoding.h>

using namespace omniruntime::vec;
using namespace arrow;

namespace omniruntime::reader {

    class ParquetDecoderImpl : virtual public ::parquet::Decoder {
    public:
        void SetData(int num_values, const uint8_t* data, int len) override {
            num_values_ = num_values;
            data_ = data;
            len_ = len;
        }

        int values_left() const override { return num_values_; }
        ::parquet::Encoding::type encoding() const override { return encoding_; }

    protected:
        explicit ParquetDecoderImpl(const ::parquet::ColumnDescriptor* descr, ::parquet::Encoding::type encoding)
                : descr_(descr), encoding_(encoding), num_values_(0), data_(NULLPTR), len_(0) {}

        // For accessing type-specific metadata, like FIXED_LEN_BYTE_ARRAY
        const ::parquet::ColumnDescriptor* descr_;

        const ::parquet::Encoding::type encoding_;
        int num_values_;
        const uint8_t* data_;
        int len_;
        int type_length_;
    };

    // TODO: optimize batch move
    template <typename T>
    inline int SpacedExpand(T* buffer, int num_values, int null_count, uint8_t* nulls, int64_t nullsOffset) {
        int idx_decode = num_values - null_count;
        memset_s(static_cast<void*>(buffer + idx_decode), null_count * sizeof(T), 0, null_count * sizeof(T));
        if (idx_decode == 0) {
            // All nulls, nothing more to do
            return num_values;
        }
        for (int i = num_values - 1; i >= 0; --i) {
            if (!BitUtil::IsBitSet(nulls, nullsOffset + i)) {
                idx_decode--;
                memmove_s(buffer + i, sizeof(T), buffer + idx_decode, sizeof(T));
            }
        }
        assert(idx_decode == 0);
        return num_values;
    }

    template <typename DType>
    class ParquetTypedDecoder : virtual public ::parquet::TypedDecoder<DType> {
    public:
        using T = typename DType::c_type;

        virtual int DecodeSpaced(T* buffer, int num_values, int null_count, uint8_t* nulls, int64_t nullsOffset) {
            if (null_count > 0) {
                int values_to_read = num_values - null_count;
                int values_read = Decode(buffer, values_to_read);
                if (values_read != values_to_read) {
                    throw ::parquet::ParquetException("Number of values / definition_levels read did not match");
                }

                return SpacedExpand<T>(buffer, num_values, null_count, nulls, nullsOffset);
            } else {
                return Decode(buffer, num_values);
            }
        }

        int Decode(T* buffer, int num_values) override {
            ::parquet::ParquetException::NYI("ParquetTypedDecoder for Decode");
        }

        virtual int DecodeArrowNonNull(int num_values, omniruntime::vec::BaseVector** outBaseVec, int64_t offset) {
            ::parquet::ParquetException::NYI("ParquetTypedDecoder for DecodeArrowNonNull");
        }

        virtual int DecodeArrow(int num_values, int null_count, uint8_t* nulls,
                        int64_t offset, omniruntime::vec::BaseVector** outBaseVec) {
            ::parquet::ParquetException::NYI("ParquetTypedDecoder for DecodeArrow");
        }
     };

    template <typename DType>
    class ParquetDictDecoder : virtual public ParquetTypedDecoder<DType> {
    public:
        using T = typename DType::c_type;

        virtual void SetDict(ParquetTypedDecoder<DType>* dictionary) = 0;

        virtual void InsertDictionary(::arrow::ArrayBuilder* builder) = 0;

        virtual int DecodeIndicesSpaced(int num_values, int null_count,
                                        const uint8_t* valid_bits, int64_t valid_bits_offset,
                                        ::arrow::ArrayBuilder* builder) = 0;

        virtual int DecodeIndices(int num_values, ::arrow::ArrayBuilder* builder) = 0;

        virtual int DecodeIndices(int num_values, int32_t* indices) = 0;

        virtual void GetDictionary(const T** dictionary, int32_t* dictionary_length) = 0;
    };

    template <typename Type>
    class ParquetDictDecoderImpl : public ParquetDecoderImpl, virtual public ParquetDictDecoder<Type> {
    public:
        typedef typename Type::c_type T;

        explicit ParquetDictDecoderImpl(const ::parquet::ColumnDescriptor* descr,
                                        ::arrow::MemoryPool* pool = ::arrow::default_memory_pool())
                : ParquetDecoderImpl(descr, ::parquet::Encoding::RLE_DICTIONARY),
                  dictionary_(::parquet::AllocateBuffer(pool, 0)),
                  dictionary_length_(0),
                  byte_array_data_(::parquet::AllocateBuffer(pool, 0)),
                  byte_array_offsets_(::parquet::AllocateBuffer(pool, 0)) {}

        void SetDict(ParquetTypedDecoder<Type>* dictionary) override;

        void SetData(int num_values, const uint8_t* data, int len) override {
            num_values_ = num_values;
            if (len == 0) {
                idx_decoder_ = ::arrow::util::RleDecoder(data, len, 1);
                return;
            }
            uint8_t bit_width = *data;
            if (ARROW_PREDICT_FALSE(bit_width > 32)) {
                throw ::parquet::ParquetException("Invalid or corrupted bit_width " +
                                       std::to_string(bit_width) + ". Maximum allowed is 32.");
            }
            idx_decoder_ = ::arrow::util::RleDecoder(++data, --len, bit_width);
        }

        int Decode(T* buffer, int num_values) override {
            num_values = std::min(num_values, num_values_);
            int decoded_values =
                    idx_decoder_.GetBatchWithDict(reinterpret_cast<const T*>(dictionary_->data()),
                                                  dictionary_length_, buffer, num_values);
            if (decoded_values != num_values) {
                ::parquet::ParquetException::EofException();
            }
            num_values_ -= num_values;
            return num_values;
        }

        int DecodeSpaced(T* buffer, int num_values, int null_count, const uint8_t* valid_bits,
                int64_t valid_bits_offset) override {
            num_values = std::min(num_values, num_values_);
            if (num_values != idx_decoder_.GetBatchWithDictSpaced(
                    reinterpret_cast<const T*>(dictionary_->data()),
                    dictionary_length_, buffer, num_values, null_count, valid_bits,
                    valid_bits_offset)) {
                ::parquet::ParquetException::EofException();
            }
            num_values_ -= num_values;
            return num_values;
        }

        int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                        int64_t valid_bits_offset,
                        typename ::parquet::EncodingTraits<Type>::Accumulator* out) override {
            ::parquet::ParquetException::NYI("DecodeArrow(Accumulator) for OmniDictDecoderImpl");
        }

        int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                        int64_t valid_bits_offset,
                        typename ::parquet::EncodingTraits<Type>::DictAccumulator* out) override {
            ::parquet::ParquetException::NYI("DecodeArrow(DictAccumulator) for OmniDictDecoderImpl");
        }

        void InsertDictionary(::arrow::ArrayBuilder* builder) override {
            ::parquet::ParquetException::NYI("InsertDictionary ArrayBuilder");
        }

        int DecodeIndicesSpaced(int num_values, int null_count, const uint8_t* valid_bits,
                                int64_t valid_bits_offset,
                                ::arrow::ArrayBuilder* builder) override {
            ::parquet::ParquetException::NYI("DecodeIndicesSpaced ArrayBuilder");
        }

        int DecodeIndices(int num_values, ::arrow::ArrayBuilder* builder) override {
            ::parquet::ParquetException::NYI("DecodeIndices ArrayBuilder");
        }

        int DecodeIndices(int num_values, int32_t* indices) override {
            if (num_values != idx_decoder_.GetBatch(indices, num_values)) {
                ::parquet::ParquetException::EofException();
            }
            num_values_ -= num_values;
            return num_values;
        }

        void GetDictionary(const T** dictionary, int32_t* dictionary_length) override {
            *dictionary_length = dictionary_length_;
            *dictionary = reinterpret_cast<T*>(dictionary_->mutable_data());
        }

        virtual int DecodeArrowNonNull(int num_values, omniruntime::vec::BaseVector** outBaseVec, int64_t offset) {
            ::parquet::ParquetException::NYI("ParquetTypedDecoder for DecodeArrowNonNull");
        }

        virtual int DecodeArrow(int num_values, int null_count, uint8_t* nulls,
                                int64_t offset, omniruntime::vec::BaseVector** outBaseVec) {
            ::parquet::ParquetException::NYI("ParquetTypedDecoder for DecodeArrow");
        }

    protected:
        arrow::Status IndexInBounds(int32_t index) {
            if (ARROW_PREDICT_TRUE(0 <= index && index < dictionary_length_)) {
                return arrow::Status::OK();
            }
            return arrow::Status::Invalid("Index not in dictionary bounds");
        }

        inline void DecodeDict(::parquet::TypedDecoder<Type>* dictionary) {
            dictionary_length_ = static_cast<int32_t>(dictionary->values_left());
            PARQUET_THROW_NOT_OK(dictionary_->Resize(dictionary_length_ * sizeof(T),
                    /*shrink_to_fit=*/false));
            dictionary->Decode(reinterpret_cast<T*>(dictionary_->mutable_data()), dictionary_length_);
        }

        std::shared_ptr<::parquet::ResizableBuffer> dictionary_;

        int32_t dictionary_length_;

        std::shared_ptr<::parquet::ResizableBuffer> byte_array_data_;

        std::shared_ptr<::parquet::ResizableBuffer> byte_array_offsets_;

        ::arrow::util::RleDecoder idx_decoder_;
    };

    template <typename Type>
    void ParquetDictDecoderImpl<Type>::SetDict(ParquetTypedDecoder<Type>* dictionary) {
         DecodeDict(dictionary);
    }

    class OmniDictByteArrayDecoderImpl : public ParquetDictDecoderImpl<::parquet::ByteArrayType> {
    public:
        using BASE = ParquetDictDecoderImpl<::parquet::ByteArrayType>;
        using BASE::ParquetDictDecoderImpl;

        int DecodeArrowNonNull(int num_values, omniruntime::vec::BaseVector** outBaseVec, int64_t offset) override {
            int result = 0;
            PARQUET_THROW_NOT_OK(DecodeArrowNonNull(num_values, &result, outBaseVec, offset));
            return result;
        }

        int DecodeArrow(int num_values, int null_count, uint8_t* nulls,
                        int64_t offset, omniruntime::vec::BaseVector** vec) override {
            int result = 0;
            PARQUET_THROW_NOT_OK(DecodeArrowDense(num_values, null_count, nulls,
                                                      offset, &result, vec));
            return result;
        }

    private:
        arrow::Status DecodeArrowDense(int num_values, int null_count, uint8_t* nulls,
                                int64_t offset,
                                int* out_num_values, omniruntime::vec::BaseVector** out) {
            constexpr int32_t kBufferSize = 1024;
            int32_t indices[kBufferSize];

            auto vec = dynamic_cast<omniruntime::vec::Vector<
                    omniruntime::vec::LargeStringContainer<std::string_view>>*>(*out);

            auto dict_values = reinterpret_cast<const ::parquet::ByteArray*>(dictionary_->data());
            int values_decoded = 0;
            int num_indices = 0;
            int pos_indices = 0;

            for (int i = 0; i < num_values; i++) {
                if (!BitUtil::IsBitSet(nulls, offset + i)) {
                    if (num_indices == pos_indices) {
                        const auto batch_size =
                                std::min<int32_t>(kBufferSize, num_values - null_count - values_decoded);
                        num_indices = idx_decoder_.GetBatch(indices, batch_size);
                        if (ARROW_PREDICT_FALSE(num_indices < 1)) {
                            return arrow::Status::Invalid("Invalid number of indices: ", num_indices);
                        }
                        pos_indices = 0;
                    }
                    const auto index = indices[pos_indices++];
                    RETURN_NOT_OK(IndexInBounds(index));
                    const auto& val = dict_values[index];
                    std::string_view value(reinterpret_cast<const char *>(val.ptr), val.len);
                    vec->SetValue(offset + i, value);
                    ++values_decoded;
                } else {
                    vec->SetNull(offset + i);
                }
            }

            *out_num_values = values_decoded;
            return arrow::Status::OK();
        }

        arrow::Status DecodeArrowNonNull(int num_values, int* out_num_values, omniruntime::vec::BaseVector** out, int offset) {
            constexpr int32_t kBufferSize = 2048;
            int32_t indices[kBufferSize];

            auto vec = dynamic_cast<omniruntime::vec::Vector<
                    omniruntime::vec::LargeStringContainer<std::string_view>>*>(*out);

            auto dict_values = reinterpret_cast<const ::parquet::ByteArray*>(dictionary_->data());

            int values_decoded = 0;
            while (values_decoded < num_values) {
                int32_t batch_size = std::min<int32_t>(kBufferSize, num_values - values_decoded);
                int num_indices = idx_decoder_.GetBatch(indices, batch_size);
                if (num_indices == 0) ::parquet::ParquetException::EofException();
                for (int i = 0; i < num_indices; ++i) {
                    auto idx = indices[i];
                    RETURN_NOT_OK(IndexInBounds(idx));
                    const auto& val = dict_values[idx];
                    std::string_view value(reinterpret_cast<const char *>(val.ptr), val.len);
                    vec->SetValue(i + offset, value);
                }
                values_decoded += num_indices;
                offset += num_indices;
            }
            *out_num_values = values_decoded;
            return arrow::Status::OK();
        }
    };

    template <typename DType>
    class ParquetPlainDecoder : public ParquetDecoderImpl, virtual public ParquetTypedDecoder<DType> {
    public:
        using T = typename DType::c_type;
        explicit ParquetPlainDecoder(const ::parquet::ColumnDescriptor* descr);

        int Decode(T* buffer, int max_values) override;

        int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                        int64_t valid_bits_offset,
                        typename ::parquet::EncodingTraits<DType>::Accumulator* builder) override {
            ::parquet::ParquetException::NYI("DecodeArrow(Accumulator) for ParquetPlainDecoder");
        }

        int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                        int64_t valid_bits_offset,
                        typename ::parquet::EncodingTraits<DType>::DictAccumulator* builder) override {
            ::parquet::ParquetException::NYI("DecodeArrow(DictAccumulator) for ParquetPlainDecoder");
        }
    };

    template <typename T>
    inline int DecodePlain(const uint8_t* data, int64_t data_size, int num_values,
                            int type_length, T* out) {
        int64_t bytes_to_decode = num_values * static_cast<int64_t>(sizeof(T));
        if (bytes_to_decode > data_size || bytes_to_decode > INT_MAX) {
            ::parquet::ParquetException::EofException();
        }
        if (bytes_to_decode > 0) {
            memcpy_s(out, data_size, data, bytes_to_decode);
        }
        return static_cast<int>(bytes_to_decode);
    }

    static inline int64_t ReadByteArray(const uint8_t* data, int64_t data_size,
                                        ::parquet::ByteArray* out) {
        if (ARROW_PREDICT_FALSE(data_size < 4)) {
            parquet::ParquetException::EofException();
        }
        const int32_t len = ::arrow::util::SafeLoadAs<int32_t>(data);
        if (len < 0) {
            throw parquet::ParquetException("Invalid BYTE_ARRAY value");
        }
        const int64_t consumed_length = static_cast<int64_t>(len) + 4;
        if (ARROW_PREDICT_FALSE(data_size < consumed_length)) {
            parquet::ParquetException::EofException();
        }
        *out = parquet::ByteArray{static_cast<uint32_t>(len), data + 4};
        return consumed_length;
    }

    template <>
    inline int DecodePlain<::parquet::ByteArray>(const uint8_t* data, int64_t data_size, int num_values,
                                       int type_length, ::parquet::ByteArray* out) {
        int bytes_decoded = 0;
        for (int i = 0; i < num_values; ++i) {
            const auto increment = ReadByteArray(data, data_size, out + i);
            if (ARROW_PREDICT_FALSE(increment > INT_MAX - bytes_decoded)) {
                throw ::parquet::ParquetException("BYTE_ARRAY chunk too large");
            }
            data += increment;
            data_size -= increment;
            bytes_decoded += static_cast<int>(increment);
        }
        return bytes_decoded;
    }

    template <>
    inline int DecodePlain<::parquet::FixedLenByteArray>(const uint8_t* data, int64_t data_size,
                                               int num_values, int type_length,
                                                          ::parquet::FixedLenByteArray* out) {
        int64_t bytes_to_decode = static_cast<int64_t>(type_length) * num_values;
        if (bytes_to_decode > data_size || bytes_to_decode > INT_MAX) {
            ::parquet::ParquetException::EofException();
        }

        memcpy_s(reinterpret_cast<uint8_t*>(out), bytes_to_decode, data, bytes_to_decode);

        return static_cast<int>(bytes_to_decode);
    }

    template <typename DType>
    ParquetPlainDecoder<DType>::ParquetPlainDecoder(const ::parquet::ColumnDescriptor* descr)
              : ParquetDecoderImpl(descr, ::parquet::Encoding::PLAIN) {
        if (descr_ && descr_->physical_type() == ::parquet::Type::FIXED_LEN_BYTE_ARRAY) {
            type_length_ = descr_->type_length();
        } else {
            type_length_ = -1;
        }
    }

    template <typename DType>
    int ParquetPlainDecoder<DType>::Decode(T* buffer, int max_values) {
        max_values = std::min(max_values, num_values_);
        int bytes_consumed = DecodePlain<T>(data_, len_, max_values, type_length_, buffer);
        data_ += bytes_consumed;
        len_ -= bytes_consumed;
        num_values_ -= max_values;
        return max_values;
    }

    class ParquetPlainByteArrayDecoder : public ParquetPlainDecoder<::parquet::ByteArrayType> {
    public:
        using Base = ParquetPlainDecoder<::parquet::ByteArrayType>;
        using Base::ParquetPlainDecoder;

        int DecodeArrowNonNull(int num_values, omniruntime::vec::BaseVector** outBaseVec, int64_t offset) override {
            int result = 0;
            PARQUET_THROW_NOT_OK(DecodeArrowDenseNonNull(num_values, &result, outBaseVec, offset));
            return result;
        }

        int DecodeArrow(int num_values, int null_count, uint8_t* nulls,
                        int64_t offset, omniruntime::vec::BaseVector** outBaseVec) {
            int result = 0;
            PARQUET_THROW_NOT_OK(DecodeArrowDense(num_values, null_count, nulls,
                                                  offset, &result, outBaseVec));
            return result;
        }

    private:
        arrow::Status DecodeArrowDense(int num_values, int null_count, uint8_t* nulls,
                                int64_t offset,
                                int* out_values_decoded, omniruntime::vec::BaseVector** out) {
            int values_decoded = 0;
            auto vec = dynamic_cast<omniruntime::vec::Vector<
                    omniruntime::vec::LargeStringContainer<std::string_view>>*>(*out);

            for (int i = 0; i < num_values; i++) {
                if (!BitUtil::IsBitSet(nulls, offset + i)) {
                    if (ARROW_PREDICT_FALSE(len_ < 4)) {
                        ::parquet::ParquetException::EofException();
                    }
                    auto value_len = ::arrow::util::SafeLoadAs<int32_t>(data_);
                    if (ARROW_PREDICT_FALSE(value_len < 0 || value_len > INT32_MAX - 4)) {
                        return arrow::Status::Invalid("Invalid or corrupted value_len '", value_len, "'");
                    }
                    auto increment = value_len + 4;
                    if (ARROW_PREDICT_FALSE(len_ < increment)) {
                        ::parquet::ParquetException::EofException();
                    }
                    std::string_view value(reinterpret_cast<const char *>(data_ + 4), value_len);
                    vec->SetValue(offset + i, value);
                    data_ += increment;
                    len_ -= increment;
                    ++values_decoded;
                } else {
                    vec->SetNull(offset + i);
                }
            }

            num_values_ -= values_decoded;
            *out_values_decoded = values_decoded;
            return arrow::Status::OK();
        }

        arrow::Status DecodeArrowDenseNonNull(int num_values,
                                int* out_values_decoded, omniruntime::vec::BaseVector** out, int64_t offset) {
            int values_decoded = 0;
            auto vec = dynamic_cast<omniruntime::vec::Vector<
                    omniruntime::vec::LargeStringContainer<std::string_view>>*>(*out);

            for (int i = 0; i < num_values; i++) {
                if (ARROW_PREDICT_FALSE(len_ < 4)) {
                    ::parquet::ParquetException::EofException();
                }
                auto value_len = ::arrow::util::SafeLoadAs<int32_t>(data_);
                if (ARROW_PREDICT_FALSE(value_len < 0 || value_len > INT32_MAX - 4)) {
                    return arrow::Status::Invalid("Invalid or corrupted value_len '", value_len, "'");
                }
                auto increment = value_len + 4;
                if (ARROW_PREDICT_FALSE(len_ < increment)) {
                    ::parquet::ParquetException::EofException();
                }
                std::string_view value(reinterpret_cast<const char *>(data_ + 4), value_len);
                (vec)->SetValue(offset + i, value);
                data_ += increment;
                len_ -= increment;
                ++values_decoded;
            }
            num_values_ -= values_decoded;
            *out_values_decoded = values_decoded;
            return arrow::Status::OK();
        }
    };

    class ParquetBooleanDecoder : virtual public ParquetTypedDecoder<::parquet::BooleanType> {
    public:
        using ParquetTypedDecoder<::parquet::BooleanType>::Decode;
        virtual int Decode(uint8_t* buffer, int max_values) = 0;
    };

    class ParquetPlainBooleanDecoder : public ParquetDecoderImpl, virtual public ParquetBooleanDecoder {
    public:
        explicit ParquetPlainBooleanDecoder(const ::parquet::ColumnDescriptor* descr);
        void SetData(int num_values, const uint8_t* data, int len) override;

        int Decode(uint8_t* buffer, int max_values) override;
        int Decode(bool* buffer, int max_values) override;

        int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                        int64_t valid_bits_offset,
                        typename ::parquet::EncodingTraits<::parquet::BooleanType>::Accumulator* out) override {
            ::parquet::ParquetException::NYI("DecodeArrow for ParquetPlainBooleanDecoder");
        }

        int DecodeArrow(
                int num_values, int null_count, const uint8_t* valid_bits,
                int64_t valid_bits_offset,
                typename ::parquet::EncodingTraits<::parquet::BooleanType>::DictAccumulator* builder) override {
            ::parquet::ParquetException::NYI("DecodeArrow for ParquetPlainBooleanDecoder");
        }

    private:
        std::unique_ptr<::arrow::bit_util::BitReader> bit_reader_;
    };

    class ParquetRleBooleanDecoder : public ParquetDecoderImpl, virtual public ParquetBooleanDecoder {
    public:
        explicit ParquetRleBooleanDecoder(const ::parquet::ColumnDescriptor* descr)
                : ParquetDecoderImpl(descr, ::parquet::Encoding::RLE) {}

        void SetData(int num_values, const uint8_t* data, int len) override {
            num_values_ = num_values;
            uint32_t num_bytes = 0;

            if (len < 4) {
                throw ::parquet::ParquetException("Received invalid length : " + std::to_string(len) +
                                                  " (corrupt data page?)");
            }

            num_bytes =
                    ::arrow::bit_util::ToLittleEndian(::arrow::util::SafeLoadAs<uint32_t>(data));
            if (num_bytes < 0 || num_bytes > static_cast<uint32_t>(len - 4)) {
                throw ::parquet::ParquetException("Received invalid number of bytes : " +
                                                  std::to_string(num_bytes) + " (corrupt data page?)");
            }

            auto decoder_data = data + 4;
            decoder_ = std::make_shared<::arrow::util::RleDecoder>(decoder_data, num_bytes,
                    /*bit_width=*/1);
        }

        int Decode(bool* buffer, int max_values) override {
            max_values = std::min(max_values, num_values_);

            if (decoder_->GetBatch(buffer, max_values) != max_values) {
                ::parquet::ParquetException::EofException();
            }
            num_values_ -= max_values;
            return max_values;
        }

        int Decode(uint8_t* buffer, int max_values) override {
            ::parquet::ParquetException::NYI("Decode(uint8_t*, int) for RleBooleanDecoder");
        }

        int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                        int64_t valid_bits_offset,
                        typename ::parquet::EncodingTraits<::parquet::BooleanType>::Accumulator* out) override {
            ::parquet::ParquetException::NYI("DecodeArrow for RleBooleanDecoder");
        }

        int DecodeArrow(
                int num_values, int null_count, const uint8_t* valid_bits,
                int64_t valid_bits_offset,
                typename ::parquet::EncodingTraits<::parquet::BooleanType>::DictAccumulator* builder) override {
            ::parquet::ParquetException::NYI("DecodeArrow for RleBooleanDecoder");
        }

    private:
        std::shared_ptr<::arrow::util::RleDecoder> decoder_;
    };

    class ParquetPlainFLBADecoder : public ParquetPlainDecoder<::parquet::FLBAType>, virtual public ::parquet::FLBADecoder {
    public:
        using Base = ParquetPlainDecoder<::parquet::FLBAType>;
        using Base::ParquetPlainDecoder;

        int DecodeSpaced(T* buffer, int num_values, int null_count, uint8_t* nulls, int64_t nullsOffset) override {
            int values_to_read = num_values - null_count;
            Decode(buffer, values_to_read);
            return num_values;
        }

        int Decode(T* buffer, int max_values) override {
            max_values = std::min(max_values, num_values_);
            int bytes_consumed = DecodePlain<T>(data_, len_, max_values, type_length_, buffer);
            data_ += bytes_consumed;
            len_ -= bytes_consumed;
            num_values_ -= max_values;
            return max_values;
        }
    };
}
#endif // OMNI_RUNTIME_ENCODING_H
