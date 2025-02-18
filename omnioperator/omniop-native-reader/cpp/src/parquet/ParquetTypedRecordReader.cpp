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

#include <parquet/arrow/reader_internal.h>
#include "ParquetTypedRecordReader.h"
#include "ParquetDecoder.h"

using namespace parquet::internal;
using namespace arrow;
using namespace parquet;

namespace omniruntime::reader {

constexpr int32_t DECIMAL64_LEN = 8;

::parquet::Decoder* MakeOmniParquetDecoder(::parquet::Type::type type_num, ::parquet::Encoding::type encoding,
    const ColumnDescriptor* descr) {
    if (encoding == ::parquet::Encoding::PLAIN) {
        switch (type_num) {
            case ::parquet::Type::BOOLEAN:
                return new ParquetPlainBooleanDecoder(descr);
            case ::parquet::Type::INT32:
                return new ParquetPlainDecoder<::parquet::Int32Type>(descr);
            case ::parquet::Type::INT64:
                return new ParquetPlainDecoder<::parquet::Int64Type>(descr);
            case ::parquet::Type::INT96:
                return new ParquetPlainDecoder<::parquet::Int96Type>(descr);
            case ::parquet::Type::DOUBLE:
                return new ParquetPlainDecoder<::parquet::DoubleType>(descr);
            case ::parquet::Type::BYTE_ARRAY:
                return new ParquetPlainByteArrayDecoder(descr);
            case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
                return new ParquetPlainFLBADecoder(descr);
            default:
                ::parquet::ParquetException::NYI("Not supported decoder type: " + type_num);
        }
    } else if (encoding == ::parquet::Encoding::RLE) {
        if (type_num == ::parquet::Type::BOOLEAN) {
            return new ParquetRleBooleanDecoder(descr);
        }
        ::parquet::ParquetException::NYI("RLE encoding only supports BOOLEAN");
    } else {
        ::parquet::ParquetException::NYI("Selected encoding is not supported");
    }
    DCHECK(false) << "Should not be able to reach this code";
    return nullptr;
}


::parquet::Decoder* MakeOmniDictDecoder(::parquet::Type::type type_num,
        const ColumnDescriptor* descr, ::arrow::MemoryPool* pool) {
    switch (type_num) {
        case ::parquet::Type::BOOLEAN:
            ::parquet::ParquetException::NYI("Dictionary BOOLEAN encoding not implemented for boolean type");
        case ::parquet::Type::INT32:
            return new ParquetDictDecoderImpl<::parquet::Int32Type>(descr, pool);
        case ::parquet::Type::INT64:
            return new ParquetDictDecoderImpl<::parquet::Int64Type>(descr, pool);
        case ::parquet::Type::INT96:
            return new ParquetDictDecoderImpl<::parquet::Int96Type>(descr, pool);
        case ::parquet::Type::DOUBLE:
            return new ParquetDictDecoderImpl<::parquet::DoubleType>(descr, pool);
        case ::parquet::Type::BYTE_ARRAY:
            return new OmniDictByteArrayDecoderImpl(descr, pool);
        case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
            return new ParquetDictDecoderImpl<::parquet::FLBAType>(descr, pool);
        default:
            ::parquet::ParquetException::NYI("Not supported dictionary decoder type: " + type_num);
    }
    DCHECK(false) << "Should not be able to reach this code";
    return nullptr;
}

template <typename DType>
std::unique_ptr<ParquetDictDecoder<DType>> MakeParquetDictDecoder(
        const ColumnDescriptor* descr = NULLPTR,
        ::arrow::MemoryPool* pool = ::arrow::default_memory_pool()) {
    using OutType = ParquetDictDecoder<DType>;
    auto decoder = MakeOmniDictDecoder(DType::type_num, descr, pool);
    return std::unique_ptr<OutType>(dynamic_cast<OutType*>(decoder));
}

template <typename DType>
std::unique_ptr<ParquetTypedDecoder<DType>> MakeParquetTypedDecoder(
        ::parquet::Encoding::type encoding, const ColumnDescriptor* descr = NULLPTR) {
    using OutType = ParquetTypedDecoder<DType>;
    auto base = MakeOmniParquetDecoder(DType::type_num, encoding, descr);
    return std::unique_ptr<OutType>(dynamic_cast<OutType*>(base));
}

// Advance to the next data page
template <typename DType>
bool ParquetColumnReaderBase<DType>::ReadNewPage() {
    // Loop until we find the next data page.
    while (true) {
        current_page_ = pager_->NextPage();
        if (!current_page_) {
            // EOS
            return false;
        }

        if (current_page_->type() == PageType::DICTIONARY_PAGE) {
            ConfigureDictionary(static_cast<const DictionaryPage*>(current_page_.get()));
            continue;
        } else if (current_page_->type() == PageType::DATA_PAGE) {
            const auto page = std::static_pointer_cast<DataPageV1>(current_page_);
            const int64_t levels_byte_size = InitializeLevelDecoders(
                *page, page->repetition_level_encoding(), page->definition_level_encoding());
            InitializeDataDecoder(*page, levels_byte_size);
            return true;
        } else if (current_page_->type() == PageType::DATA_PAGE_V2) {
            const auto page = std::static_pointer_cast<DataPageV2>(current_page_);
            int64_t levels_byte_size = InitializeLevelDecodersV2(*page);
            InitializeDataDecoder(*page, levels_byte_size);
            return true;
        } else {
            // We don't know what this page type is. We're allowed to skip non-data
            // pages.
            continue;
        }
    }
    return true;
}

template <typename DType>
void ParquetColumnReaderBase<DType>::ConfigureDictionary(const DictionaryPage* page) {
    int encoding = static_cast<int>(page->encoding());
    if (page->encoding() == ::parquet::Encoding::PLAIN_DICTIONARY ||
        page->encoding() == ::parquet::Encoding::PLAIN) {
        encoding = static_cast<int>(::parquet::Encoding::RLE_DICTIONARY);
    }

    auto it = decoders_.find(encoding);
    if (it != decoders_.end()) {
        throw ParquetException("Column cannot have more than one dictionary.");
    }

    if (page->encoding() == ::parquet::Encoding::PLAIN_DICTIONARY ||
        page->encoding() == ::parquet::Encoding::PLAIN) {
        auto dictionary = MakeParquetTypedDecoder<DType>(::parquet::Encoding::PLAIN, descr_);
        dictionary->SetData(page->num_values(), page->data(), page->size());

        // The dictionary is fully decoded during DictionaryDecoder::Init, so the
        // DictionaryPage buffer is no longer required after this step
        std::unique_ptr<ParquetDictDecoder<DType>> decoder = MakeParquetDictDecoder<DType>(descr_, pool_);
        decoder->SetDict(dynamic_cast<DecoderType*>(dictionary.get()));
        decoders_[encoding] =
            std::unique_ptr<DecoderType>(dynamic_cast<DecoderType*>(decoder.release()));
    } else {
        ParquetException::NYI("only plain dictionary encoding has been implemented");
    }

    new_dictionary_ = true;
    current_decoder_ = decoders_[encoding].get();
    DCHECK(current_decoder_);
}

// Initialize repetition and definition level decoders on the next data page.

// If the data page includes repetition and definition levels, we
// initialize the level decoders and return the number of encoded level bytes.
// The return value helps determine the number of bytes in the encoded data.
template <typename DType>
int64_t ParquetColumnReaderBase<DType>::InitializeLevelDecoders(const DataPage& page,
        ::parquet::Encoding::type repetition_level_encoding,
        ::parquet::Encoding::type definition_level_encoding) {
    // Read a data page.
    num_buffered_values_ = page.num_values();

    // Have not decoded any values from the data page yet
    num_decoded_values_ = 0;

    const uint8_t* buffer = page.data();
    int32_t levels_byte_size = 0;
    int32_t max_size = page.size();

    // Data page Layout: Repetition Levels - Definition Levels - encoded values.
    // Levels are encoded as rle or bit-packed.
    // Init repetition levels
    if (max_rep_level_ > 0) {
        int32_t rep_levels_bytes = repetition_level_decoder_.SetData(
            repetition_level_encoding, max_rep_level_,
            static_cast<int>(num_buffered_values_), buffer, max_size);
        buffer += rep_levels_bytes;
        levels_byte_size += rep_levels_bytes;
        max_size -= rep_levels_bytes;
    }

    // Init definition levels
    if (max_def_level_ > 0) {
        int32_t def_levels_bytes = definition_level_decoder_.SetData(
            definition_level_encoding, max_def_level_,
            static_cast<int>(num_buffered_values_), buffer, max_size);
        levels_byte_size += def_levels_bytes;
        max_size -= def_levels_bytes;
    }

    return levels_byte_size;
}

template <typename DType>
int64_t ParquetColumnReaderBase<DType>::InitializeLevelDecodersV2(const ::parquet::DataPageV2& page) {
    // Read a data page.
    num_buffered_values_ = page.num_values();

    // Have not decoded any values from the data page yet
    num_decoded_values_ = 0;
    const uint8_t* buffer = page.data();

    const int64_t total_levels_length =
        static_cast<int64_t>(page.repetition_levels_byte_length()) +
        page.definition_levels_byte_length();

    if (total_levels_length > page.size()) {
        throw ParquetException("Data page too small for levels (corrupt header?)");
    }

    if (max_rep_level_ > 0) {
        repetition_level_decoder_.SetDataV2(page.repetition_levels_byte_length(),
            max_rep_level_, static_cast<int>(num_buffered_values_), buffer);
    }
    // ARROW-17453: Even if max_rep_level_ is 0, there may still be
    // repetition level bytes written and/or reported in the header by
    // some writers (e.g. Athena)
    buffer += page.repetition_levels_byte_length();

    if (max_def_level_ > 0) {
        definition_level_decoder_.SetDataV2(page.definition_levels_byte_length(),
            max_def_level_, static_cast<int>(num_buffered_values_), buffer);
    }

    return total_levels_length;
}

static bool IsDictionaryIndexEncoding(const ::parquet::Encoding::type& e) {
    return e == ::parquet::Encoding::RLE_DICTIONARY || e == ::parquet::Encoding::PLAIN_DICTIONARY;
}

// Get a decoder object for this page or create a new decoder if this is the
// first page with this encoding.
template <typename DType>
void ParquetColumnReaderBase<DType>::InitializeDataDecoder(const DataPage& page, int64_t levels_byte_size) {
    const uint8_t* buffer = page.data() + levels_byte_size;
    const int64_t data_size = page.size() - levels_byte_size;

    if (data_size < 0) {
        throw ParquetException("Page smaller than size of encoded levels");
    }

    ::parquet::Encoding::type encoding = page.encoding();

    if (IsDictionaryIndexEncoding(encoding)) {
        encoding = ::parquet::Encoding::RLE_DICTIONARY;
    }

    auto it = decoders_.find(static_cast<int>(encoding));
    if (it != decoders_.end()) {
        DCHECK(it->second.get() != nullptr);
        current_decoder_ = it->second.get();
    } else {
        switch (encoding) {
            case ::parquet::Encoding::PLAIN: {
                auto decoder = MakeParquetTypedDecoder<DType>(::parquet::Encoding::PLAIN, descr_);
                current_decoder_ = decoder.get();
                decoders_[static_cast<int>(encoding)] = std::move(decoder);
                break;
            }
            case ::parquet::Encoding::RLE: {
                auto decoder = MakeParquetTypedDecoder<DType>(::parquet::Encoding::PLAIN, descr_);
                current_decoder_ = decoder.get();
                decoders_[static_cast<int>(encoding)] = std::move(decoder);
                break;
            }
            case ::parquet::Encoding::RLE_DICTIONARY:
            case ::parquet::Encoding::BYTE_STREAM_SPLIT:
            case ::parquet::Encoding::DELTA_BINARY_PACKED:
            case ::parquet::Encoding::DELTA_BYTE_ARRAY:
            case ::parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY:
            default:
                throw ParquetException("Unknown encoding type.");
        }
    }
    current_encoding_ = encoding;
    current_decoding_type = DType::type_num;
    current_decoder_->SetData(static_cast<int>(num_buffered_values_), buffer,static_cast<int>(data_size));
}

std::shared_ptr<OmniRecordReader> MakeByteArrayRecordReader(const ColumnDescriptor* descr,
        LevelInfo leaf_info,
        ::arrow::MemoryPool* pool,
        bool read_dictionary) {
    if (read_dictionary) {
        std::stringstream ss;
        ss << "Invalid ParquetByteArrayDictionary is not implement yet " << static_cast<int>(descr->physical_type());
        throw ParquetException(ss.str());
    } else {
        return std::make_shared<ParquetByteArrayChunkedRecordReader>(descr, leaf_info, pool);
    }
}

std::shared_ptr<OmniRecordReader> MakeRecordReader(const ColumnDescriptor* descr, LevelInfo leaf_info, 
    ::arrow::MemoryPool* pool, bool read_dictionary, const std::shared_ptr<::arrow::DataType>& type, 
    common::TimeRebaseInfo *rebaseInfoPtr) 
{
    switch (type->id()) {
        case ::arrow::Type::BOOL: {
            return std::make_shared<ParquetTypedRecordReader<OMNI_BOOLEAN, ::parquet::BooleanType>>(descr,
                leaf_info, pool);
        }
        case ::arrow::Type::INT16: {
            return std::make_shared<ParquetShortRecordReader>(descr, leaf_info, pool);
        }
        case ::arrow::Type::INT32: {
            return std::make_shared<ParquetTypedRecordReader<OMNI_INT, ::parquet::Int32Type>>(descr, leaf_info, pool);
        }
        case ::arrow::Type::DATE32: {
            return std::make_shared<ParquetTypedRecordReader<OMNI_DATE32, ::parquet::Int32Type>>(descr,
                leaf_info, pool);
        }
        case ::arrow::Type::INT64: {
            return std::make_shared<ParquetTypedRecordReader<OMNI_LONG, ::parquet::Int64Type>>(descr, leaf_info, pool);
        }
        case ::arrow::Type::TIMESTAMP: {
            switch (descr->physical_type()) {
                case ::parquet::Type::INT64: {
                    const ::arrow::TimeUnit::type unit = 
                        ::arrow::internal::checked_cast<::arrow::TimestampType &>(*type).unit();
                    return std::make_shared<ParquetTimestamp64RecordReader>(descr, leaf_info, pool, unit, 
                        rebaseInfoPtr);
                }
                case ::parquet::Type::INT96:
                    return std::make_shared<ParquetTimestamp96RecordReader>(descr, leaf_info, pool, rebaseInfoPtr);
                default:
                    std::stringstream ss;
                    ss << "RecordReader not support timestamp type " << static_cast<int>(descr->physical_type());
                    throw ParquetException(ss.str());
            }
        }
        case ::arrow::Type::DATE64: {
            return std::make_shared<ParquetTypedRecordReader<OMNI_DATE64, ::parquet::Int64Type>>(descr,
                leaf_info, pool);
        }
        case ::arrow::Type::DOUBLE: {
            return std::make_shared<ParquetTypedRecordReader<OMNI_DOUBLE, ::parquet::DoubleType>>(descr,
                leaf_info, pool);
        }
        case ::arrow::Type::STRING: {
            return MakeByteArrayRecordReader(descr, leaf_info, pool, read_dictionary);
        }
        case ::arrow::Type::DECIMAL: {
            switch (descr->physical_type()) {
                case ::parquet::Type::INT32:
                    return std::make_shared<ParquetIntDecimal64RecordReader>(descr, leaf_info, pool);
                case ::parquet::Type::INT64:
                    return std::make_shared<ParquetTypedRecordReader<OMNI_LONG, ::parquet::Int64Type>>(descr, leaf_info, pool);
                case ::parquet::Type::FIXED_LEN_BYTE_ARRAY: {
                    int32_t precision = ::arrow::internal::checked_cast<const ::arrow::DecimalType&>(*type).precision();
                    if (precision > PARQUET_MAX_DECIMAL64_DIGITS) {
                        return std::make_shared<ParquetFLBADecimal128RecordReader>(descr, leaf_info, pool);
                    } else {
                        return std::make_shared<ParquetFLBADecimal64RecordReader>(descr, leaf_info, pool);
                    }
                }
                default:
                    std::stringstream ss;
                    ss << "RecordReader not support decimal type " << static_cast<int>(descr->physical_type());
                    throw ParquetException(ss.str());
            }
        }
        default: {
            // PARQUET-1481: This can occur if the file is corrupt
            std::stringstream ss;
            ss << "Invalid physical column type: " << static_cast<int>(descr->physical_type());
            throw ParquetException(ss.str());
        }
    }
    // Unreachable code, but suppress compiler warning
    return nullptr;
}

// Helper function used by Decimal128::FromBigEndian
static inline uint64_t UInt64FromBigEndian(const uint8_t* bytes, int32_t length) {
    // We don't bounds check the length here because this is called by
    // FromBigEndian that has a Decimal128 as its out parameters and
    // that function is already checking the length of the bytes and only
    // passes lengths between zero and eight.
    uint64_t result = 0;
    // Using memcpy instead of special casing for length
    // and doing the conversion in 16, 32 parts, which could
    // possibly create unaligned memory access on certain platforms
    memcpy_s(reinterpret_cast<uint8_t*>(&result) + 8 - length, length, bytes, length);
    return ::arrow::bit_util::FromBigEndian(result);
}

static inline Result<omniruntime::type::Decimal128> FromBigEndianToOmniDecimal128(const uint8_t* bytes, int32_t length) {
    static constexpr int32_t kMinDecimalBytes = 1;
    static constexpr int32_t kMaxDecimalBytes = 16;

    int64_t high, low;

    if (ARROW_PREDICT_FALSE(length < kMinDecimalBytes || length > kMaxDecimalBytes)) {
        return Status::Invalid("Length of byte array passed to Decimal128::FromBigEndian ",
                   "was ", length, ", but must be between ", kMinDecimalBytes,
                   " and ", kMaxDecimalBytes);
    }

    // Bytes are coming in big-endian, so the first byte is the MSB and therefore holds the
    // sign bit.
    const bool is_negative = static_cast<int8_t>(bytes[0]) < 0;

    // 1. Extract the high bytes
    // Stop byte of the high bytes
    const int32_t high_bits_offset = std::max(0, length - DECIMAL64_LEN);
    const auto high_bits = UInt64FromBigEndian(bytes, high_bits_offset);

    if (high_bits_offset == DECIMAL64_LEN) {
        // Avoid undefined shift by 64 below
        high = high_bits;
    } else {
        high = -1 * (is_negative && length < kMaxDecimalBytes);
        // Shift left enough bits to make room for the incoming int64_t
        high = SafeLeftShift(high, high_bits_offset * CHAR_BIT);
        // Preserve the upper bits by inplace OR-ing the int64_t
        high |= high_bits;
    }

    // 2. Extract the low bytes
    // Stop byte of the low bytes
    const int32_t low_bits_offset = std::min(length, DECIMAL64_LEN);
    const auto low_bits =
        UInt64FromBigEndian(bytes + high_bits_offset, length - high_bits_offset);

    if (low_bits_offset == DECIMAL64_LEN) {
        // Avoid undefined shift by 64 below
        low = low_bits;
    } else {
        // Sign extend the low bits if necessary
        low = -1 * (is_negative && length < DECIMAL64_LEN);
        // Shift left enough bits to make room for the incoming int64_t
        low = SafeLeftShift(low, low_bits_offset * CHAR_BIT);
        // Preserve the upper bits by inplace OR-ing the int64_t
        low |= low_bits;
    }

    __int128_t temp_high = high;
    temp_high = temp_high << (8 * CHAR_BIT);
    __int128_t val = temp_high | static_cast<uint64_t>(low);

    return omniruntime::type::Decimal128(val);
}

Status RawBytesToDecimal128Bytes(const uint8_t* bytes, int32_t length,
        omniruntime::vec::BaseVector** out_buf, int64_t index) {
    auto out = static_cast<omniruntime::vec::Vector<omniruntime::type::Decimal128>*>(*out_buf);
    ARROW_ASSIGN_OR_RAISE(auto t, FromBigEndianToOmniDecimal128(bytes, length));
    out->SetValue(index, t);
    return Status::OK();
}

Status RawBytesToDecimal64Bytes(const uint8_t* bytes, int32_t length,
        omniruntime::vec::BaseVector** out_buf, int64_t index) {
    auto out = static_cast<omniruntime::vec::Vector<int64_t>*>(*out_buf);

    // Directly Extract the low bytes
    // Stop byte of the low bytes
    int64_t low = 0;
    const bool is_negative = static_cast<int8_t>(bytes[0]) < 0;
    const int32_t low_bits_offset = std::min(length, DECIMAL64_LEN);
    auto low_bits = UInt64FromBigEndian(bytes, low_bits_offset);

    if (low_bits_offset == DECIMAL64_LEN) {
        // Avoid undefined shift by 64 below
        low = low_bits;
    } else {
        // Sign extend the low bits if necessary
        low = -1 * (is_negative && length < DECIMAL64_LEN);
        // Shift left enough bits to make room for the incoming int64_t
        low = SafeLeftShift(low, low_bits_offset * CHAR_BIT);
        // Preserve the upper bits by inplace OR-ing the int64_t
        low |= low_bits;
    }

    out->SetValue(index, low);
    return Status::OK();
}

void DefLevelsToNullsSIMD(const int16_t* def_levels, int64_t num_def_levels, const int16_t max_def_level,
        int64_t* values_read, int64_t* null_count, uint8_t* nulls, int64_t nullsOffset) {
    for (int64_t i = 0; i < num_def_levels; ++i) {
        if (def_levels[i] < max_def_level) {
            BitUtil::SetBit(nulls, nullsOffset + i, true);
            (*null_count)++;
        }
    }
    *values_read = num_def_levels;
}

void DefLevelsToNulls(const int16_t* def_levels, int64_t num_def_levels, LevelInfo level_info,
        int64_t* values_read, int64_t* null_count, uint8_t* nulls, int64_t nullsOffset) {
    if (level_info.rep_level == 0) {
        DefLevelsToNullsSIMD(def_levels, num_def_levels, level_info.def_level, values_read, null_count, nulls,
            nullsOffset);
    } else {
        ::ParquetException::NYI("rep_level > 0 NYI");
    }
}

}