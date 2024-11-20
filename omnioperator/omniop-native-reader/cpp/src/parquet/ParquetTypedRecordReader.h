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


#ifndef OMNI_RUNTIME_COLUMN_TYPE_READER_H
#define OMNI_RUNTIME_COLUMN_TYPE_READER_H

#include "ParquetDecoder.h"
#include <parquet/column_page.h>
#include <parquet/column_reader.h>
#include <arrow/util/int_util_overflow.h>
#include "common/TimeRebaseInfo.h"

using ResizableBuffer = ::arrow::ResizableBuffer;
using namespace omniruntime::vec;

namespace omniruntime::reader {
    constexpr int64_t kMinLevelBatchSize = 1024;
    static constexpr int32_t PARQUET_MAX_DECIMAL64_DIGITS = 18;

    inline void CheckNumberDecoded(int64_t number_decoded, int64_t expected) {
        if (ARROW_PREDICT_FALSE(number_decoded != expected)) {
            ::parquet::ParquetException::EofException("Decoded values " + std::to_string(number_decoded) +
                " does not match expected" + std::to_string(expected));
        }
    }

    template <typename SignedInt, typename Shift>
    SignedInt SafeLeftShift(SignedInt u, Shift shift) {
        using UnsignedInt = typename std::make_unsigned<SignedInt>::type;
        return static_cast<SignedInt>(static_cast<UnsignedInt>(u) << shift);
    }

    ::arrow::Status RawBytesToDecimal128Bytes(const uint8_t* bytes, int32_t length, BaseVector** out_buf, int64_t index);

    ::arrow::Status RawBytesToDecimal64Bytes(const uint8_t* bytes, int32_t length, BaseVector** out_buf, int64_t index);

    void DefLevelsToNulls(const int16_t* def_levels, int64_t num_def_levels, ::parquet::internal::LevelInfo level_info,
        int64_t* values_read, int64_t* null_count, bool* nulls);

    template <typename DType>
    class ParquetColumnReaderBase {
    public:
        using T = typename DType::c_type;

        ParquetColumnReaderBase(const ::parquet::ColumnDescriptor* descr, ::arrow::MemoryPool* pool)
            : descr_(descr),
            max_def_level_(descr->max_definition_level()),
            max_rep_level_(descr->max_repetition_level()),
            num_buffered_values_(0),
            num_decoded_values_(0),
            pool_(pool),
            current_decoder_(nullptr),
            current_encoding_(::parquet::Encoding::UNKNOWN) {}

        virtual ~ParquetColumnReaderBase() = default;

    protected:
        int64_t ReadDefinitionLevels(int64_t batch_size, int16_t* levels) {
            if (max_def_level_ == 0) {
                return 0;
            }
            return definition_level_decoder_.Decode(static_cast<int>(batch_size), levels);
        }

        bool HasNextInternal() {
            if (num_buffered_values_ == 0 || num_decoded_values_ == num_buffered_values_) {
                if (!ReadNewPage() || num_buffered_values_ == 0) {
                    return false;
                }
            }
            return true;
        }

        int64_t ReadRepetitionLevels(int64_t batch_size, int16_t* levels) {
            if (max_rep_level_ == 0) {
                return 0;
            }
            return repetition_level_decoder_.Decode(static_cast<int>(batch_size), levels);
        }

        bool ReadNewPage();

        void ConfigureDictionary(const ::parquet::DictionaryPage* page);

        int64_t InitializeLevelDecoders(const ::parquet::DataPage& page,
            ::parquet::Encoding::type repetition_level_encoding,
            ::parquet::Encoding::type definition_level_encoding);

        int64_t InitializeLevelDecodersV2(const ::parquet::DataPageV2& page);

        void InitializeDataDecoder(const ::parquet::DataPage& page, int64_t levels_byte_size);

        int64_t available_values_current_page() const {
            return num_buffered_values_ - num_decoded_values_;
        }

        const ::parquet::ColumnDescriptor* descr_;
        const int16_t max_def_level_;
        const int16_t max_rep_level_;

        std::unique_ptr<::parquet::PageReader> pager_;
        std::shared_ptr<::parquet::Page> current_page_;

        ::parquet::LevelDecoder definition_level_decoder_;
        ::parquet::LevelDecoder repetition_level_decoder_;

        int64_t num_buffered_values_;
        int64_t num_decoded_values_;

        ::arrow::MemoryPool* pool_;

        using DecoderType = ParquetTypedDecoder<DType>;
        DecoderType* current_decoder_;
        ::parquet::Encoding::type current_encoding_;
        ::parquet::Type::type current_decoding_type;

        bool new_dictionary_ = false;

        std::unordered_map<int, std::unique_ptr<DecoderType>> decoders_;

        void ConsumeBufferedValues(int64_t num_values) {
            num_decoded_values_ += num_values;
        }
    };

    class OmniRecordReader {
    public:
        virtual ~OmniRecordReader() = default;

        /// \brief Attempt to read indicated number of records from column chunk
        /// Note that for repeated fields, a record may have more than one value
        /// and all of them are read.
        virtual int64_t ReadRecords(int64_t num_records) = 0;

        /// \brief Attempt to skip indicated number of records from column chunk.
        /// Note that for repeated fields, a record may have more than one value
        /// and all of them are skipped.
        /// \return number of records skipped
        virtual int64_t SkipRecords(int64_t num_records) = 0;

        /// \brief Pre-allocate space for data. Results in better flat read performance
        virtual void Reserve(int64_t num_values) = 0;

        /// \brief Clear consumed values and repetition/definition levels as the
        /// result of calling ReadRecords
        virtual void Reset() = 0;

        /// \brief Return true if the record reader has more internal data yet to
        /// process
        virtual bool HasMoreData() const = 0;

        /// \brief Advance record reader to the next row group. Must be set before
        /// any records could be read/skipped.
        /// \param[in] reader obtained from RowGroupReader::GetColumnPageReader
        virtual void SetPageReader(std::unique_ptr<parquet::PageReader> reader) = 0;

        virtual BaseVector* GetBaseVec() = 0;

        /// \brief Decoded definition levels
        int16_t* def_levels() const {
            return reinterpret_cast<int16_t*>(def_levels_->mutable_data());
        }

        /// \brief Decoded repetition levels
        int16_t* rep_levels() const {
            return reinterpret_cast<int16_t*>(rep_levels_->mutable_data());
        }

        /// \brief Decoded values, including nulls, if any
        /// FLBA and ByteArray types do not use this array and read into their own
        /// builders.
        uint8_t* values() const { return values_->mutable_data(); }

        /// \brief Number of values written, including space left for nulls if any.
        /// If this Reader was constructed with read_dense_for_nullable(), there is no space for
        /// nulls and null_count() will be 0. There is no read-ahead/buffering for values. For
        /// FLBA and ByteArray types this value reflects the values written with the last
        /// ReadRecords call since those readers will reset the values after each call.
        int64_t values_written() const { return values_written_; }

        /// \brief Number of definition / repetition levels (from those that have
        /// been decoded) that have been consumed inside the reader.
        int64_t levels_position() const { return levels_position_; }

        /// \brief Number of definition / repetition levels that have been written
        /// internally in the reader. This may be larger than values_written() because
        /// for repeated fields we need to look at the levels in advance to figure out
        /// the record boundaries.
        int64_t levels_written() const { return levels_written_; }

        /// \brief Number of nulls in the leaf that we have read so far into the
        /// values vector. This is only valid when !read_dense_for_nullable(). When
        /// read_dense_for_nullable() it will always be 0.
        int64_t null_count() const { return null_count_; }

        /// \brief True if the leaf values are nullable
        bool nullable_values() const { return nullable_values_; }

        /// \brief True if reading directly as Arrow dictionary-encoded
        bool read_dictionary() const { return read_dictionary_; }


        /// \brief Indicates if we can have nullable values. Note that repeated fields
        /// may or may not be nullable.
        bool nullable_values_;

        bool at_record_start_;
        int64_t records_read_;
        int64_t values_decode_;

        /// \brief Stores values. These values are populated based on each ReadRecords
        /// call. No extra values are buffered for the next call. SkipRecords will not
        /// add any value to this buffer.
        std::shared_ptr<ResizableBuffer> values_;
        /// \brief False for BYTE_ARRAY, in which case we don't allocate the values
        /// buffer and we directly read into builder classes.
        bool uses_values_;

        /// \brief Values that we have read into 'values_' + 'null_count_'.
        int64_t values_written_;
        int64_t values_capacity_;
        int64_t null_count_;

        /// \brief Buffer for definition levels. May contain more levels than
        /// is actually read. This is because we read levels ahead to
        /// figure out record boundaries for repeated fields.
        /// For flat required fields, 'def_levels_' and 'rep_levels_' are not
        ///  populated. For non-repeated fields 'rep_levels_' is not populated.
        /// 'def_levels_' and 'rep_levels_' must be of the same size if present.
        std::shared_ptr<ResizableBuffer> def_levels_;
        /// \brief Buffer for repetition levels. Only populated for repeated
        /// fields.
        std::shared_ptr<ResizableBuffer> rep_levels_;

        /// \brief Number of definition / repetition levels that have been written
        /// internally in the reader. This may be larger than values_written() since
        /// for repeated fields we need to look at the levels in advance to figure out
        /// the record boundaries.
        int64_t levels_written_;
        /// \brief Position of the next level that should be consumed.
        int64_t levels_position_;
        int64_t levels_capacity_;

        bool read_dictionary_ = false;
    };

    /**
    * ParquetTypedRecordReader is used to generate omnivector directly from the def_level/rep_level/values.
    * And we directly use omnivector's nulls to store each null value flag instead of bitmap to reduce extra cost.
    * When setting omnivector's values, it can choose whether transferring values according to the TYPE_ID and DType.
    * @tparam TYPE_ID omni type
    * @tparam DType parquet store type
    */
    template <DataTypeId TYPE_ID, typename DType>
    class ParquetTypedRecordReader : public ParquetColumnReaderBase<DType>, virtual public OmniRecordReader {
    public:
        using T = typename DType::c_type;
        using V = typename NativeType<TYPE_ID>::type;
        using BASE = ParquetColumnReaderBase<DType>;

        explicit ParquetTypedRecordReader(const ::parquet::ColumnDescriptor* descr,
                ::parquet::internal::LevelInfo leaf_info, ::arrow::MemoryPool* pool)
            // Pager must be set using SetPageReader.
            : BASE(descr, pool) {
            leaf_info_ = leaf_info;
            nullable_values_ = leaf_info.HasNullableValues();
            at_record_start_ = true;
            values_written_ = 0;
            null_count_ = 0;
            values_capacity_ = 0;
            levels_written_ = 0;
            levels_position_ = 0;
            levels_capacity_ = 0;
            uses_values_ = !(descr->physical_type() == ::parquet::Type::BYTE_ARRAY);
            byte_width_ = descr->type_length();
            values_decode_ = 0;

            if (uses_values_) {
                values_ = ::parquet::AllocateBuffer(pool);
            }
            def_levels_ = ::parquet::AllocateBuffer(pool);
            rep_levels_ = ::parquet::AllocateBuffer(pool);
            Reset();
        }

        ~ParquetTypedRecordReader() {
          if (parquet_vec_ != nullptr) {
            delete[] parquet_vec_;
          }
        }

        // Compute the values capacity in bytes for the given number of elements
        int64_t bytes_for_values(int64_t nitems) const {
            int64_t type_size = GetTypeByteSize(this->descr_->physical_type());
            int64_t bytes_for_values = -1;
            if (::arrow::internal::MultiplyWithOverflow(nitems, type_size, &bytes_for_values)) {
                throw ::parquet::ParquetException("Total size of items too large");
            }
            return bytes_for_values;
        }

        int64_t ReadRecords(int64_t num_records) override {
            if (num_records == 0) return 0;
            // Delimit records, then read values at the end
            int64_t records_read = 0;

            if (has_values_to_process()) {
                records_read += ReadRecordData(num_records);
            }

            int64_t level_batch_size = std::max<int64_t>(kMinLevelBatchSize, num_records);

            // If we are in the middle of a record, we continue until reaching the
            // desired number of records or the end of the current record if we've found
            // enough records
            while (!at_record_start_ || records_read < num_records) {
                // Is there more data to read in this row group?
                if (!this->HasNextInternal()) {
                    if (!at_record_start_) {
                        // We ended the row group while inside a record that we haven't seen
                        // the end of yet. So increment the record count for the last record in
                        // the row group
                        ++records_read;
                        at_record_start_ = true;
                    }
                    break;
               }

                /// We perform multiple batch reads until we either exhaust the row group
                /// or observe the desired number of records
                int64_t batch_size =
                    std::min(level_batch_size, this->available_values_current_page());

                // No more data in column
                if (batch_size == 0) {
                    break;
                }

                if (this->max_def_level_ > 0) {
                    ReserveLevels(batch_size);

                    int16_t* def_levels = this->def_levels() + levels_written_;
                    int16_t* rep_levels = this->rep_levels() + levels_written_;

                    // Not present for non-repeated fields
                    int64_t levels_read = 0;
                    if (this->max_rep_level_ > 0) {
                        levels_read = this->ReadDefinitionLevels(batch_size, def_levels);
                        if (this->ReadRepetitionLevels(batch_size, rep_levels) != levels_read) {
                            throw ::parquet::ParquetException("Number of decoded rep / def levels did not match");
                        }
                    } else if (this->max_def_level_ > 0) {
                        levels_read = this->ReadDefinitionLevels(batch_size, def_levels);
                    }

                    // Exhausted column chunk
                    if (levels_read == 0) {
                      break;
                    }

                    levels_written_ += levels_read;
                    records_read += ReadRecordData(num_records - records_read);
                } else {
                    // No repetition or definition levels
                    batch_size = std::min(num_records - records_read, batch_size);
                    records_read += ReadRecordData(batch_size);
                }
            }

            return records_read;
        }

        // Throw away levels from start_levels_position to levels_position_.
        // Will update levels_position_, levels_written_, and levels_capacity_
        // accordingly and move the levels to left to fill in the gap.
        // It will resize the buffer without releasing the memory allocation.
        void ThrowAwayLevels(int64_t start_levels_position) {
            ARROW_DCHECK_LE(levels_position_, levels_written_);
            ARROW_DCHECK_LE(start_levels_position, levels_position_);
            ARROW_DCHECK_GT(this->max_def_level_, 0);
            ARROW_DCHECK_NE(def_levels_, nullptr);

            int64_t gap = levels_position_ - start_levels_position;
            if (gap == 0) return;

            int64_t levels_remaining = levels_written_ - gap;

            auto left_shift = [&](ResizableBuffer* buffer) {
                int16_t* data = reinterpret_cast<int16_t*>(buffer->mutable_data());
                std::copy(data + levels_position_, data + levels_written_,
                        data + start_levels_position);
                PARQUET_THROW_NOT_OK(buffer->Resize(levels_remaining * sizeof(int16_t),
                                                  /*shrink_to_fit=*/false));
            };

            left_shift(def_levels_.get());

            if (this->max_rep_level_ > 0) {
                ARROW_DCHECK_NE(rep_levels_, nullptr);
                left_shift(rep_levels_.get());
            }

            levels_written_ -= gap;
            levels_position_ -= gap;
            levels_capacity_ -= gap;
        }


        int64_t SkipRecords(int64_t num_records) override {
            throw ::parquet::ParquetException("SkipRecords not implemented yet");
        }

        // We may outwardly have the appearance of having exhausted a column chunk
        // when in fact we are in the middle of processing the last batch
        bool has_values_to_process() const { return levels_position_ < levels_written_; }

        // Process written repetition/definition levels to reach the end of
        // records. Only used for repeated fields.
        // Process no more levels than necessary to delimit the indicated
        // number of logical records. Updates internal state of RecordReader
        //
        // \return Number of records delimited
        int64_t DelimitRecords(int64_t num_records, int64_t* values_seen) {
            int64_t values_to_read = 0;
            int64_t records_read = 0;

            const int16_t* def_levels = this->def_levels() + levels_position_;
            const int16_t* rep_levels = this->rep_levels() + levels_position_;

            DCHECK_GT(this->max_rep_level_, 0);

            // Count logical records and number of values to read
            while (levels_position_ < levels_written_) {
                const int16_t rep_level = *rep_levels++;
                if (rep_level == 0) {
                    // If at_record_start_ is true, we are seeing the start of a record
                    // for the second time, such as after repeated calls to
                    // DelimitRecords. In this case we must continue until we find
                    // another record start or exhausting the ColumnChunk
                    if (!at_record_start_) {
                        // We've reached the end of a record; increment the record count.
                        ++records_read;
                        if (records_read == num_records) {
                            // We've found the number of records we were looking for. Set
                            // at_record_start_ to true and break
                            at_record_start_ = true;
                            break;
                        }
                    }
                }
                // We have decided to consume the level at this position; therefore we
                // must advance until we find another record boundary
                at_record_start_ = false;

                const int16_t def_level = *def_levels++;
                if (def_level == this->max_def_level_) {
                    ++values_to_read;
                }
                ++levels_position_;
            }
            *values_seen = values_to_read;
            return records_read;
        }

        void Reserve(int64_t capacity) override {
            ReserveLevels(capacity);
            ReserveValues(capacity);
            InitVec(capacity);
        }

        virtual void InitVec(int64_t capacity) {
            vec_ = new Vector<V>(capacity);
            auto capacity_bytes = capacity * byte_width_;
            if (parquet_vec_ != nullptr) {
                memset_s(parquet_vec_, capacity_bytes, 0, capacity_bytes);
            } else {
                parquet_vec_ = new uint8_t[capacity_bytes];
            }
            // Init nulls
            if (nullable_values_) {
                nulls_ = unsafe::UnsafeBaseVector::GetNulls(vec_);
            }
        }


        int64_t UpdateCapacity(int64_t capacity, int64_t size, int64_t extra_size) {
            if (extra_size < 0) {
                throw ::parquet::ParquetException("Negative size (corrupt file?)");
            }
            int64_t target_size = -1;
            if (::arrow::internal::AddWithOverflow(size, extra_size, &target_size)) {
                throw ::parquet::ParquetException("Allocation size too large (corrupt file?)");
            }
            if (target_size >= (1LL << 62)) {
                throw ::parquet::ParquetException("Allocation size too large (corrupt file?)");
            }
            if (capacity >= target_size) {
                return capacity;
            }
            return ::arrow::bit_util::NextPower2(target_size);
        }

        void ReserveLevels(int64_t extra_levels) {
            if (this->max_def_level_ > 0) {
                const int64_t new_levels_capacity =
                UpdateCapacity(levels_capacity_, levels_written_, extra_levels);
                if (new_levels_capacity > levels_capacity_) {
                    constexpr auto kItemSize = static_cast<int64_t>(sizeof(int16_t));
                    int64_t capacity_in_bytes = -1;
                    if (::arrow::internal::MultiplyWithOverflow(new_levels_capacity, kItemSize, &capacity_in_bytes)) {
                        throw ::parquet::ParquetException("Allocation size too large (corrupt file?)");
                    }
                    PARQUET_THROW_NOT_OK(
                    def_levels_->Resize(capacity_in_bytes, /*shrink_to_fit=*/false));
                    if (this->max_rep_level_ > 0) {
                        PARQUET_THROW_NOT_OK(
                        rep_levels_->Resize(capacity_in_bytes, /*shrink_to_fit=*/false));
                    }
                    levels_capacity_ = new_levels_capacity;
                }
            }
        }

        void ReserveValues(int64_t extra_values) {
            const int64_t new_values_capacity =
                UpdateCapacity(values_capacity_, values_written_, extra_values);
            if (new_values_capacity > values_capacity_) {
                // XXX(wesm): A hack to avoid memory allocation when reading directly
                // into builder classes
                if (uses_values_) {
                    PARQUET_THROW_NOT_OK(values_->Resize(bytes_for_values(new_values_capacity),
                                                     /*shrink_to_fit=*/false));
                }
                values_capacity_ = new_values_capacity;
            }
       }

        void Reset() override {
            ResetValues();
            if (levels_written_ > 0) {
            // Throw away levels from 0 to levels_position_.
                ThrowAwayLevels(0);
            }

            vec_ = nullptr;
        }

        void SetPageReader(std::unique_ptr<::parquet::PageReader> reader) override {
            at_record_start_ = true;
            this->pager_ = std::move(reader);
            ResetDecoders();
        }

        bool HasMoreData() const override { return this->pager_ != nullptr; }

        const ::parquet::ColumnDescriptor* descr() const { return this->descr_; }

        // Dictionary decoders must be reset when advancing row groups
        void ResetDecoders() { this->decoders_.clear(); }

        virtual void ReadValuesSpaced(int64_t values_with_nulls, int64_t null_count) {
            int64_t num_decoded = this->current_decoder_->DecodeSpaced(
                ValuesHead<T>(), static_cast<int>(values_with_nulls),
                static_cast<int>(null_count), nulls_ + values_written_);
            CheckNumberDecoded(num_decoded, values_with_nulls);
        }

        virtual void ReadValuesDense(int64_t values_to_read) {
        int64_t num_decoded =
            this->current_decoder_->Decode(ValuesHead<T>(), static_cast<int>(values_to_read));
        CheckNumberDecoded(num_decoded, values_to_read);
        }

        // Return number of logical records read.
        int64_t ReadRecordData(int64_t num_records) {
            // Conservative upper bound
            const int64_t possible_num_values =
                std::max<int64_t>(num_records, levels_written_ - levels_position_);
            ReserveValues(possible_num_values);

            const int64_t start_levels_position = levels_position_;

            int64_t records_read = 0;
            int64_t values_to_read = 0;
            if (this->max_rep_level_ > 0) {
                records_read = DelimitRecords(num_records, &values_to_read);
            } else if (this->max_def_level_ > 0) {
                records_read = std::min<int64_t>(levels_written_ - levels_position_, num_records);
                levels_position_ += records_read;
            } else {
                records_read = values_to_read = num_records;
            }

            int64_t null_count = 0;
            if (leaf_info_.HasNullableValues()) {
                int64_t values_read = 0;
                DefLevelsToNulls(def_levels() + start_levels_position, levels_position_ - start_levels_position, leaf_info_,
                    &values_read, &null_count, nulls_ + start_levels_position);
                values_to_read = values_read - null_count;
                DCHECK_GE(values_to_read, 0);
                ReadValuesSpaced(values_read, null_count);
            } else {
                DCHECK_GE(values_to_read, 0);
                ReadValuesDense(values_to_read);
            }

            if (this->leaf_info_.def_level > 0) {
                // Optional, repeated, or some mix thereof
                this->ConsumeBufferedValues(levels_position_ - start_levels_position);
            } else {
                // Flat, non-repeated
                this->ConsumeBufferedValues(values_to_read);
            }
            // Total values, including null spaces, if any
            values_written_ += values_to_read + null_count;
            null_count_ += null_count;

            return records_read;
        }

        void ResetValues() {
            if (values_written_ <= 0) {
                return;
            }
            // Resize to 0, but do not shrink to fit
            if (uses_values_) {
                PARQUET_THROW_NOT_OK(values_->Resize(0, /*shrink_to_fit=*/false));
            }
            values_written_ = 0;
            values_capacity_ = 0;
            null_count_ = 0;
            values_decode_ = 0;
        }

        virtual BaseVector* GetBaseVec() {
            if (vec_ == nullptr) {
                throw ::parquet::ParquetException("BaseVector is nullptr!");
            }
            auto res = dynamic_cast<Vector<V>*>(vec_);
            res->SetValues(0, Values<T>(), values_written_);
            return vec_;
        }

    protected:
        template <typename T>
        T* ValuesHead() {
            return reinterpret_cast<T*>(values_->mutable_data()) + values_written_;
        }

        template <typename T>
        T* Values() const {
            return reinterpret_cast<T*>(values_->mutable_data());
        }
        ::parquet::internal::LevelInfo leaf_info_;
        omniruntime::vec::BaseVector* vec_ = nullptr;
        uint8_t* parquet_vec_ = nullptr;
        bool* nulls_ = nullptr;
        int32_t byte_width_;
    };

    class ParquetShortRecordReader : public ParquetTypedRecordReader<OMNI_SHORT, ::parquet::Int32Type> {
    public:
        using BASE = ParquetTypedRecordReader<OMNI_SHORT, ::parquet::Int32Type>;
        ParquetShortRecordReader(const ::parquet::ColumnDescriptor* descr, ::parquet::internal::LevelInfo leaf_info,
            ::arrow::MemoryPool* pool)
            : BASE(descr, leaf_info, pool) {}

        BaseVector* GetBaseVec() override {
            if (vec_ == nullptr) {
                throw ::parquet::ParquetException("GetBaseVec() is nullptr!");
            }
            auto res = dynamic_cast<Vector<V> *>(vec_);
            auto values = Values<int32_t>();
            for (int i = 0; i < values_written_; i++) {
                res->SetValue(i, static_cast<int16_t>(values[i]));
            }
            return vec_;
        }
    };

    class ParquetIntDecimal64RecordReader : public ParquetTypedRecordReader<OMNI_DECIMAL64, ::parquet::Int32Type> {
    public:
        using BASE = ParquetTypedRecordReader<OMNI_DECIMAL64, ::parquet::Int32Type>;
        ParquetIntDecimal64RecordReader(const ::parquet::ColumnDescriptor* descr, ::parquet::internal::LevelInfo leaf_info,
            ::arrow::MemoryPool* pool)
            : BASE(descr, leaf_info, pool) {}

        BaseVector* GetBaseVec() override {
            if (vec_ == nullptr) {
                throw ::parquet::ParquetException("GetBaseVec() is nullptr!");
            }
            auto res = dynamic_cast<Vector<V> *>(vec_);
            auto values = Values<int32_t>();
            for (int i = 0; i < values_written_; i++) {
                res->SetValue(i, static_cast<int64_t>(values[i]));
            }
            return vec_;
        }
    };

    class ParquetFLBADecimal64RecordReader : public ParquetTypedRecordReader<OMNI_DECIMAL64, ::parquet::FLBAType> {
    public:
        ParquetFLBADecimal64RecordReader(const ::parquet::ColumnDescriptor* descr, ::parquet::internal::LevelInfo leaf_info,
            ::arrow::MemoryPool* pool)
            : ParquetTypedRecordReader<OMNI_DECIMAL64, ::parquet::FLBAType>(descr, leaf_info, pool) {}

        void ReadValuesDense(int64_t values_to_read) override {
            uint8_t* values = GetParquetVecOffsetPtr(0);
            int64_t num_decoded = this->current_decoder_->Decode(
                reinterpret_cast<::parquet::FixedLenByteArray*>(values), static_cast<int>(values_to_read));
            values_decode_ += num_decoded;
            DCHECK_EQ(num_decoded, values_to_read);
        }

        void ReadValuesSpaced(int64_t values_to_read, int64_t null_count) override {
            uint8_t* values = GetParquetVecOffsetPtr(0);
            int64_t no_null_values_to_read = values_to_read - null_count;
            int64_t num_decoded = this->current_decoder_->Decode(
                reinterpret_cast<::parquet::FixedLenByteArray*>(values), static_cast<int>(no_null_values_to_read));
            values_decode_ += num_decoded;
            DCHECK_EQ(num_decoded, no_null_values_to_read);
        }

        uint8_t* GetParquetVecOffsetPtr(int index) {
            return parquet_vec_ + (index + values_decode_) * byte_width_;
        }

        uint8_t* GetParquetVecHeadPtr(int index) {
            return parquet_vec_ + index * byte_width_;
        }

        BaseVector* GetBaseVec() override {
            if (vec_ == nullptr) {
                throw ::parquet::ParquetException("GetBaseVector() is nullptr");
            }
            int index = 0;
            for (int64_t i = 0; i < values_written_; i++) {
                if (nulls_ == nullptr || !nulls_[i]) {
                    PARQUET_THROW_NOT_OK(RawBytesToDecimal64Bytes(GetParquetVecHeadPtr(index++), byte_width_, &vec_, i));
                }
            }
            return vec_;
        }
    };

    class ParquetFLBADecimal128RecordReader : public ParquetTypedRecordReader<OMNI_DECIMAL128, ::parquet::FLBAType> {
    public:
        ParquetFLBADecimal128RecordReader(const ::parquet::ColumnDescriptor* descr, ::parquet::internal::LevelInfo leaf_info,
            ::arrow::MemoryPool* pool)
            : ParquetTypedRecordReader<OMNI_DECIMAL128, ::parquet::FLBAType>(descr, leaf_info, pool) {}

        void ReadValuesDense(int64_t values_to_read) override {
            uint8_t* values = GetParquetVecOffsetPtr(0);
            int64_t num_decoded = this->current_decoder_->Decode(
                reinterpret_cast<::parquet::FixedLenByteArray*>(values), static_cast<int>(values_to_read));
            values_decode_ += num_decoded;
            DCHECK_EQ(num_decoded, values_to_read);
        }

        void ReadValuesSpaced(int64_t values_to_read, int64_t null_count) override {
            uint8_t* values = GetParquetVecOffsetPtr(0);
            int64_t no_null_values_to_read = values_to_read - null_count;
            int64_t num_decoded = this->current_decoder_->Decode(
                reinterpret_cast<::parquet::FixedLenByteArray*>(values), static_cast<int>(no_null_values_to_read));
            values_decode_ += num_decoded;
            DCHECK_EQ(num_decoded, no_null_values_to_read);
        }

        uint8_t* GetParquetVecOffsetPtr(int index) {
            return parquet_vec_ + (index + values_decode_) * byte_width_;
        }

        uint8_t* GetParquetVecHeadPtr(int index) {
            return parquet_vec_ + index * byte_width_;
        }

        BaseVector* GetBaseVec() override {
            if (vec_ == nullptr) {
                throw ::parquet::ParquetException("GetBaseVector() is nullptr");
            }
            int index = 0;
            for (int64_t i = 0; i < values_written_; i++) {
                if (nulls_ == nullptr || !nulls_[i]) {
                    PARQUET_THROW_NOT_OK(RawBytesToDecimal128Bytes(GetParquetVecHeadPtr(index++), byte_width_, &vec_, i));
                }
            }
            return vec_;
        }
    };

    class ParquetByteArrayChunkedRecordReader : public ParquetTypedRecordReader<OMNI_VARCHAR, ::parquet::ByteArrayType> {
    public:
        ParquetByteArrayChunkedRecordReader(const ::parquet::ColumnDescriptor* descr, ::parquet::internal::LevelInfo leaf_info,
            ::arrow::MemoryPool* pool)
            : ParquetTypedRecordReader<OMNI_VARCHAR, ::parquet::ByteArrayType>(descr, leaf_info, pool) {
                DCHECK_EQ(descr_->physical_type(), ::parquet::Type::BYTE_ARRAY);
            }

        void InitVec(int64_t capacity) override {
            vec_ = new Vector<LargeStringContainer<std::string_view>>(capacity);
            if (nullable_values_) {
                nulls_ = unsafe::UnsafeBaseVector::GetNulls(vec_);
            }
        }

        void ReadValuesDense(int64_t values_to_read) override {
            int64_t num_decoded = this->current_decoder_->DecodeArrowNonNull(static_cast<int>(values_to_read),
                &vec_, values_written_);
            CheckNumberDecoded(num_decoded, values_to_read);
        }

        void ReadValuesSpaced(int64_t values_to_read, int64_t null_count) override {
            int64_t num_decoded = this->current_decoder_->DecodeArrow(
                static_cast<int>(values_to_read), static_cast<int>(null_count),
                nulls_, values_written_, &vec_);
            CheckNumberDecoded(num_decoded, values_to_read -  null_count);
        }

        BaseVector* GetBaseVec() {
            if (vec_ == nullptr) {
                throw ::parquet::ParquetException("GetBaseVec() is nullptr");
            }
            return vec_;
        }
    };

    class ParquetTimestamp64RecordReader : public ParquetTypedRecordReader<OMNI_TIMESTAMP, ::parquet::Int64Type> {
    public:
        using BASE = ParquetTypedRecordReader<OMNI_TIMESTAMP, ::parquet::Int64Type>;
        ParquetTimestamp64RecordReader(const ::parquet::ColumnDescriptor* descr,
            ::parquet::internal::LevelInfo leaf_info, ::arrow::MemoryPool* pool, ::arrow::TimeUnit::type unit,
            common::TimeRebaseInfo *rebasePtr)
            : BASE(descr, leaf_info, pool), timeUnit(unit), rebaseInfoPtr(rebasePtr)
        {}

        BaseVector* GetBaseVec() override
        {
            if (vec_ == nullptr) {
                throw ::parquet::ParquetException("GetBaseVec() is nullptr!");
            }
            auto res = dynamic_cast<Vector<V> *>(vec_);
            auto values = Values<T>();
            if (timeUnit == ::arrow::TimeUnit::MILLI) {
                for (int i = 0; i < values_written_; i++) {
                    res->SetValue(i, rebaseInfoPtr->timestampRebaseFunc(values[i] * 1000L));
                }
            } else {
                for (int i = 0; i < values_written_; i++) {
                    res->SetValue(i, rebaseInfoPtr->timestampRebaseFunc(values[i]));
                }
            }
            return vec_;
        }

    private:
        ::arrow::TimeUnit::type timeUnit;
        common::TimeRebaseInfo *rebaseInfoPtr;
    };

    class ParquetTimestamp96RecordReader : public ParquetTypedRecordReader<OMNI_TIMESTAMP, ::parquet::Int96Type> {
    public:
        using BASE = ParquetTypedRecordReader<OMNI_TIMESTAMP, ::parquet::Int96Type>;
        ParquetTimestamp96RecordReader(const ::parquet::ColumnDescriptor* descr,
            ::parquet::internal::LevelInfo leaf_info, ::arrow::MemoryPool* pool, common::TimeRebaseInfo *rebasePtr)
            : BASE(descr, leaf_info, pool), rebaseInfoPtr(rebasePtr)
        {}

        BaseVector* GetBaseVec() override
        {
            if (vec_ == nullptr) {
                throw ::parquet::ParquetException("GetBaseVec() is nullptr!");
            }
            auto res = dynamic_cast<Vector<V> *>(vec_);
            auto values = Values<T>();
            for (int i = 0; i < values_written_; i++) {
                if (values[i].value[2] == 0) {
                    res->SetValue(i, 0);
                } else {
                    res->SetValue(i, rebaseInfoPtr->int96RebaseFunc(Int96GetMicroSeconds(values[i])));
                }
            }
            return vec_;
        }

    private:
        common::TimeRebaseInfo *rebaseInfoPtr;
    };

    std::shared_ptr<OmniRecordReader> MakeRecordReader(const ::parquet::ColumnDescriptor* descr,
        ::parquet::internal::LevelInfo leaf_info, ::arrow::MemoryPool* pool, const bool read_dictionary,
        const std::shared_ptr<::arrow::DataType>& type, common::TimeRebaseInfo *rebaseInfoPtr);
}
#endif //OMNI_RUNTIME_COLUMN_TYPE_READER_H