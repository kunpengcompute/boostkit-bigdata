/**
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

#include "splitter.h"
#include "utils.h"

SplitOptions SplitOptions::Defaults() { return SplitOptions(); }

// 计算分区id,每个batch初始化
int Splitter::ComputeAndCountPartitionId(VectorBatch& vb) {
    auto num_rows = vb.GetRowCount();
    memset_s(partition_id_cnt_cur_, num_partitions_ * sizeof(int32_t), 0, num_partitions_ * sizeof(int32_t));
    partition_id_.resize(num_rows);

    if (singlePartitionFlag) {
        partition_id_cnt_cur_[0] = num_rows;
        partition_id_cnt_cache_[0] += num_rows;
        for (auto i = 0; i < num_rows; ++i) {
            partition_id_[i] = 0;
        }
    } else {
        auto hash_vct = reinterpret_cast<Vector<int32_t> *>(vb.Get(0));
        for (auto i = 0; i < num_rows; ++i) {
            // positive mod
            int32_t pid = hash_vct->GetValue(i);
            if (pid >= num_partitions_) {
                LogsError(" Illegal pid Value: %d >= partition number %d .", pid, num_partitions_);
                throw std::runtime_error("Shuffle pidVec Illegal pid Value!");
            }
            partition_id_[i] = pid;
            partition_id_cnt_cur_[pid]++;
            partition_id_cnt_cache_[pid]++;
        }
    }
    return 0;
}

//分区信息内存分配
int Splitter::AllocatePartitionBuffers(int32_t partition_id, int32_t new_size) {
    std::vector<std::shared_ptr<Buffer>> new_binary_builders;
    std::vector<std::shared_ptr<Buffer>> new_value_buffers;
    std::vector<std::shared_ptr<Buffer>> new_validity_buffers;

    int num_fields = column_type_id_.size();
    auto fixed_width_idx = 0;

    for (auto i = 0; i < num_fields; ++i) {
        switch (column_type_id_[i]) {
            case SHUFFLE_BINARY: {
                break;
            }
            case SHUFFLE_LARGE_BINARY:
            case SHUFFLE_NULL:
                 break;
            case SHUFFLE_1BYTE:
            case SHUFFLE_2BYTE:
            case SHUFFLE_4BYTE:
            case SHUFFLE_8BYTE:
            case SHUFFLE_DECIMAL128:
            default: {
                void *ptr_tmp = static_cast<void *>(options_.allocator->Alloc(new_size * (1 << column_type_id_[i])));
                fixed_valueBuffer_size_[partition_id] += new_size * (1 << column_type_id_[i]);
                if (nullptr == ptr_tmp) {
                    throw std::runtime_error("Allocator for AllocatePartitionBuffers Failed! ");
                }
                std::shared_ptr<Buffer> value_buffer (new Buffer((uint8_t *)ptr_tmp, 0, new_size * (1 << column_type_id_[i])));
                new_value_buffers.push_back(std::move(value_buffer));
                new_validity_buffers.push_back(nullptr);
                fixed_width_idx++;
                break;
            }
        }
    }

    // point to newly allocated buffers
    fixed_width_idx = 0;
    for (auto i = 0; i < num_fields; ++i) {
        switch (column_type_id_[i]) {
            case SHUFFLE_1BYTE:
            case SHUFFLE_2BYTE:
            case SHUFFLE_4BYTE:
            case SHUFFLE_8BYTE:
            case SHUFFLE_DECIMAL128: {
                partition_fixed_width_value_addrs_[fixed_width_idx][partition_id] =
                        const_cast<uint8_t *>(new_value_buffers[fixed_width_idx].get()->data_);
                partition_fixed_width_validity_addrs_[fixed_width_idx][partition_id] = nullptr;
                // partition_fixed_width_buffers_[fixed_width_idx][partition_id] 位置0执行bitmap,位置1指向数据
                partition_fixed_width_buffers_[fixed_width_idx][partition_id] = {
                    std::move(new_validity_buffers[fixed_width_idx]),
                    std::move(new_value_buffers[fixed_width_idx])};
                fixed_width_idx++;
                break;
            }
            case SHUFFLE_BINARY:
            default: {
                break;
            }
        }
    }

    partition_buffer_size_[partition_id] = new_size;
    return 0;
}

int Splitter::SplitFixedWidthValueBuffer(VectorBatch& vb) {
    const auto num_rows = vb.GetRowCount();
    for (uint col = 0; col < fixed_width_array_idx_.size(); ++col) {
        memset_s(partition_buffer_idx_offset_, num_partitions_ * sizeof(int32_t), 0, num_partitions_ * sizeof(int32_t));
        auto col_idx_vb = fixed_width_array_idx_[col];
        auto col_idx_schema = singlePartitionFlag ? col_idx_vb : (col_idx_vb - 1);
        const auto& dst_addrs =  partition_fixed_width_value_addrs_[col];
        if (vb.Get(col_idx_vb)->GetEncoding() == OMNI_DICTIONARY) {
            LogsDebug("Dictionary Columnar process!");

            auto ids_addr = VectorHelper::UnsafeGetValues(vb.Get(col_idx_vb));
            auto src_addr = reinterpret_cast<int64_t>(VectorHelper::UnsafeGetDictionary(vb.Get(col_idx_vb)));
            switch (column_type_id_[col_idx_schema]) {
#define PROCESS(SHUFFLE_TYPE, CTYPE)                                                             \
    case SHUFFLE_TYPE:                                                                           \
        {                                                                                        \
            auto shuffle_size = (1 << SHUFFLE_TYPE);                                             \
            for (auto row = 0; row < num_rows; ++row) {                                          \
                auto pid = partition_id_[row];                                                   \
                auto dst_offset =                                                                \
                    partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid];         \
                reinterpret_cast<CTYPE*>(dst_addrs[pid])[dst_offset] =                           \
                reinterpret_cast<CTYPE*>(src_addr)[reinterpret_cast<int32_t *>(ids_addr)[row]];  \
                partition_fixed_width_buffers_[col][pid][1]->size_ += shuffle_size;              \
                partition_buffer_idx_offset_[pid]++;                                             \
            }                                                                                    \
        }                                                                                        \
    break;
                PROCESS(SHUFFLE_1BYTE, uint8_t)
                PROCESS(SHUFFLE_2BYTE, uint16_t)
                PROCESS(SHUFFLE_4BYTE, uint32_t)
                PROCESS(SHUFFLE_8BYTE, uint64_t)
#undef PROCESS
                case SHUFFLE_DECIMAL128:
                    {
                        auto shuffle_size = (1 << SHUFFLE_DECIMAL128);
                        for (auto row = 0; row < num_rows; ++row) {
                            auto pid =  partition_id_[row];
                            auto dst_offset =
                                    partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid];
                            // 前64位取值、赋值
                            reinterpret_cast<uint64_t*>(dst_addrs[pid])[dst_offset << 1] =
                            reinterpret_cast<uint64_t*>(src_addr)[reinterpret_cast<int32_t *>(ids_addr)[row] << 1];
                            // 后64位取值、赋值
                            reinterpret_cast<uint64_t*>(dst_addrs[pid])[(dst_offset << 1) | 1] =
                            reinterpret_cast<uint64_t*>(src_addr)[(reinterpret_cast<int32_t *>(ids_addr)[row] << 1) | 1];
                            partition_fixed_width_buffers_[col][pid][1]->size_ += shuffle_size; //decimal128 16Bytes
                            partition_buffer_idx_offset_[pid]++;
                        }
                    }
                    break;
                default: {
                    LogsError("SplitFixedWidthValueBuffer not match this type: %d", column_type_id_[col_idx_schema]);
                    throw std::runtime_error("SplitFixedWidthValueBuffer not match this type: " + column_type_id_[col_idx_schema]);
                }
            }
        } else {
            auto src_addr = reinterpret_cast<int64_t>(VectorHelper::UnsafeGetValues(vb.Get(col_idx_vb)));
            switch (column_type_id_[col_idx_schema]) {
#define PROCESS(SHUFFLE_TYPE, CTYPE)                                                           \
    case SHUFFLE_TYPE:                                                                         \
        {                                                                                      \
            auto shuffle_size = (1 << SHUFFLE_TYPE);                                           \
            for (auto row = 0; row < num_rows; ++row) {                                        \
                auto pid = partition_id_[row];                                                 \
                auto dst_offset =                                                              \
                    partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid];       \
                reinterpret_cast<CTYPE*>(dst_addrs[pid])[dst_offset] =                         \
                    reinterpret_cast<CTYPE*>(src_addr)[row];                                   \
                partition_fixed_width_buffers_[col][pid][1]->size_ += shuffle_size;            \
                partition_buffer_idx_offset_[pid]++;                                           \
            }                                                                                  \
        }                                                                                      \
    break;
                PROCESS(SHUFFLE_1BYTE, uint8_t)
                PROCESS(SHUFFLE_2BYTE, uint16_t)
                PROCESS(SHUFFLE_4BYTE, uint32_t)
                PROCESS(SHUFFLE_8BYTE, uint64_t)
#undef PROCESS
                case SHUFFLE_DECIMAL128:
                    {
                        auto shuffle_size = (1 << SHUFFLE_DECIMAL128);
                        for (auto row = 0; row < num_rows; ++row) {
                            auto pid =  partition_id_[row];
                            auto dst_offset =
                                    partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid];
                            reinterpret_cast<uint64_t*>(dst_addrs[pid])[dst_offset << 1] =
                                    reinterpret_cast<uint64_t*>(src_addr)[row << 1]; // 前64位取值、赋值
                            reinterpret_cast<uint64_t*>(dst_addrs[pid])[(dst_offset << 1) | 1] =
                                    reinterpret_cast<uint64_t*>(src_addr)[(row << 1) | 1]; // 后64位取值、赋值
                            partition_fixed_width_buffers_[col][pid][1]->size_ += shuffle_size; //decimal128 16Bytes
                            partition_buffer_idx_offset_[pid]++;
                        }
                    }
                    break;
                default: {
                    LogsError("ERROR: SplitFixedWidthValueBuffer not match this type: %d", column_type_id_[col_idx_schema]);
                    throw std::runtime_error("ERROR: SplitFixedWidthValueBuffer not match this type: " + column_type_id_[col_idx_schema]);
                }
            }
        }
    }
    return 0;
}

void HandleNull(VCBatchInfo &vcbInfo, bool isNull) {
    if(isNull) {
        vcbInfo.SetNullFlag(isNull);
    }
}

template<bool hasNull>
void Splitter::SplitBinaryVector(BaseVector *varcharVector, int col_schema) {
    int32_t num_rows = varcharVector->GetSize();
    bool is_null = false;
    if (varcharVector->GetEncoding() == OMNI_DICTIONARY) {
        auto vc = reinterpret_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(
                varcharVector);
	cached_vectorbatch_size_ += num_rows * (sizeof(bool) + sizeof(int32_t));
        for (auto row = 0; row < num_rows; ++row) {
            auto pid = partition_id_[row];
            uint8_t *dst = nullptr;
            uint32_t str_len = 0;
            if (!vc->IsNull(row)) {
                std::string_view value = vc->GetValue(row);
                dst = reinterpret_cast<uint8_t *>(reinterpret_cast<int64_t>(value.data()));
                str_len = static_cast<uint32_t>(value.length());
            }
            if constexpr (hasNull) {
                is_null = vc->IsNull(row);
            }
            cached_vectorbatch_size_ += str_len; // 累计变长部分cache数据
            VCLocation cl((uint64_t) dst, str_len, is_null);
            if ((vc_partition_array_buffers_[pid][col_schema].size() != 0) &&
                (vc_partition_array_buffers_[pid][col_schema].back().getVcList().size() <
                options_.spill_batch_row_num)) {
                if constexpr(hasNull) {
                    HandleNull(vc_partition_array_buffers_[pid][col_schema].back(), is_null);
                }
                vc_partition_array_buffers_[pid][col_schema].back().getVcList().push_back(cl);
                vc_partition_array_buffers_[pid][col_schema].back().vcb_total_len += str_len;
            } else {
                VCBatchInfo svc(options_.spill_batch_row_num);
                svc.getVcList().push_back(cl);
                svc.vcb_total_len += str_len;
                if constexpr (hasNull) {
                    HandleNull(svc, is_null);
                }
                vc_partition_array_buffers_[pid][col_schema].push_back(svc);
            }
        }
    } else {
        auto vc = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(varcharVector);
        cached_vectorbatch_size_ += num_rows * (sizeof(bool) + sizeof(int32_t)) + sizeof(int32_t);
	for (auto row = 0; row < num_rows; ++row) {
            auto pid = partition_id_[row];
            uint8_t *dst = nullptr;
            uint32_t str_len = 0;
            if (!vc->IsNull(row)) {
                std::string_view value = vc->GetValue(row);
                dst = reinterpret_cast<uint8_t *>(reinterpret_cast<int64_t>(value.data()));
                str_len = static_cast<uint32_t>(value.length());
            }

            if constexpr (hasNull) {
                is_null = vc->IsNull(row);
            }
            cached_vectorbatch_size_ += str_len; // 累计变长部分cache数据
            VCLocation cl((uint64_t) dst, str_len, is_null);
            if ((vc_partition_array_buffers_[pid][col_schema].size() != 0) &&
                (vc_partition_array_buffers_[pid][col_schema].back().getVcList().size() <
                options_.spill_batch_row_num)) {
                if constexpr(hasNull) {
                    HandleNull(vc_partition_array_buffers_[pid][col_schema].back(), is_null);
                }
                vc_partition_array_buffers_[pid][col_schema].back().getVcList().push_back(cl);
                vc_partition_array_buffers_[pid][col_schema].back().vcb_total_len += str_len;
            } else {
                VCBatchInfo svc(options_.spill_batch_row_num);
                svc.getVcList().push_back(cl);
                if constexpr(hasNull) {
                    HandleNull(svc, is_null);
                }
                svc.vcb_total_len += str_len;
                vc_partition_array_buffers_[pid][col_schema].push_back(svc);
            }
        }
    }
}

int Splitter::SplitBinaryArray(VectorBatch& vb)
{
    auto vec_cnt_vb = vb.GetVectorCount();
    auto vec_cnt_schema = singlePartitionFlag ? vec_cnt_vb : vec_cnt_vb - 1;
    for (auto col_schema = 0; col_schema < vec_cnt_schema; ++col_schema) {
        switch (column_type_id_[col_schema]) {
            case SHUFFLE_BINARY: {
                auto col_vb = singlePartitionFlag ? col_schema : col_schema + 1;
                auto *varcharVector = vb.Get(col_vb);
                varcharVectorCache.insert(varcharVector);
                if (varcharVector->HasNull()) {
                    this->template SplitBinaryVector<true>(varcharVector, col_schema);
                } else {
                    this->template SplitBinaryVector<false>(varcharVector, col_schema);
                }
                break;
            }
            case SHUFFLE_LARGE_BINARY:
                break;
            default:{
                break;
            }
        }
    }
    return 0;
}

int Splitter::SplitFixedWidthValidityBuffer(VectorBatch& vb){
    for (uint col = 0; col < fixed_width_array_idx_.size(); ++col) {
        auto col_idx = fixed_width_array_idx_[col];
        auto& dst_addrs = partition_fixed_width_validity_addrs_[col];
        // 分配内存并初始化
        if (vb.Get(col_idx)->HasNull()) {
            for (auto pid = 0; pid < num_partitions_; ++pid) {
                if (partition_id_cnt_cur_[pid] > 0 && dst_addrs[pid] == nullptr) {
                    // init bitmap if it's null
                    auto new_size = partition_id_cnt_cur_[pid] > options_.buffer_size ? partition_id_cnt_cur_[pid] : options_.buffer_size;
                    auto ptr_tmp = static_cast<uint8_t *>(options_.allocator->Alloc(new_size));
                    if (nullptr == ptr_tmp) {
                        throw std::runtime_error("Allocator for ValidityBuffer Failed! ");
                    }
                    std::shared_ptr<Buffer> validity_buffer (
                        new Buffer((uint8_t *)ptr_tmp, partition_id_cnt_cur_[pid], new_size));
                    dst_addrs[pid] = const_cast<uint8_t*>(validity_buffer->data_);
                    memset_s(validity_buffer->data_, new_size, 0, new_size);
                    partition_fixed_width_buffers_[col][pid][0] = std::move(validity_buffer);
                    fixed_nullBuffer_size_[pid] += new_size;
                }
            }

            // 计算并填充数据
            auto src_addr = const_cast<uint8_t *>((uint8_t *)(
                    reinterpret_cast<int64_t>(omniruntime::vec::unsafe::UnsafeBaseVector::GetNulls(vb.Get(col_idx)))));
            memset_s(partition_buffer_idx_offset_, num_partitions_ * sizeof(int32_t), 0, num_partitions_ * sizeof(int32_t));
            const auto num_rows = vb.GetRowCount();
            for (auto row = 0; row < num_rows; ++row) {
                auto pid = partition_id_[row];
                auto dst_offset = partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid];
                dst_addrs[pid][dst_offset] = src_addr[row];
                partition_buffer_idx_offset_[pid]++;
            }
        }
    }
    return 0;
}

int Splitter::CacheVectorBatch(int32_t partition_id, bool reset_buffers) {
    if (partition_buffer_idx_base_[partition_id] > 0 && fixed_width_array_idx_.size() > 0) {
        auto fixed_width_idx = 0;
        auto num_fields = num_fields_;
        int64_t batch_partition_size = 0;
        std::vector<std::vector<std::shared_ptr<Buffer>>> bufferArrayTotal(num_fields);

        for (int i = 0; i < num_fields; ++i) {
            switch (column_type_id_[i]) {
                case SHUFFLE_BINARY: {
                    break;
                }
                case SHUFFLE_LARGE_BINARY: {
                    break;
                }
                case SHUFFLE_NULL: {
                    break;
                }
                default: {
                    auto& buffers = partition_fixed_width_buffers_[fixed_width_idx][partition_id];
                    if (buffers[0] != nullptr) {
                        batch_partition_size += buffers[0]->capacity_; // 累计null数组所占内存大小
                    }
                    batch_partition_size += buffers[1]->capacity_; // 累计value数组所占内存大小
                    if (reset_buffers) {
                        bufferArrayTotal[fixed_width_idx] = std::move(buffers);
                        buffers = {nullptr};
                        partition_fixed_width_validity_addrs_[fixed_width_idx][partition_id] = nullptr;
                        partition_fixed_width_value_addrs_[fixed_width_idx][partition_id] = nullptr;
                    } else {
                        bufferArrayTotal[fixed_width_idx] = buffers;
                    }
                    fixed_width_idx++;
                    break;
                }
            }
        }
	cached_vectorbatch_size_ += batch_partition_size;
	partition_cached_vectorbatch_[partition_id].push_back(std::move(bufferArrayTotal));
	fixed_valueBuffer_size_[partition_id] = 0;
	fixed_nullBuffer_size_[partition_id] = 0;
	partition_buffer_idx_base_[partition_id] = 0;
    }
    return 0;
}

int Splitter::DoSplit(VectorBatch& vb) {
    // prepare partition buffers and spill if necessary
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        if (fixed_width_array_idx_.size() > 0 &&
            partition_id_cnt_cur_[pid] > 0 &&
            partition_buffer_idx_base_[pid] + partition_id_cnt_cur_[pid] > partition_buffer_size_[pid]) {
            auto new_size = partition_id_cnt_cur_[pid] > options_.buffer_size ? partition_id_cnt_cur_[pid] : options_.buffer_size;
            if (partition_buffer_size_[pid] == 0) { // first allocate?
                AllocatePartitionBuffers(pid, new_size);
            } else { // not first allocate, spill
                    CacheVectorBatch(pid, true);
                    AllocatePartitionBuffers(pid, new_size);
            }
        }
    }
    SplitFixedWidthValueBuffer(vb);
    SplitFixedWidthValidityBuffer(vb);

    current_fixed_alloc_buffer_size_ = 0; // 用于统计定长split但未cache部分内存大小
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        // update partition buffer base
        partition_buffer_idx_base_[pid] += partition_id_cnt_cur_[pid];
        current_fixed_alloc_buffer_size_ += fixed_valueBuffer_size_[pid];
        current_fixed_alloc_buffer_size_ += fixed_nullBuffer_size_[pid];
    }

    // Binary split last vector batch...
    SplitBinaryArray(vb);
    num_row_splited_ += vb.GetRowCount();
    // release the fixed width vector and release vectorBatch at the same time
    ReleaseVectorBatch(&vb);
    this->ResetInputVecBatch();

    // spill
    // process level: If the memory usage of the current executor exceeds the threshold, spill is triggered.
    uint64_t usedMemorySize = omniruntime::mem::MemoryManager::GetGlobalAccountedMemory();
    if (usedMemorySize > options_.executor_spill_mem_threshold) {
        LogsDebug(" Spill For Executor Memory Size Threshold.");
        TIME_NANO_OR_RAISE(total_spill_time_, SpillToTmpFile());
        isSpill = true;
    }

    // task level: If the memory usage of the current task exceeds the threshold, spill is triggered.
    if (cached_vectorbatch_size_ + current_fixed_alloc_buffer_size_ >= options_.task_spill_mem_threshold) {
        LogsDebug(" Spill For Task Memory Size Threshold.");
        TIME_NANO_OR_RAISE(total_spill_time_, SpillToTmpFile());
        isSpill = true;
    }
    return 0;
}

void Splitter::ToSplitterTypeId(int num_cols)
{
    for (int i = 0; i < num_cols; ++i) {
        switch (input_col_types.inputVecTypeIds[i]) {
            case OMNI_BOOLEAN: {
                CastOmniToShuffleType(OMNI_BOOLEAN, SHUFFLE_1BYTE);
                break;
            }
            case OMNI_SHORT: {
                CastOmniToShuffleType(OMNI_SHORT, SHUFFLE_2BYTE);
                break;
            }
            case OMNI_INT: {
                CastOmniToShuffleType(OMNI_INT, SHUFFLE_4BYTE);
                break;
            }
            case OMNI_LONG: {
                CastOmniToShuffleType(OMNI_LONG, SHUFFLE_8BYTE);
                break;
            }
            case OMNI_TIMESTAMP: {
                CastOmniToShuffleType(OMNI_TIMESTAMP, SHUFFLE_8BYTE);
                break;
            }
            case OMNI_DOUBLE: {
                CastOmniToShuffleType(OMNI_DOUBLE, SHUFFLE_8BYTE);
                break;
            }
            case OMNI_DATE32: {
                CastOmniToShuffleType(OMNI_DATE32, SHUFFLE_4BYTE);
                break;
            }
            case OMNI_DATE64: {
                CastOmniToShuffleType(OMNI_DATE64, SHUFFLE_8BYTE);
                break;
            }
            case OMNI_DECIMAL64: {
                CastOmniToShuffleType(OMNI_DECIMAL64, SHUFFLE_8BYTE);
                break;
            }
            case OMNI_DECIMAL128: {
                CastOmniToShuffleType(OMNI_DECIMAL128, SHUFFLE_DECIMAL128);
                break;
            }
            case OMNI_CHAR: {
                CastOmniToShuffleType(OMNI_CHAR, SHUFFLE_BINARY);
                break;
            }
            case OMNI_VARCHAR: {
                CastOmniToShuffleType(OMNI_VARCHAR, SHUFFLE_BINARY);
                break;
            }
            default: throw std::runtime_error("Unsupported DataTypeId: " + input_col_types.inputVecTypeIds[i]);
        }
    }
}

void Splitter::CastOmniToShuffleType(DataTypeId omniType, ShuffleTypeId shuffleType)
{
    proto_col_types_.push_back(CastOmniTypeIdToProtoVecType(omniType));
    column_type_id_.push_back(shuffleType);
}

int Splitter::Split_Init(){
    num_row_splited_ = 0;
    cached_vectorbatch_size_ = 0;

    partition_id_cnt_cur_ = new int32_t[num_partitions_]();
    partition_id_cnt_cache_ = new uint64_t[num_partitions_]();
    partition_buffer_size_ = new int32_t[num_partitions_]();
    partition_buffer_idx_base_ = new int32_t[num_partitions_]();
    partition_buffer_idx_offset_ = new int32_t[num_partitions_]();
    partition_serialization_size_ = new uint32_t[num_partitions_]();

    partition_cached_vectorbatch_.resize(num_partitions_);
    fixed_width_array_idx_.clear();
    partition_lengths_.resize(num_partitions_);

    fixed_valueBuffer_size_ = new uint32_t[num_partitions_]();
    fixed_nullBuffer_size_ = new uint32_t[num_partitions_]();

    // obtain configed dir from Environment Variables
    configured_dirs_ = GetConfiguredLocalDirs();
    sub_dir_selection_.assign(configured_dirs_.size(), 0);

    // Both data_file and shuffle_index_file should be set through jni.
    // For test purpose, Create a temporary subdirectory in the system temporary
    // dir with prefix "columnar-shuffle"
    if (options_.data_file.length() == 0) {
        options_.data_file = CreateTempShuffleFile(configured_dirs_[0]);
    }

    for (uint i = 0; i < column_type_id_.size(); ++i) {
        switch (column_type_id_[i]) {
            case ShuffleTypeId::SHUFFLE_1BYTE:
            case ShuffleTypeId::SHUFFLE_2BYTE:
            case ShuffleTypeId::SHUFFLE_4BYTE:
            case ShuffleTypeId::SHUFFLE_8BYTE:
            case ShuffleTypeId::SHUFFLE_DECIMAL128:
                if (singlePartitionFlag) {
                    fixed_width_array_idx_.push_back(i);
                } else {
                    fixed_width_array_idx_.push_back(i + 1);
                }
                break;
            case ShuffleTypeId::SHUFFLE_BINARY:
            default:
                break;
        }
    }
    auto num_fixed_width = fixed_width_array_idx_.size();
    partition_fixed_width_validity_addrs_.resize(num_fixed_width);
    partition_fixed_width_value_addrs_.resize(num_fixed_width);
    partition_fixed_width_buffers_.resize(num_fixed_width);
    for (uint i = 0; i < num_fixed_width; ++i) {
        partition_fixed_width_validity_addrs_[i].resize(num_partitions_);
        partition_fixed_width_value_addrs_[i].resize(num_partitions_);
        partition_fixed_width_buffers_[i].resize(num_partitions_);
    }

    /* init varchar partition */
    vc_partition_array_buffers_.resize(num_partitions_);
    for (auto i = 0; i < num_partitions_; ++i) {
        vc_partition_array_buffers_[i].resize(column_type_id_.size());
    }

    partition_rows.resize(num_partitions_);
    return 0;
}

int Splitter::Split(VectorBatch& vb )
{
    // 计算vectorBatch分区信息
    LogsTrace(" split vb row number: %d ", vb.GetRowCount());
    TIME_NANO_OR_RAISE(total_compute_pid_time_, ComputeAndCountPartitionId(vb));
    // 执行分区动作
    DoSplit(vb);
    return 0;
}

int Splitter::SplitByRow(VectorBatch *vecBatch) {
    int32_t rowCount = vecBatch->GetRowCount();
    for (int pid = 0; pid < num_partitions_; ++pid) {
        auto needCapacity = partition_rows[pid].size() + rowCount;
        if (partition_rows[pid].capacity() < needCapacity) {
            auto prepareCapacity = partition_rows[pid].capacity() * expansion;
            auto newCapacity = prepareCapacity > needCapacity ? prepareCapacity : needCapacity;
            partition_rows[pid].reserve(newCapacity);
        }
    }

    if (singlePartitionFlag) {
        RowBatch *rowBatch = VectorHelper::TransRowBatchFromVectorBatch(vecBatch);
        for (int i = 0; i < rowCount; ++i) {
            RowInfo *rowInfo = rowBatch->Get(i);
            partition_rows[0].emplace_back(rowInfo);
            total_input_size += rowInfo->length;
        }
        delete vecBatch;
    } else {
        auto pidVec = reinterpret_cast<Vector<int32_t> *>(vecBatch->Get(0));
        auto tmpVectorBatch = new VectorBatch(rowCount);
        for (int i = 1; i < vecBatch->GetVectorCount(); ++i) {
            tmpVectorBatch->Append(vecBatch->Get(i));
        }
        vecBatch->ResizeVectorCount(1);
        RowBatch *rowBatch = VectorHelper::TransRowBatchFromVectorBatch(tmpVectorBatch);
        for (int i = 0; i < rowCount; ++i) {
            auto pid = pidVec->GetValue(i);
            RowInfo *rowInfo = rowBatch->Get(i);
            partition_rows[pid].emplace_back(rowInfo);
            total_input_size += rowInfo->length;
        }
        delete vecBatch;
        delete tmpVectorBatch;
    }

    // spill
    // process level: If the memory usage of the current executor exceeds the threshold, spill is triggered.
    uint64_t usedMemorySize = omniruntime::mem::MemoryManager::GetGlobalAccountedMemory();
    if (usedMemorySize > options_.executor_spill_mem_threshold) {
        TIME_NANO_OR_RAISE(total_spill_time_, SpillToTmpFileByRow());
        total_input_size = 0;
        isSpill = true;
    }

    // task level: If the memory usage of the current task exceeds the threshold, spill is triggered.
    if (total_input_size > options_.task_spill_mem_threshold) {
        TIME_NANO_OR_RAISE(total_spill_time_, SpillToTmpFileByRow());
        total_input_size = 0;
        isSpill = true;
    }
    return 0;
}

std::shared_ptr<Buffer> Splitter::CaculateSpilledTmpFilePartitionOffsets() {
    void *ptr_tmp = static_cast<void *>(options_.allocator->Alloc((num_partitions_ + 1) * sizeof(uint64_t)));
    if (nullptr == ptr_tmp) {
        throw std::runtime_error("Allocator for partitionOffsets Failed! ");
    }
    std::shared_ptr<Buffer> ptrPartitionOffsets (new Buffer((uint8_t*)ptr_tmp, 0, (num_partitions_ + 1) * sizeof(uint64_t)));
    uint64_t pidOffset = 0;
    // 顺序记录每个partition的offset
    auto pid = 0;
    for (pid = 0; pid < num_partitions_; ++pid) {
        reinterpret_cast<uint64_t *>(ptrPartitionOffsets->data_)[pid] = pidOffset;
        pidOffset += partition_serialization_size_[pid];
        // reset partition_cached_vectorbatch_size_ to 0
        partition_serialization_size_[pid] = 0;
    }
    reinterpret_cast<uint64_t *>(ptrPartitionOffsets->data_)[pid] = pidOffset;
    return ptrPartitionOffsets;
}

spark::VecType::VecTypeId Splitter::CastOmniTypeIdToProtoVecType(int32_t omniType) {
    switch (omniType) {
        case OMNI_NONE:
            return spark::VecType::VEC_TYPE_NONE;
        case OMNI_INT:
            return spark::VecType::VEC_TYPE_INT;
        case OMNI_LONG:
            return spark::VecType::VEC_TYPE_LONG;
        case OMNI_DOUBLE:
            return spark::VecType::VEC_TYPE_DOUBLE;
        case OMNI_BOOLEAN:
            return spark::VecType::VEC_TYPE_BOOLEAN;
        case OMNI_SHORT:
            return spark::VecType::VEC_TYPE_SHORT;
        case OMNI_DECIMAL64:
            return spark::VecType::VEC_TYPE_DECIMAL64;
        case OMNI_DECIMAL128:
            return spark::VecType::VEC_TYPE_DECIMAL128;
        case OMNI_DATE32:
            return spark::VecType::VEC_TYPE_DATE32;
        case OMNI_DATE64:
            return spark::VecType::VEC_TYPE_DATE64;
        case OMNI_TIME32:
            return spark::VecType::VEC_TYPE_TIME32;
        case OMNI_TIME64:
            return spark::VecType::VEC_TYPE_TIME64;
        case OMNI_TIMESTAMP:
            return spark::VecType::VEC_TYPE_TIMESTAMP;
        case OMNI_INTERVAL_MONTHS:
            return spark::VecType::VEC_TYPE_INTERVAL_MONTHS;
        case OMNI_INTERVAL_DAY_TIME:
            return spark::VecType::VEC_TYPE_INTERVAL_DAY_TIME;
        case OMNI_VARCHAR:
            return spark::VecType::VEC_TYPE_VARCHAR;
        case OMNI_CHAR:
            return spark::VecType::VEC_TYPE_CHAR;
        case OMNI_CONTAINER:
            return spark::VecType::VEC_TYPE_CONTAINER;
        case DataTypeId::OMNI_INVALID:
            return spark::VecType::VEC_TYPE_INVALID;
        default: {
            throw std::runtime_error("CastOmniTypeIdToProtoVecType() unexpected OmniTypeId");
        }
    }
};

void Splitter::SerializingFixedColumns(int32_t partitionId,
                                      spark::Vec& vec,
                                      int fixColIndexTmp,
                                      SplitRowInfo* splitRowInfoTmp)
{
    LogsDebug(" Fix col :%d th, partition_cached_vectorbatch_[%d].size: %ld", fixColIndexTmp, partitionId, partition_cached_vectorbatch_[partitionId].size());
    if (splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp] < partition_cached_vectorbatch_[partitionId].size()) {
        auto colIndexTmpSchema = 0;
        colIndexTmpSchema = singlePartitionFlag ? fixed_width_array_idx_[fixColIndexTmp] : fixed_width_array_idx_[fixColIndexTmp] - 1;
        auto onceCopyLen = splitRowInfoTmp->onceCopyRow * (1 << column_type_id_[colIndexTmpSchema]);
        // 临时内存，拷贝拼接onceCopyRow批，用完释放
        std::string valueStr;
        valueStr.resize(onceCopyLen);
        std::string nullStr;

        std::shared_ptr<Buffer> ptr_value (new Buffer((uint8_t*)valueStr.data(), 0, onceCopyLen, false));
        std::shared_ptr<Buffer> ptr_validity;

        // options_.spill_batch_row_num长度切割与拼接
        uint destCopyedLength = 0;
        uint memCopyLen = 0;
        uint cacheBatchSize = 0;
        bool nullAllocated = false;
        while (destCopyedLength < onceCopyLen) {
            if (splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp] >= partition_cached_vectorbatch_[partitionId].size()) { // 数组越界保护
                throw std::runtime_error("Columnar shuffle CacheBatchIndex out of bound.");
            }
            cacheBatchSize = partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][1]->size_;
            LogsDebug(" partitionId:%d  splitRowInfoTmp.cacheBatchIndex[%d]:%d  cacheBatchSize:%d  onceCopyLen:%d  destCopyedLength:%d  splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp]:%d ",
                      partitionId,
                      fixColIndexTmp,
                      splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp],
                      cacheBatchSize,
                      onceCopyLen,
                      destCopyedLength,
                      splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp]);
            if (not nullAllocated && partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][0] != nullptr) {
                nullStr.resize(splitRowInfoTmp->onceCopyRow);
                ptr_validity.reset(new Buffer((uint8_t*)nullStr.data(), 0, splitRowInfoTmp->onceCopyRow, false));
                nullAllocated = true;
            }
            if ((onceCopyLen - destCopyedLength) >= (cacheBatchSize - splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp])) {
                memCopyLen = cacheBatchSize - splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp];
                memcpy_s((uint8_t*)(ptr_value->data_) + destCopyedLength,
                       memCopyLen,
                       partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][1]->data_ + splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp],
                       memCopyLen);
                // (destCopyedLength / (1 << column_type_id_[colIndexTmpSchema])) 等比例计算null数组偏移
                if (partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][0] != nullptr) {
                    memcpy_s((uint8_t*)(ptr_validity->data_) + (destCopyedLength / (1 << column_type_id_[colIndexTmpSchema])),
                           memCopyLen / (1 << column_type_id_[colIndexTmpSchema]),
                           partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][0]->data_ + (splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp] / (1 << column_type_id_[colIndexTmpSchema])),
                           memCopyLen / (1 << column_type_id_[colIndexTmpSchema]));
                    // 释放内存
                    options_.allocator->Free(partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][0]->data_,
                                             partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][0]->capacity_);
                    partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][0]->SetReleaseFlag();
                }
                options_.allocator->Free(partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][1]->data_,
                                         partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][1]->capacity_);
                partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][1]->SetReleaseFlag();
                destCopyedLength += memCopyLen;
                splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp] += 1; // cacheBatchIndex下标后移
                splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp] = 0; // 初始化下一个cacheBatch的起始偏移
            } else {
                memCopyLen = onceCopyLen - destCopyedLength;
                memcpy_s((uint8_t*)(ptr_value->data_) + destCopyedLength,
                    memCopyLen,
                    partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][1]->data_ + splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp],
                    memCopyLen);
                // (destCopyedLength / (1 << column_type_id_[colIndexTmpSchema])) 等比例计算null数组偏移

                if(partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][0] != nullptr) {
                    memcpy_s((uint8_t*)(ptr_validity->data_) + (destCopyedLength / (1 << column_type_id_[colIndexTmpSchema])),
                                        memCopyLen / (1 << column_type_id_[colIndexTmpSchema]),
                                        partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][0]->data_ + (splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp] / (1 << column_type_id_[colIndexTmpSchema])),
                                        memCopyLen / (1 << column_type_id_[colIndexTmpSchema]));
                }
                destCopyedLength = onceCopyLen; // copy目标完成，结束while循环
                splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp] += memCopyLen;
            }
            LogsDebug("  memCopyedLen=%d, splitRowInfoTmp.cacheBatchIndex[fix_col%d]=%d   splitRowInfoTmp.cacheBatchCopyedLen[fix_col%d]=%d ",
                    memCopyLen,
                    fixColIndexTmp,
                    splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp],
                    fixColIndexTmp,
                    splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp]);
        }
        auto *protoValue = vec.mutable_values();
        *protoValue = std::move(valueStr);
        auto *protoNulls = vec.mutable_nulls();
        *protoNulls = std::move(nullStr);
        // 临时内存，拷贝拼接onceCopyRow批，用完释放
    }
    // partition_cached_vectorbatch_[partition_id][cache_index][col][0]代表ByteMap,
    // partition_cached_vectorbatch_[partition_id][cache_index][col][1]代表value
}

void Splitter::SerializingBinaryColumns(int32_t partitionId, spark::Vec& vec, int colIndex, int curBatch)
{
    LogsDebug(" vc_partition_array_buffers_[partitionId:%d][colIndex:%d] cacheBatchNum:%lu curBatch:%d", partitionId, colIndex, vc_partition_array_buffers_[partitionId][colIndex].size(), curBatch);
    VCBatchInfo vcb = vc_partition_array_buffers_[partitionId][colIndex][curBatch];
    int valuesTotalLen = vcb.getVcbTotalLen();
    std::vector<VCLocation> lst = vcb.getVcList();
    int itemsTotalLen = lst.size();

    std::string offsetsStr;
    offsetsStr.resize(sizeof(int32_t) * (itemsTotalLen + 1));
    std::string nullsStr;
    std::string valuesStr;
    valuesStr.resize(valuesTotalLen);
    if(vcb.hasNull()) {
        BytesGen<true>(reinterpret_cast<uint64_t>(offsetsStr.data()),
                 nullsStr,
                 reinterpret_cast<uint64_t>(valuesStr.data()), vcb);
    } else {
        BytesGen<false>(reinterpret_cast<uint64_t>(offsetsStr.data()),
                         nullsStr,
                         reinterpret_cast<uint64_t>(valuesStr.data()), vcb);
    }
    auto *protoValue = vec.mutable_values();
    *protoValue = std::move(valuesStr);
    auto *protoNulls = vec.mutable_nulls();
    *protoNulls = std::move(nullsStr);
    auto *protoOffset = vec.mutable_offset();
    *protoOffset = std::move(offsetsStr);
}

int32_t Splitter::ProtoWritePartition(int32_t partition_id, std::unique_ptr<BufferedOutputStream> &bufferStream, void *bufferOut, int32_t &sizeOut) {
    SplitRowInfo splitRowInfoTmp;
    splitRowInfoTmp.copyedRow = 0;
    splitRowInfoTmp.remainCopyRow = partition_id_cnt_cache_[partition_id];
    splitRowInfoTmp.cacheBatchIndex.resize(fixed_width_array_idx_.size());
    splitRowInfoTmp.cacheBatchCopyedLen.resize(fixed_width_array_idx_.size());

    int curBatch = 0;
    while (0 < splitRowInfoTmp.remainCopyRow) {
        if (options_.spill_batch_row_num < splitRowInfoTmp.remainCopyRow) {
            splitRowInfoTmp.onceCopyRow = options_.spill_batch_row_num;
        } else {
            splitRowInfoTmp.onceCopyRow = splitRowInfoTmp.remainCopyRow;
        }

        vecBatchProto->set_rowcnt(splitRowInfoTmp.onceCopyRow);
        vecBatchProto->set_veccnt(column_type_id_.size());
        int fixColIndexTmp = 0;
        for (size_t indexSchema = 0; indexSchema < column_type_id_.size(); indexSchema++) {
            spark::Vec *vec = vecBatchProto->add_vecs();
            switch (column_type_id_[indexSchema]) {
                case ShuffleTypeId::SHUFFLE_1BYTE:
                case ShuffleTypeId::SHUFFLE_2BYTE:
                case ShuffleTypeId::SHUFFLE_4BYTE:
                case ShuffleTypeId::SHUFFLE_8BYTE:
                case ShuffleTypeId::SHUFFLE_DECIMAL128: {
                    SerializingFixedColumns(partition_id, *vec, fixColIndexTmp, &splitRowInfoTmp);
                    fixColIndexTmp++; // 定长序列化数量++
                    break;
                }
                case ShuffleTypeId::SHUFFLE_BINARY: {
                    SerializingBinaryColumns(partition_id, *vec, indexSchema, curBatch);
                    break;
                }
                default: {
                    throw std::runtime_error("Unsupported ShuffleType.");
                }
            }
            spark::VecType *vt = vec->mutable_vectype();
            vt->set_typeid_(proto_col_types_[indexSchema]);
            if(vt->typeid_() == spark::VecType::VEC_TYPE_DECIMAL128 || vt->typeid_() == spark::VecType::VEC_TYPE_DECIMAL64){
                vt->set_precision(input_col_types.inputDataPrecisions[indexSchema]);
                vt->set_scale(input_col_types.inputDataScales[indexSchema]);
                 LogsDebug("precision[indexSchema %d]: %d , scale[indexSchema %d]: %d ",
                          indexSchema, input_col_types.inputDataPrecisions[indexSchema],
                          indexSchema, input_col_types.inputDataScales[indexSchema]);
            }
        }
        curBatch++;

        if (vecBatchProto->ByteSizeLong() > UINT32_MAX) {
            throw std::runtime_error("Unsafe static_cast long to uint_32t.");
        }
        uint32_t vecBatchProtoSize = reversebytes_uint32t(static_cast<uint32_t>(vecBatchProto->ByteSizeLong()));
        if (bufferStream->Next(&bufferOut, &sizeOut)) {
            memcpy_s(bufferOut, sizeof(vecBatchProtoSize), &vecBatchProtoSize, sizeof(vecBatchProtoSize));
            if (sizeof(vecBatchProtoSize) < static_cast<uint32_t>(sizeOut)) {
                bufferStream->BackUp(sizeOut - sizeof(vecBatchProtoSize));
            }
        }

        vecBatchProto->SerializeToZeroCopyStream(bufferStream.get());
        splitRowInfoTmp.remainCopyRow -= splitRowInfoTmp.onceCopyRow;
        splitRowInfoTmp.copyedRow += splitRowInfoTmp.onceCopyRow;
        vecBatchProto->Clear();
    }

    uint64_t partitionBatchSize = bufferStream->flush();
    total_bytes_written_ += partitionBatchSize;
    partition_lengths_[partition_id] += partitionBatchSize;
    LogsDebug(" partitionBatch write length: %lu", partitionBatchSize);

   // 及时清理分区数据
    partition_cached_vectorbatch_[partition_id].clear(); // 定长数据内存释放
    for (size_t col = 0; col < column_type_id_.size(); col++) {
        vc_partition_array_buffers_[partition_id][col].clear(); // binary 释放内存
    }

    return 0;
}

int32_t Splitter::ProtoWritePartitionByRow(int32_t partition_id, std::unique_ptr<BufferedOutputStream> &bufferStream, void *bufferOut, int32_t &sizeOut) {
    uint64_t rowCount = partition_rows[partition_id].size();
    uint64_t onceCopyRow = 0;
    uint32_t batchCount = 0;
    while (0 < rowCount) {
        if (options_.spill_batch_row_num < rowCount) {
            onceCopyRow = options_.spill_batch_row_num;
        } else {
            onceCopyRow = rowCount;
        }

        protoRowBatch->set_rowcnt(onceCopyRow);
        protoRowBatch->set_veccnt(proto_col_types_.size());
        for (uint32_t i = 0; i < proto_col_types_.size(); ++i) {
            spark::VecType *vt = protoRowBatch->add_vectypes();
            vt->set_typeid_(proto_col_types_[i]);
            if(vt->typeid_() == spark::VecType::VEC_TYPE_DECIMAL128 || vt->typeid_() == spark::VecType::VEC_TYPE_DECIMAL64){
                vt->set_precision(input_col_types.inputDataPrecisions[i]);
                vt->set_scale(input_col_types.inputDataScales[i]);
                LogsDebug("precision[indexSchema %d]: %d , scale[indexSchema %d]: %d ",
                          i, input_col_types.inputDataPrecisions[i],
                          i, input_col_types.inputDataScales[i]);
            }
        }

        int64_t offset = batchCount * options_.spill_batch_row_num;
        auto rowInfoPtr = partition_rows[partition_id].data() + offset;
        for (uint64_t i = 0; i < onceCopyRow; ++i) {
            RowInfo *rowInfo = rowInfoPtr[i];
            spark::ProtoRow *protoRow = protoRowBatch->add_rows();
            protoRow->set_data(rowInfo->row, rowInfo->length);
            protoRow->set_length(rowInfo->length);
            // free row memory
            delete rowInfo;
        }

        if (protoRowBatch->ByteSizeLong() > UINT32_MAX) {
            throw std::runtime_error("Unsafe static_cast long to uint_32t.");
        }
        uint32_t protoRowBatchSize = reversebytes_uint32t(static_cast<uint32_t>(protoRowBatch->ByteSizeLong()));
        if (bufferStream->Next(&bufferOut, &sizeOut)) {
            memcpy_s(bufferOut, sizeof(protoRowBatchSize), &protoRowBatchSize, sizeof(protoRowBatchSize));
            if (sizeof(protoRowBatchSize) < static_cast<uint32_t>(sizeOut)) {
                bufferStream->BackUp(sizeOut - sizeof(protoRowBatchSize));
            }
        }

        protoRowBatch->SerializeToZeroCopyStream(bufferStream.get());
        rowCount -= onceCopyRow;
        batchCount++;
        protoRowBatch->Clear();
    }

    uint64_t partitionBatchSize = bufferStream->flush();
    total_bytes_written_ += partitionBatchSize;
    partition_lengths_[partition_id] += partitionBatchSize;
    partition_rows[partition_id].clear();
    LogsDebug(" partitionBatch write length: %lu", partitionBatchSize);
    return 0;
}

int Splitter::protoSpillPartition(int32_t partition_id, std::unique_ptr<BufferedOutputStream> &bufferStream) {
    SplitRowInfo splitRowInfoTmp;
    splitRowInfoTmp.copyedRow = 0;
    splitRowInfoTmp.remainCopyRow = partition_id_cnt_cache_[partition_id];
    splitRowInfoTmp.cacheBatchIndex.resize(fixed_width_array_idx_.size());
    splitRowInfoTmp.cacheBatchCopyedLen.resize(fixed_width_array_idx_.size());
    LogsDebug(" Spill Pid %d , remainCopyRow %d , partition_cache_batch_num %lu .",
               partition_id,
               splitRowInfoTmp.remainCopyRow,
               partition_cached_vectorbatch_[partition_id].size());
    int curBatch = 0; // 变长cache batch下标，split已按照options_.spill_batch_row_num切割完成
    total_spill_row_num_ += splitRowInfoTmp.remainCopyRow;
    while (0 < splitRowInfoTmp.remainCopyRow) {
        if (options_.spill_batch_row_num < splitRowInfoTmp.remainCopyRow) {
            splitRowInfoTmp.onceCopyRow = options_.spill_batch_row_num;
        } else {
            splitRowInfoTmp.onceCopyRow = splitRowInfoTmp.remainCopyRow;
        }

        vecBatchProto->set_rowcnt(splitRowInfoTmp.onceCopyRow);
        vecBatchProto->set_veccnt(column_type_id_.size());
        int fixColIndexTmp = 0;
        for (size_t indexSchema = 0; indexSchema < column_type_id_.size(); indexSchema++) {
            spark::Vec *vec = vecBatchProto->add_vecs();
            switch (column_type_id_[indexSchema]) {
                case ShuffleTypeId::SHUFFLE_1BYTE:
                case ShuffleTypeId::SHUFFLE_2BYTE:
                case ShuffleTypeId::SHUFFLE_4BYTE:
                case ShuffleTypeId::SHUFFLE_8BYTE:
                case ShuffleTypeId::SHUFFLE_DECIMAL128: {
                    SerializingFixedColumns(partition_id, *vec, fixColIndexTmp, &splitRowInfoTmp);
                    fixColIndexTmp++; // 定长序列化数量++
                    break;
                }
                case ShuffleTypeId::SHUFFLE_BINARY: {
                    SerializingBinaryColumns(partition_id, *vec, indexSchema, curBatch);
                    break;
                }
                default: {
                    throw std::runtime_error("Unsupported ShuffleType.");
                }
            }
            spark::VecType *vt = vec->mutable_vectype();
            vt->set_typeid_(proto_col_types_[indexSchema]);
            if(vt->typeid_() == spark::VecType::VEC_TYPE_DECIMAL128 || vt->typeid_() == spark::VecType::VEC_TYPE_DECIMAL64){
                vt->set_precision(input_col_types.inputDataPrecisions[indexSchema]);
                vt->set_scale(input_col_types.inputDataScales[indexSchema]);
                 LogsDebug("precision[indexSchema %d]: %d , scale[indexSchema %d]: %d ",
                          indexSchema, input_col_types.inputDataPrecisions[indexSchema],
                          indexSchema, input_col_types.inputDataScales[indexSchema]);
            }
        }
        curBatch++;

        if (vecBatchProto->ByteSizeLong() > UINT32_MAX) {
            throw std::runtime_error("Unsafe static_cast long to uint_32t.");
        }
        uint32_t vecBatchProtoSize = reversebytes_uint32t(static_cast<uint32_t>(vecBatchProto->ByteSizeLong()));
        void *buffer = nullptr;
        if (!bufferStream->NextNBytes(&buffer, sizeof(vecBatchProtoSize))) {
            LogsError("Allocate Memory Failed: Flush Spilled Data, Next failed.");
            throw std::runtime_error("Allocate Memory Failed: Flush Spilled Data, Next failed.");
        }
        // set serizalized bytes to stream
        memcpy_s(buffer, sizeof(vecBatchProtoSize), &vecBatchProtoSize, sizeof(vecBatchProtoSize));
        LogsDebug(" A Slice Of vecBatchProtoSize: %d ", reversebytes_uint32t(vecBatchProtoSize));

        vecBatchProto->SerializeToZeroCopyStream(bufferStream.get());

        splitRowInfoTmp.remainCopyRow -= splitRowInfoTmp.onceCopyRow;
        splitRowInfoTmp.copyedRow += splitRowInfoTmp.onceCopyRow;
        LogsTrace(" SerializeVecBatch:\n%s", vecBatchProto->DebugString().c_str());
        vecBatchProto->Clear();
    }

    uint64_t partitionBatchSize = bufferStream->flush();
    total_bytes_spilled_ += partitionBatchSize;
    partition_serialization_size_[partition_id] = partitionBatchSize;
    LogsDebug(" partitionBatch write length: %lu", partitionBatchSize);

    // 及时清理分区数据
    partition_cached_vectorbatch_[partition_id].clear(); // 定长数据内存释放
    for (size_t col = 0; col < column_type_id_.size(); col++) {
        vc_partition_array_buffers_[partition_id][col].clear(); // binary 释放内存
    }

    return 0;
}

int Splitter::protoSpillPartitionByRow(int32_t partition_id, std::unique_ptr<BufferedOutputStream> &bufferStream) {
    uint64_t rowCount = partition_rows[partition_id].size();
    total_spill_row_num_ += rowCount;

    uint64_t onceCopyRow = 0;
    uint32_t batchCount = 0;
    while (0 < rowCount) {
        if (options_.spill_batch_row_num < rowCount) {
            onceCopyRow = options_.spill_batch_row_num;
        } else {
            onceCopyRow = rowCount;
        }

        protoRowBatch->set_rowcnt(onceCopyRow);
        protoRowBatch->set_veccnt(proto_col_types_.size());
        for (uint32_t i = 0; i < proto_col_types_.size(); ++i) {
            spark::VecType *vt = protoRowBatch->add_vectypes();
            vt->set_typeid_(proto_col_types_[i]);
            if(vt->typeid_() == spark::VecType::VEC_TYPE_DECIMAL128 || vt->typeid_() == spark::VecType::VEC_TYPE_DECIMAL64){
                vt->set_precision(input_col_types.inputDataPrecisions[i]);
                vt->set_scale(input_col_types.inputDataScales[i]);
                LogsDebug("precision[indexSchema %d]: %d , scale[indexSchema %d]: %d ",
                          i, input_col_types.inputDataPrecisions[i],
                          i, input_col_types.inputDataScales[i]);
            }
        }

        int64_t offset = batchCount * options_.spill_batch_row_num;
        auto rowInfoPtr = partition_rows[partition_id].data() + offset;
        for (uint64_t i = 0; i < onceCopyRow; ++i) {
            RowInfo *rowInfo = rowInfoPtr[i];
            spark::ProtoRow *protoRow = protoRowBatch->add_rows();
            protoRow->set_data(rowInfo->row, rowInfo->length);
            protoRow->set_length(rowInfo->length);
            // free row memory
            delete rowInfo;
        }

        if (protoRowBatch->ByteSizeLong() > UINT32_MAX) {
            throw std::runtime_error("Unsafe static_cast long to uint_32t.");
        }
        uint32_t protoRowBatchSize = reversebytes_uint32t(static_cast<uint32_t>(protoRowBatch->ByteSizeLong()));
        void *buffer = nullptr;
        if (!bufferStream->NextNBytes(&buffer, sizeof(protoRowBatchSize))) {
            throw std::runtime_error("Allocate Memory Failed: Flush Spilled Data, Next failed.");
        }
        // set serizalized bytes to stream
        memcpy_s(buffer, sizeof(protoRowBatchSize), &protoRowBatchSize, sizeof(protoRowBatchSize));
        LogsDebug(" A Slice Of vecBatchProtoSize: %d ", reversebytes_uint32t(protoRowBatchSize));

        protoRowBatch->SerializeToZeroCopyStream(bufferStream.get());
        rowCount -= onceCopyRow;
        batchCount++;
        protoRowBatch->Clear();
    }

    uint64_t partitionBatchSize = bufferStream->flush();
    total_bytes_spilled_ += partitionBatchSize;
    partition_serialization_size_[partition_id] = partitionBatchSize;
    partition_rows[partition_id].clear();
    LogsDebug(" partitionBatch write length: %lu", partitionBatchSize);
    return 0;
}

int Splitter::WriteDataFileProto() {
    LogsDebug(" spill DataFile: %s ", (options_.next_spilled_file_dir + ".data").c_str());
    std::unique_ptr<OutputStream> outStream = writeLocalFile(options_.next_spilled_file_dir + ".data");
    WriterOptions options;
    // tmp spilled file no need compression
    options.setCompression(CompressionKind_NONE);
    std::unique_ptr<StreamsFactory> streamsFactory = createStreamsFactory(options, outStream.get());
    std::unique_ptr<BufferedOutputStream> bufferStream = streamsFactory->createStream();
    // 顺序写入每个partition的offset
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        protoSpillPartition(pid, bufferStream);
    }
    memset_s(partition_id_cnt_cache_, num_partitions_ * sizeof(uint64_t), 0, num_partitions_ * sizeof(uint64_t));
    outStream->close();
    return 0;
}

int Splitter::WriteDataFileProtoByRow() {
    LogsDebug(" spill DataFile: %s ", (options_.next_spilled_file_dir + ".data").c_str());
    std::unique_ptr<OutputStream> outStream = writeLocalFile(options_.next_spilled_file_dir + ".data");
    WriterOptions options;
    // tmp spilled file no need compression
    options.setCompression(CompressionKind_NONE);
    std::unique_ptr<StreamsFactory> streamsFactory = createStreamsFactory(options, outStream.get());
    std::unique_ptr<BufferedOutputStream> bufferStream = streamsFactory->createStream();
    // 顺序写入每个partition的offset
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        protoSpillPartitionByRow(pid, bufferStream);
    }
    outStream->close();
    return 0;
}

void Splitter::MergeSpilled() {
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        CacheVectorBatch(pid, true);
        partition_buffer_size_[pid] = 0; // 溢写之后将其清零，条件溢写需要重新分配内存
    }

    std::unique_ptr<OutputStream> outStream = writeLocalFile(options_.data_file);
    LogsDebug(" Merge Spilled Tmp File: %s ", options_.data_file.c_str());
    WriterOptions options;
    options.setCompression(options_.compression_type);
    options.setCompressionBlockSize(options_.compress_block_size);
    options.setCompressionStrategy(CompressionStrategy_COMPRESSION);
    std::unique_ptr<StreamsFactory> streamsFactory = createStreamsFactory(options, outStream.get());
    std::unique_ptr<BufferedOutputStream> bufferOutPutStream = streamsFactory->createStream();

    void* bufferOut = nullptr;
    int sizeOut = 0;
    for (int pid = 0; pid < num_partitions_; pid++) {
        ProtoWritePartition(pid, bufferOutPutStream, bufferOut, sizeOut);
        LogsDebug(" MergeSpilled traversal partition( %d ) ", pid);
        for (auto &pair : spilled_tmp_files_info_) {
            auto tmpDataFilePath = pair.first + ".data";
            auto tmpPartitionOffset = reinterpret_cast<uint64_t *>(pair.second->data_)[pid];
            auto tmpPartitionSize = reinterpret_cast<uint64_t *>(pair.second->data_)[pid + 1] - reinterpret_cast<uint64_t *>(pair.second->data_)[pid];
            LogsDebug(" get Partition Stream...tmpPartitionOffset %d tmpPartitionSize %d path %s",
                      tmpPartitionOffset, tmpPartitionSize, tmpDataFilePath.c_str());
            std::unique_ptr<InputStream> inputStream = readLocalFile(tmpDataFilePath);
            uint64_t targetLen = tmpPartitionSize;
            uint64_t seekPosit = tmpPartitionOffset;
            uint64_t onceReadLen = 0;
            while ((targetLen > 0) && bufferOutPutStream->Next(&bufferOut, &sizeOut)) {
                onceReadLen = targetLen > static_cast<uint64_t>(sizeOut) ? sizeOut : targetLen;
                inputStream->read(bufferOut, onceReadLen, seekPosit);
                targetLen -= onceReadLen;
                seekPosit += onceReadLen;
                if (onceReadLen < static_cast<uint64_t>(sizeOut)) {
                    // Reached END.
                    bufferOutPutStream->BackUp(sizeOut - onceReadLen);
                    break;
                }
            }

            uint64_t flushSize = bufferOutPutStream->flush();
            total_bytes_written_ += flushSize;
            LogsDebug(" Merge Flush Partition[%d] flushSize: %ld ", pid, flushSize);
            partition_lengths_[pid] += flushSize;
        }
    }

    memset_s(partition_id_cnt_cache_, num_partitions_ * sizeof(uint64_t), 0, num_partitions_ * sizeof(uint64_t));
    ReleaseVarcharVector();
    num_row_splited_ = 0;
    cached_vectorbatch_size_ = 0;
    outStream->close();
}

void Splitter::MergeSpilledByRow() {
    std::unique_ptr<OutputStream> outStream = writeLocalFile(options_.data_file);
    LogsDebug(" Merge Spilled Tmp File: %s ", options_.data_file.c_str());
    WriterOptions options;
    options.setCompression(options_.compression_type);
    options.setCompressionBlockSize(options_.compress_block_size);
    options.setCompressionStrategy(CompressionStrategy_COMPRESSION);
    std::unique_ptr<StreamsFactory> streamsFactory = createStreamsFactory(options, outStream.get());
    std::unique_ptr<BufferedOutputStream> bufferOutPutStream = streamsFactory->createStream();

    void* bufferOut = nullptr;
    int sizeOut = 0;
    for (int pid = 0; pid < num_partitions_; pid++) {
        ProtoWritePartitionByRow(pid, bufferOutPutStream, bufferOut, sizeOut);
        LogsDebug(" MergeSpilled traversal partition( %d ) ", pid);
        for (auto &pair : spilled_tmp_files_info_) {
            auto tmpDataFilePath = pair.first + ".data";
            auto tmpPartitionOffset = reinterpret_cast<uint64_t *>(pair.second->data_)[pid];
            auto tmpPartitionSize = reinterpret_cast<uint64_t *>(pair.second->data_)[pid + 1] - reinterpret_cast<uint64_t *>(pair.second->data_)[pid];
            LogsDebug(" get Partition Stream...tmpPartitionOffset %d tmpPartitionSize %d path %s",
                      tmpPartitionOffset, tmpPartitionSize, tmpDataFilePath.c_str());
            std::unique_ptr<InputStream> inputStream = readLocalFile(tmpDataFilePath);
            uint64_t targetLen = tmpPartitionSize;
            uint64_t seekPosit = tmpPartitionOffset;
            uint64_t onceReadLen = 0;
            while ((targetLen > 0) && bufferOutPutStream->Next(&bufferOut, &sizeOut)) {
                onceReadLen = targetLen > static_cast<uint64_t>(sizeOut) ? sizeOut : targetLen;
                inputStream->read(bufferOut, onceReadLen, seekPosit);
                targetLen -= onceReadLen;
                seekPosit += onceReadLen;
                if (onceReadLen < static_cast<uint64_t>(sizeOut)) {
                    // Reached END.
                    bufferOutPutStream->BackUp(sizeOut - onceReadLen);
                    break;
                }
            }

            uint64_t flushSize = bufferOutPutStream->flush();
            total_bytes_written_ += flushSize;
            LogsDebug(" Merge Flush Partition[%d] flushSize: %ld ", pid, flushSize);
            partition_lengths_[pid] += flushSize;
        }
    }
    outStream->close();
}

void Splitter::WriteSplit() {
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        CacheVectorBatch(pid, true);
        partition_buffer_size_[pid] = 0; // 溢写之后将其清零，条件溢写需要重新分配内存
    }

    std::unique_ptr<OutputStream> outStream = writeLocalFile(options_.data_file);
    WriterOptions options;
    options.setCompression(options_.compression_type);
    options.setCompressionBlockSize(options_.compress_block_size);
    options.setCompressionStrategy(CompressionStrategy_COMPRESSION);
    std::unique_ptr<StreamsFactory> streamsFactory = createStreamsFactory(options, outStream.get());
    std::unique_ptr<BufferedOutputStream> bufferOutPutStream = streamsFactory->createStream();

    void* bufferOut = nullptr;
    int32_t sizeOut = 0;
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        ProtoWritePartition(pid, bufferOutPutStream, bufferOut, sizeOut);
    }

    memset_s(partition_id_cnt_cache_, num_partitions_ * sizeof(uint64_t), 0, num_partitions_ * sizeof(uint64_t));
    ReleaseVarcharVector();
    num_row_splited_ = 0;
    cached_vectorbatch_size_ = 0;
    outStream->close();
}

void Splitter::WriteSplitByRow() {
    std::unique_ptr<OutputStream> outStream = writeLocalFile(options_.data_file);
    WriterOptions options;
    options.setCompression(options_.compression_type);
    options.setCompressionBlockSize(options_.compress_block_size);
    options.setCompressionStrategy(CompressionStrategy_COMPRESSION);
    std::unique_ptr<StreamsFactory> streamsFactory = createStreamsFactory(options, outStream.get());
    std::unique_ptr<BufferedOutputStream> bufferOutPutStream = streamsFactory->createStream();

    void* bufferOut = nullptr;
    int32_t sizeOut = 0;
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        ProtoWritePartitionByRow(pid, bufferOutPutStream, bufferOut, sizeOut);
    }
    outStream->close();
}

int Splitter::DeleteSpilledTmpFile() {
    for (auto &pair : spilled_tmp_files_info_) {
        auto tmpDataFilePath = pair.first + ".data";
        // 释放存储有各个临时文件的偏移数据内存
        options_.allocator->Free(pair.second->data_, pair.second->capacity_);
        pair.second->SetReleaseFlag();
        if (IsFileExist(tmpDataFilePath)) {
            remove(tmpDataFilePath.c_str());
        }
    }
    // 释放内存空间，Reset spilled_tmp_files_info_, 这个地方是否有内存泄漏的风险？？？
    spilled_tmp_files_info_.clear();
    return 0;
}

int Splitter::SpillToTmpFile() {
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        CacheVectorBatch(pid, true);
        partition_buffer_size_[pid] = 0; // 溢写之后将其清零，条件溢写需要重新分配内存
    }

    options_.next_spilled_file_dir = CreateTempShuffleFile(NextSpilledFileDir());
    WriteDataFileProto();
    std::shared_ptr<Buffer> ptrTmp = CaculateSpilledTmpFilePartitionOffsets();
    spilled_tmp_files_info_[options_.next_spilled_file_dir] = ptrTmp;
    ReleaseVarcharVector();
    num_row_splited_ = 0;
    cached_vectorbatch_size_ = 0;
    return 0;
}

int Splitter::SpillToTmpFileByRow() {
    options_.next_spilled_file_dir = CreateTempShuffleFile(NextSpilledFileDir());
    WriteDataFileProtoByRow();
    std::shared_ptr<Buffer> ptrTmp = CaculateSpilledTmpFilePartitionOffsets();
    spilled_tmp_files_info_[options_.next_spilled_file_dir] = ptrTmp;
    return 0;
}

Splitter::Splitter(InputDataTypes inputDataTypes, int32_t num_cols, int32_t num_partitions, SplitOptions options, bool flag)
        : singlePartitionFlag(flag),
          num_partitions_(num_partitions),
          options_(std::move(options)),
          num_fields_(num_cols),
          input_col_types(inputDataTypes)
{
    LogsDebug("Input Schema colNum: %d", num_cols);
    ToSplitterTypeId(num_cols);
}

Splitter *Create(InputDataTypes inputDataTypes,
                                 int32_t num_cols,
                                 int32_t num_partitions,
                                 SplitOptions options,
                                 bool flag)
{
    auto res = new Splitter(inputDataTypes, num_cols, num_partitions, std::move(options), flag);
    res->Split_Init();
    return res;
}

Splitter *Splitter::Make(
         const std::string& short_name,
         InputDataTypes inputDataTypes,
         int32_t num_cols,
         int num_partitions,
         SplitOptions options) {
    if (short_name == "hash" || short_name == "rr" || short_name == "range") {
        return Create(inputDataTypes, num_cols, num_partitions, std::move(options), false);
    } else if (short_name == "single") {
        return Create(inputDataTypes, num_cols, num_partitions, std::move(options), true);
    } else {
        throw("ERROR: Unsupported Splitter Type.");
    }
}

std::string Splitter::NextSpilledFileDir() {
    auto spilled_file_dir = GetSpilledShuffleFileDir(configured_dirs_[dir_selection_],
                                                     sub_dir_selection_[dir_selection_]);
    LogsDebug(" spilled_file_dir %s ", spilled_file_dir.c_str());
    sub_dir_selection_[dir_selection_] =
            (sub_dir_selection_[dir_selection_] + 1) % options_.num_sub_dirs;
    dir_selection_ = (dir_selection_ + 1) % configured_dirs_.size();
    return spilled_file_dir;
}

int Splitter::Stop() {
    if (isSpill) {
        TIME_NANO_OR_RAISE(total_write_time_, MergeSpilled());
        TIME_NANO_OR_RAISE(total_write_time_, DeleteSpilledTmpFile());
        LogsDebug(" Spill For Splitter Stopped. total_spill_row_num_: %ld ", total_spill_row_num_);
    } else {
        TIME_NANO_OR_RAISE(total_write_time_, WriteSplit());
    }
    if (nullptr == vecBatchProto) {
        throw std::runtime_error("delete nullptr error for free protobuf vecBatch memory");
    }
    return 0;
}

int Splitter::StopByRow() {
    if (isSpill) {
        TIME_NANO_OR_RAISE(total_write_time_, MergeSpilledByRow());
        TIME_NANO_OR_RAISE(total_write_time_, DeleteSpilledTmpFile());
        LogsDebug(" Spill For Splitter Stopped. total_spill_row_num_: %ld ", total_spill_row_num_);
    } else {
        TIME_NANO_OR_RAISE(total_write_time_, WriteSplitByRow());
    }
    if (nullptr == protoRowBatch) {
        throw std::runtime_error("delete nullptr error for free protobuf rowBatch memory");
    }
    return 0;
}
