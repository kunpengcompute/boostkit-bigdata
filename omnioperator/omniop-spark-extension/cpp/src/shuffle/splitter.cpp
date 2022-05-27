/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#include "splitter.h"
#include "utils.h"

uint64_t SHUFFEL_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD = UINT64_MAX;

SplitOptions SplitOptions::Defaults() { return SplitOptions(); }

// 计算分区id,每个batch初始化
int Splitter::ComputeAndCountPartitionId(VectorBatch& vb) {
    auto num_rows = vb.GetRowCount();
    std::filll(std::begin(partition_id_cnt_cur_), std::end(partition_id_cnt_cur_), 0);
    partition_id_.resize(num_rows);

    if (singlePartitionFlag) {
        partition_id_cnt_cur_[0] = num_rows;
        partition_id_cnt_cache_[0] += num_rows;
        for (auto i = 0; i < num_rows; ++1) {
            partition_id_[i] = 0;
        }
    } else {
        IntVector* hashVct =  static_cast<IntVector *>(vb.GetVector(0));
        for (auto i = 0; i < num_rows; ++1) {
            // positive mod
            int32_t pid =  hashVct->GetValue(i);
            partition_id_[i] = pid;
            partition_id_cnt_cur_[pid]++;
            partiiton_id_cnt_cache_[pid]++;
        }
    }
    total_compute_pid_time_ += 1;
    return 0;
}

//分区信息内存分配
int Splitter::AllocatePartitionBuffers(int32_t partition_id, int32_t new_size) {
    //TODO:内存申请失败重试及异常抛出
    std::vector<std::shared_ptr<Buffer>> new_binary_builders;
    std::vector<std::shared_ptr<Buffer>> new_value_buffers;
    std::vector<std::shared_ptr<Buffer>> new_validity_buffers;

    int num_fields = column_type_id.size();
    auto fixed_width_idx = 0;
    auto binary_idx = 0;

    for (auto i = 0; i < num_fields; ++i) {
        switch (column_type_id_[i]) {
            //TODO:不同数据类型的内存空间分配差异处理
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
                void *ptr_tmp = static_cast<void *>(opitons_.allocator->alloc(new_size * (1 << column_type_id_[i])));
                fixed_valueBuffer_size_[partition_id] = new_size * (1 << column_type_id_[i]);
                if (nullptr == ptr_tmp) {
                    return -1;
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
            // TODO:不同数据类型的内存空间分配差异处理
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

int Splitter::SplitFixedWidthValidityBuffer(VectorBatch& vb) {
    const auto num_rows = vb.GetRowCount();
    for (auto col = 0; col < fixed_width_array_idx_.size(); ++col) {
        std::fill(std::begin(partition_buffer_idx_offset_),
                  std::end(partition_buffer_idx_offset_), 0);
        auto col_idx_vb = fixed_width_array_idx_[col];
        auto col_idx_schema = singlePartitionFlag ? col_idx_vb : (col_idx_vb - 1);
        const auto& dst_addrs =  partition_fixed_width_value_addrs_[col];
        if (vb.GetVector(col_idx_vb)->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
            LogsDebug("Dictionary Columnar process!");
            auto ids_tmp =  static_cast<int32_t *>(options_.allocator->alloc(num_rows * sizeof(int32_t)));
            Buffer *ids (new Buffer((uint8_t*)ids_tmp, 0, num_rows * sizeof(int32_t)));
            if (ids->data_ == nullptr) {
                throw std::runtime_error("Allocator for SpiltFixedWidthValueBuffer ids Failed! ");
            }
            auto dictionaryTmp = ((DictionaryVector *)(vb.GetVector(col_idx_vb)))->ExtractDictionaryAndIds(0, num_rows, (int32_t *)(ids->data_));
            auto src_addr = Vectorhelper::GetValuesAddr(dictionaryTmp);
            switch (column_type_id_[col_idx_schema]) {
#define PROCESS(SHUFFLE_TYPE, CTYPE)                                                           \
    case SHUFFLE_TYPE:                                                                         \
        for (auto row = 0; row < num_rows; ++row) {                                            \
            auto pid = partition_id_[row];                                                     \
            auto dst_offset =                                                                  \
                partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid];           \
            reinterpret_cast<CTYPE*>(dst_addrs[pid])[dst_offset] =                             \
            reinterpret_cast<CTYPE*>(src_addr)[reinterpret_cast<int32_t*>(ids->data_)[row]];   \
            partition_fixed_width_buffer_[col][pid][1]->size_ += (1 << SHUFFLE_TYPE);          \
            partition_buffer_idx_offset_[pid]++;                                               \
        }                                                                                      \
    break;
                PROCESS(SHUFFLE_1BYTE, uint8_t)
                PROCESS(SHUFFLE_2BYTE, uint16_t)
                PROCESS(SHUFFLE_4BYTE, uint32_t)
                PROCESS(SHUFFLE_8BYTE, uint64_t)
#undef PROCESS
                case SHUFFLE_DECIMAL128:
                    for (auto row = 0; row < num_rows; ++row) {
                        auto pid =  partition_id_[row];
                        auto dst_offset =
                                partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid];
                        reinterpret_cast<uint64_t*>(dst_addrs[pid])[dst_offset << 1] =
                        reinterpret_cast<uint64_t*>(src_addr)[reinterpret_cast<int32_t *>(ids->data_)[row] << 1]; // 前64位取值、赋值
                        reinterpret_cast<uint64_t*>(dst_addrs[pid])[(dst_offset << 1) | 1] =
                        reinterpret_cast<uint64_t*>(src_addr)[reinterpret_cast<int32_t *>(ids->data_)[row] << 1 | 1]; // 后64位取值、赋值
                        partition_fixed_width_buffers_[col][pid][1]->size_ +=
                                (1 << SHUFFLE_DECIMAL128); //decimal128 16Bytes
                        partition_buffer_idx_offset_[pid]++;
                    }
                    break;
                default:
                    throw "ERROR: SPlitFixedWidthValueBuffer not match this type: " + column_type_id_[col_idx_schema];                
            }
            options_.allocator->free(ids->data_, ids->capacity_);
        } else {
            auto src_addr = VectorHelper::GetValuesAddr(vb.GetVector(col_idx_vb));
            //TODO:check Null return
            switch (column_type_id_[col_idx_schema]) {
#define PROCESS(SHUFFLE_TYPE, CTYPE)                                                           \
    case SHUFFLE_TYPE:                                                                         \
        for (auto row = 0; row < num_rows; ++row) {                                            \
            auto pid = partition_id_[row];                                                     \
            auto dst_offset =                                                                  \
                partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid];           \
            reinterpret_cast<CTYPE*>(dst_addrs[pid])[dst_offset] =                             \
                reinterpret_cast<CTYPE*>(src_addr)[row];                                       \
            partition_fixed_width_buffer_[col][pid][1]->size_ += (1 << SHUFFLE_TYPE);          \
            partition_buffer_idx_offset_[pid]++;                                               \
        }                                                                                      \
    break;
                PROCESS(SHUFFLE_1BYTE, uint8_t)
                PROCESS(SHUFFLE_2BYTE, uint16_t)
                PROCESS(SHUFFLE_4BYTE, uint32_t)
                PROCESS(SHUFFLE_8BYTE, uint64_t)
#undef PROCESS
                case SHUFFLE_DECIMAL128:
                    for (auto row = 0; row < num_rows; ++row) {
                        auto pid =  partition_id_[row];
                        auto dst_offset =
                                partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid];
                        reinterpret_cast<uint64_t*>(dst_addrs[pid])[dst_offset << 1] =
                                reinterpret_cast<uint64_t*>(src_addr)[row << 1]; // 前64位取值、赋值
                        reinterpret_cast<uint64_t*>(dst_addrs[pid])[(dst_offset << 1) | 1] =
                                reinterpret_cast<uint64_t*>(src_addr)[(row << 1) | 1]; // 后64位取值、赋值
                        partition_fixed_width_buffers_[col][pid][1]->size_ +=
                                (1 << SHUFFLE_DECIMAL128); //decimal128 16Bytes
                        partition_buffer_idx_offset_[pid]++;
                    }
                    break;
                default:
                    throw "ERROR: SPlitFixedWidthValueBuffer not match this type: " + column_type_id_[col_idx_schema];                
            }
        }
    }
    return 0;
}

int Splitter::SplitBinaryArray(VectorBatch& vb)
{
    const auto numRows = vb.GetRowCount();
    auto vecCntVb = vb.GetVectorCount();
    auto vecCntSchema = singlePartitionFlag ? vecCntVb : vecCntVb - 1;
    for (auto colSchema = 0; colSchema < vecCntSchema; ++colSchema) {
        switch (column_typte_id_[colSchema]) {
            case SHUFFLE_BINARY: {
                auto colVb = singlePartitionFlag ? colSchema : colSchema + 1;
                if (vb.GetVector(colVb)->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
                    for (auto row = 0; row < numRows; ++row) {
                        auto pid = partition_id_[row];
                        uint8_t *dst = nullptr;
                        auto str_len = ((DictionaryVector *)(vb.GetVector(colVb)))->GetVarchar(row, &dst);
                        cacheed_vectorbatch_size_ += str_len; // 累计变长部分cache数据
                        VCLocation cl((uint64_t) dst, str_len);
                        if ((vc_partition_array_buffers_[pid][colSchema].size() != 0) &&
                            (vc_partition_array_buffers_[pid][colSchema].back().getVcList().size() <
                            options_.spill_batch_row_num)) {
                            vc_partition_array_buffers_[pid][colSchema].back().getVcList().push_back(cl);
                            vc_partition_array_buffers_[pid][colSchema].back().vcb_total_len += str_len;
                        } else {
                            VCBatchInfo svc(options_.spill_batch_row_num);
                            svc.getVcList().push_back(cl);
                            svc.vcb_total_len += str_len;
                            vc_partition_array_buffers_[pid][colSchema].push_back(svc);
                        }
                    }
                } else {
                    VarcharVector *vc = nullptr;
                    vc = static_cast<VarcharVector *>(vb.GetVector(colVb));
                    for (auto row = 0; row < numRows; ++row) {
                        auto pid = partition_id_[row];
                        uint8_t *dst = nullptr;
                        int str_len = vc->GetValue(row, &dst);
                        cached_cectorbatch_size_ += str_len; // 累计变长部分cache数据
                        VCLocation cl((uint64_t) dst, str_len);
                        if ((vc_partition_array_buffers_[pid][colSchema].size() != 0) &&
                            (vc_partition_array_buffers_[pid][colSchema].back().getVcList().size() <
                            options_.spill_batch_row_num)) {
                            vc_partition_array_buffers_[pid][colSchema].back().getVcList().push_back(cl);
                            vc_partition_array_buffers_[pid][colSchema].back().vcb_total_len += str_len;
                        } else {
                            VCBatchInfo svc(options_.spill_batch_row_num);
                            svc.getVcList().push_back(cl);
                            svc.vcb_total_len += str_len;
                            vc_partition_array_buffers_[pid][colSchema].push_back(svc);
                        }
                    }
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
    for (auto col = 0; col < fixed_width_array_dix_.size(); ++col) {
        auto col_idx = fixed_width_array_idx[col];
        auto& dst_addrs = partition_fixed_width_validity_addrs_[col];
        // TODO(), 补齐GetNullCount()接口，-1逻辑必不相等
        // 分配内存并初始化
        for (auto pid = 0; pid < num_partitions_; ++pid) {
            if (partition_id_cnt_cur_[pid] > 0 && dst_addrs[pid] == nullptr) {
                // init bitmap if it's null
                auto new_size = partition_id_cnt_cur_[pid] > options_.buffer_size ? partition_id_cnt_cur_[pid] : options_.buffer_size;
                auto ptr_tmp = static_cast<uint8_t *>(options_.allocator->alloc(new_size));
                std::shared_ptr<Buffer> validity_buffer (new Buffer((uint8_t *)ptr_tmp, 0, new_size));
                dst_addr[pid] = const_cast<uint8_t*>(validity_buffer->data_);
                std::memset(validity_buffer->data_, 0, new_size);
                partition_fixed_width_buffers_[col][pid][0] = std::move(validity_buffer);
                fixed_nullBuffer_size_[pid] = new_size;
            } 
        }

        // 计算并填充数据
        auto src_addr = const_cast<uint8_t*>((uint8_t*)((vb.GetVector(col_idx))->GetValueNulls()));
        std::fill(std::begin(partition_buffer_idx_offset_),
                std::end(partition_buffer_idx_offset_), 0);
        const auto num_rows = vb.GetRowCount();
        for (auto row = 0; row < num_rows; ++row) {
            auto pid = partition_id_[row];
            auto dst_offset = partition_buffer_idx_base_[pid] + partition_buffer_idx_offset_[pid];
            dst_addrs[pid][dst_offset] = src_addr[row];
            partition_buffer_idx_offset_[pid]++;
            partition_fixed_width_buffers_[col][pid][0]->size_ += 1;
        }
    }
    return 0;
}

int Splitter::CacheVectorBatch(int32_t partition_id, bool reset_buffers) {
    if (partition_buffer_idx_base_[partition_id] > 0 && fixed_width_array_idx_.size() > 0) {
        auto fixed_width_idx = 0;
        auto num_fields = num_fields_;
        int64_t batch_partiton_size = 0;
        std::vector<std::vector<std::shared_ptr<Buffer>>> bufferArrayTotal(num_fields);

        for (int i = 0; i < num_fields; ++i) {
            switch (column_type_id_[i]) {
                case SHUFFLE_BINARY: {
                    // TODO:...
                    break;
                }
                case SHUFFLE_LARGE_BINARY: {
                    // TODO:...
                    break;
                }
                case SHUFFLE_NULL: {
                    // TODO:...
                    break;
                }
                default: {
                    auto& buffers = partiiton_fixed_width_buffers_[fixed_width_idx][partition_id];
                    batch_partition_size += buffers[0]->capacity_; // 累计null数组所占内存大小
                    batch_partition_size += buffers[1]->capacity_; // 累计value数组所占内存大小
                    if (reset_buffers) {
                        buffreArrayTotal[fixed_width_idx] = std::move(buffres);
                        buffers = {nullptr};
                        partition_fixed_width_validity_addrs_[fixed_width_idx][partition_id] = nullptr;
                        partition_fixed_width_value_addrs_[fixed_width_idx][partition_id] = nullptr;
                    } else {
                        bufferArrayTotal[fixed_width_dix] = buffers;
                    }
                    fixed_width_idx++;
                    break;
                }
            }
        }
        cached_vectorbatch_size_ += batch_partition_size;
        partition_cached_vectorbatch_[partition_id].push_back(std:;move(bufferArrayTotal));
        partition_buffer_idx_base_[partition_id] = 0;
    }
    return 0;
}

int Splitter::DoSplit(VectorBatch& vb) {
    // for the first input record batch, scan binary arrays and large binary
    // arrays to get their empirical sizes

    if (!first_vector_batch_) {
        first_vector_batch_ = true;
    }

    for (auto col = 0; col < fixed_width_array_idx_.size(); ++col) {
        auto col_idx = fixed_width_array_dix_[col];
        // TOD(),补齐GetNullCount()接口，-1逻辑必不相等
        if (vb.GetVector(col_idx)->GetValueNulls() != nullptr) {
            input_fixed_width_has_null_[col] = true;
        }
    }

    // prepare partiiton buffers and spill if necessary
    for (auto pid = 0; pid < num_partitions_; ++pid) {
        if (fixed_width_array_idx_.size() > 0 &&
            partition_id_cnt_cur_[pid] > 0 &&
            partition_buffer_idx_base_[pid] + partition_id_cnt_cur_[pid] > partition_buffer_size_[pid]) {
            auto new_size = partition_id_cnt_cur_[pid] > options_.buffers_size ? partition_id_cnt_cur_[pid] : options_.buffer_size;
            // TODO: options_.prefer_spill 配置项支持
            if (partition_buffer_size_[pid] == 0) { // first allocate?
                AllocatePartitionBuffers(pid, new_size);
            } else { // not first allocate, spill
                if (partition_id_cnt_cur_[pid] > partition_buffer_size_[pid]) { // need reallocate?
                    // AllocatePartitionBuffers will then Reserve memory for builder based on last
                    // recordbatch, the logic on reservation size should be cleaned up
                    CacheVectorBatch(pid, true);
                    AllocatePartitionBuffers(pid, new_size);
                } else {
                    CacheVectorBatch(pid, true);
                    AllocatePartitionBuffers(pid, new_size);
                }
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
    vectorBatch_cache_.push_back(&vb); // record for release vector

    // 阈值检查，是否溢写
    num_row_splited_ += vb.GetRowCount();
    if (num_row_splited_ + vb.GetRowCount() >= SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD) {
        LogsDebug(" Spill For Row Num Threshold.");
        TIME_NANO_OR_RAISE(total_spill_time_, SpillToTmpFile());
    }
    if (cached_vectorbatch_size_ + current_fixed_alloc_buffer_size_ >= options.spill_mem_threshold) {
        LogsDebug(" Spill For Memory Size Threshold.");
        TIME_NANO_OR_RAISE(total_spill_time_, SpillToTmpFile());
    }
    return 0;
}

void Splitter::ToSplitterTypeId(int num_cols)
{
    for (int i = 0; i < num_cols; ++i) {
        switch (input_col_types.inputVecTypeIds[i]) {
            case OMNI_INT:{
                column_type_id_.push_back(ShuffleTypeId::SHUFFLE_4BYTE);
                vector_batch_col_types.push_back(OMNI_INT);
                break;
            }
            case OMNI_LONG:{
                column_type_id_.push_back(ShuffleTypeId::SHUFFLE_8BYTE);
                vector_batch_col_types.push_back(OMNI_LONG);
                break;
            }
            case OMNI_DOUBLE:{
                column_type_id_.push_back(ShuffleTypeId::SHUFFLE_8BYTE);
                vector_batch_col_types.push_back(OMNI_DOUBLE);
                break;
            }
            case OMNI_DATE32:{
                column_type_id_.push_back(ShuffleTypeId::SHUFFLE_4BYTE);
                vector_batch_col_types.push_back(OMNI_DATE32);
                break;
            }
            case OMNI_DATE64:{
                column_type_id_.push_back(ShuffleTypeId::SHUFFLE_8BYTE);
                vector_batch_col_types.push_back(OMNI_DATE64);
                break;
            }
            case OMNI_DECIMAL64:{
                column_type_id_.push_back(ShuffleTypeId::SHUFFLE_8BYTE);
                vector_batch_col_types.push_back(OMNI_DECIMAL64);
                break;
            }
            case OMNI_DECIMAL128:{
                column_type_id_.push_back(ShuffleTypeId::SHUFFLE_DECIMAL128);
                vector_batch_col_types.push_back(OMNI_DECIMAL128);
                break;
            }
            case OMNI_CHAR:{
                column_type_id_.push_back(ShuffleTypeId::SHUFFLE_BINARY);
                vector_batch_col_types.push_back(OMNI_CHAR);
                break;
            }
            case OMNI_VARCHAR:{
                column_type_id_.push_back(ShuffleTypeId::SHUFFLE_BINARY);
                vector_batch_col_types.push_back(OMNI_VARCHAR);
                break;
            }
            default:{
                throw std::runtime_error("Unsupported DataTypeId.");
            }
        }
    }
}

int SPlitter::Split_Init(){
    num_row_splited_ = 0;
    cached_vectorbatch_size_ = 0;
    partition_id_cnt_cur.resize(num_partitions_);
    partition_id_cnt_cache_.resize(num_partitions_);
    partition_buffer_size_.resize(num_partitions_);
    partition_buffer_idx_base_.resize(num_partitions_);
    partition_buffer_offset_.resize(num_partitions_);
    partition_cached_vectorbatch_.resize(num_partitions_);
    partition_serialization_size_.resize(num_partitions_);
    fixed_width_array_idx_.clear();
    partition_lengths_.resize(num_partitions_);
    fixed_valueBuffer_size_.resize(num_partitions_);
    fixed_nullBuffer_size_.resize(num_partitions_);

    //obtain configed dir from Environment Variables
    configured_dirs_ = GetConfiguredLocalDirs();
    sub_dir_selection_.assign(configured_dirs_.size(), 0);

    // Both data_file and shuffle_index_file should be set through jni.
    // For test purpose, Create a temporary subdirectory in the system temporary
    // dir with prefix "xolumnar-shuffle"
    if (options_.data_file.length() == 0) {
        options_.data_file = CreateTempShuffleFile(configured_dirs_[0]);
    }

    for (int i = 0; i < column_type_id_.size(); ++i) {
        switch (column_tyoe_id_[i]) {
            // TODO:补充多种数据类型
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
    input_fixed_width_has_null_.resize(num_fixed_width, false);
    for (auto i = 0; i < num_fixed_width; ++i) {
        partition_fixed_width_validity_addrs_[i].resize(num_partitions_);
        partition_fixed_width_value_addrs_[i].resize(num_partitions_);
        partition_fixed_width_buffers_[i].resize(num_partitions_);
    }

    /* init varchar partition */
    vc_partition_array_buffers_.resize(num_partitions_);
    for (auto i = 0; i < num_partitions_; ++i) {
        vc_partition_array_buffers_[i].resize(column_type_id_.size());
    }
    return 0;
}

int Splitter::Split(VectorBatch& vb )
{
    //计算vectorBatch分区信息
    LogsTrace(" split vb row number: %d", vb.GetRowCount());
    TIME_NANO_OR_RAISE(total_compute_pid_time_, ComputeAndCountPartiionId(vb));
    //执行分区动作
    DoSplit(vb);
    return 0;
}

std::shared_ptr<Buffer> Splitter::CaculateSpilledTmpFilePartitionOffsets() {
    void *ptr_tmp = static_cast<void *>(options_.allocate->alloc((num_partitions_ + 1) * sizeof(uint32_t)));
    std::shared_ptr<Buffer> ptrPartitionOffsets (new Buffer((uint8_t*)ptr_tmp, 0, (num_partitions_ + 1) * sizeof(uint32_t)));
    uint32_t pidOffset = 0;
    // 顺序记录每个partition的offset
    auto pid = 0;
    for (pid = 0; pid < num_partitions_; ++pid) {
        reinterpret_cast<uint32_t *>(ptrPartitiionOffsets->data_)[pid] = pidOffset;
        pidOffset += partition_serialization_size_[pid];
        // reset partition_cached_vectorbatch_size_ to 0
        partition_serialization_size_[pid] = 0;
    }
    reinterpret_cast<uint32_t *>(ptrPartitionOffsets->data_)[pid] = pidOffset;
    return ptrPartitionOffsets;
}

spark::VecType::VecTypeId CastShuffleTypeIdToVecType(int32_t tmpType) {
    switch (tmpType) {
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
            return spark::VecType::OMNI_INTERVAL_DAY_TIME;
        case OMNI_VARCHAR:
            return spark::VecType::VEC_TYPE_VARCHAR;
        case OMNI_CHAR:
            return spark::VecType::VEC_TYPE_CHAR;
        case OMNI_CONSTAINER:
            return spark::VecType::VEC_TYPE_CONTAINER;
        case OMNI_INVALID:
            return spark::VecType::VEC_TYPE_INVALID;
        case default: {
            throw std::runtime_error("castShuffleTypeIdToVecType() unexpected ShuffleTypeId");
        }
    }
};

int Splitter::SerializingFixedColumns(int32_t partitionId,
                                      spark::Vec& vec,
                                      int fixColIndexTmp,
                                      SplitRowInfo* splitRowInfoTmp)
{
    LogsDebug(" Fix col: %d th...", fixColIndexTmp);
    LogsDebug(" partition_cached_vectorbatch_[%d].size: %ld", partitionId, partition_cached_vectorbatch_[partitionId].size());
    if (splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp] < partition_cached_vectorbatch_[partitionId].size()) {
        auto colIndexTmpSchema = 0;
        colIndexTmpSchema = singlePartitionFlag ? fixed_width_array_idx_[fixColIndexTmp] : fixed_width_array_idx_[fixColIndexTmp] - 1;
        auto onceCopyLen = splitRowInfoTmp->onceCopyRow * (1 << column_type_id_[colIndexTmpSchema]);
        // 临时内存，拷贝拼接onceCopyRow批，用完释放
        void *ptr_value_tmp = static_cast<void *>(options_.allocator->alloc(onceCopyLen));
        std::shared_ptr<Buffer> ptr_value (new Buffer((uint8_t*)ptr_value_tmp, 0, onceCopyLen));
        void *ptr_validity_tmp = static_cast<void *>(options_.allocator->alloc(splitRowInfoTmp->onceCopyRow));
        std::shared_ptr<Buffer> ptr_validity (new Buffer((uint8_t*)ptr_validity_tmp, 0, SplitRowInfoTmp->onceCopyRow));
        if (nullptr == ptr_value->data_ || nullptr == ptr_validity->data_) {
            return -1;
        }
        // options_.spill_batch_row_num长度切割与拼接
        int destCopyedLength = 0;
        int memCopyLen = 0;
        int cacheBatchSize = 0;
        while (destCopyedLength < onceCopyLen) {
            if (splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp] >= partition_cached_vectorbatch_[partitionId].size()) { // 数组越界保护
                throw std::runtime_error("Columnar shuffle CacheBatchIndex out of bound.");
            }
            cacheBatchSize = partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][1]->size_;
            LogsDebug(" partitionId:%d splitRowInfoTmp.cacheBatchIndex[%d]:%d cacheBatchSize:%d onceCopyLen:%d destCopyedLength:%d splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp]:%d ",
                      partitionId,
                      fixColIndexTmp,
                      splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp],
                      cacheBatchSize,
                      onceCopyLen,
                      destCopyedLength,
                      splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp]);
            if ((onceCopyLen - destCopyedLength) >= (cacheBatchSize - splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp])) {
                memCopyLen = cacheBatchSize - splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp];
                memcpy((uint8_t*)(ptr_value->data_) + destCopyedLength,
                       partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][1]->data_ + splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp],
                       memCopyLen);
                // (destCopyedLength / (1 << column_type_id_[colIndexTmpSchema])) 等比例计算null数组偏移
                memcpy((uint8_t*)(ptr_validity->data_) + (destCopyedLength / (1 << column_type_id_[colIndexTmpSchema]))
                       partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][1]->data_ + (splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp] / (1 << column_type_id_[colIndexTmpSchema])),
                       memCOpyLen / (1 << column_type_id_[colIndexTmpSchema]));
                // 释放内存
                LogsDebug(" free memory Partition[%d] cacheindex[col%d]:%d ", partitionId, fixColIndexTmp, splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]);
                options_.allocator->free(partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][0]->data_,
                                         partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][0]->capacity_);
                options_.allocator->free(partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][1]->data_,
                                         partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][1]->capacity_);
                destCopyedLength += memCopyLen;
                splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp] += 1; // cacheBatchIndex下标后移
                splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp] = 0; // 初始化下一个cacheBatch的起始偏移
            } else {
                memCopyLen = onceCopyLen - destCopyedLength;
                memcpy((uint8_t*)(ptr_validity->data_) + (destCopyedLength / (1 << column_type_id_[colIndexTmpSchema])),
                    partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][1]->data_ + splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp],
                    memCopyLen);
                // (destCOpyEdLength / (1 << column_type_id_[colIndexTmpSchema])) 等比例计算null数组偏移
                memcpy((uint8_t*)(ptr_validity->data_) + (destCopyedLength / (1 << column_type_id_[colIndexTmpSchema])),
                    partition_cached_vectorbatch_[partitionId][splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp]][fixColIndexTmp][0]->data_ + (splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp] / (1 << column_type_id_[colIndexTmpSchema])),
                    memCopyLen / (1 << column_type_id_[colIndexTmpSchema]));
                destCopyedLength = onceCopyLen; // copy目标完成，结束while循环
                splitRowInfoTmp->cacheBatchCopyedLen[fixColIndexTmp] += memCopyLen;
            }
            LogsDebug(" memCopyedLen=%d.", memCopyLen);
            LogsDebug(" splitRowInfoTMp.cacheBatchIndex[fix_col%d]=%d   splitRowInfoTmp.cacheBatchCopyedLen[fix_col%d]=%d ",
                    fixColIndexTmp,
                    splitRowInfoTmp->cacheBatchIndex[fixColIndexTmp],
                    fixColIndexTmp,
                    splitRowInfoTmp->cacheBatchCOpyedLen[fixColIndexTmp]);
        }
        vec.set_values(ptr_value->data_, onceCopyLen);
        vec.set_nulls(ptr_validity->data_, splitRowInfoTMp->onceCopyRow);
        // 临时内存，拷贝拼接onceCopyRow批，用完释放
        options_.allocator->free(ptr_value->data_, ptr_value->capacity_);
        options_.allocator->free(ptr_validity->data_, ptr_validity->capacity_);
    }
    // partition_cached_vectorbatch_[partition_id][cache_index][col][0]代表ByteMap,
    // partition_cached_vectorbatch_[partition_id][cache_index][col][1]代表value
}

int Splitter::SerializingBinaryColumns(int32_t partitionId, spark::Vec& vec, int colIndex, int curBtch)
{
    LogsDebug(" vc_partition_array_buffers_[partitionId:%d][colIndex:%d] cacheBatchNum:%lu curBatch:%d", partitionId, colIndex, vc_partition_array_buffers_[partitionId][colIndex].size(), curBatch);
    VCBatchInfo vcb = vc_partition_array_buffers_[partitionId][colIndex][curBatch];
    int valuesTotalLen = vcb.getVcbTotalLen();
    std::vector<VCLocation> lst = vcb.getVcList();
    int itemsTotalLen = lst.size();
    auto OffsetsByte(std::make_unique<int32_t[]>(itemsTotalLen + 1));
    auto nullsByte(std::make_unique<char[]>(itemsTotalLen));
    auto valuesByte(std::make_unique<char[]>(valuesTotalLen));
    BytesGen(reinterpret_cast<uint64_t>(OffsetsByte.get())
             reinterpret_cast<uint64_t>(nullsByte.get()),
             reinterpret_cast<uint64_t>(valuesByte.get()), vcb);
    vec.set_values(valuesByte.get(), valuesTotalLen);
    // nulls add boolean array; serizelized tobytearray
    vec.set_nulls((char *)nullsByte.get(), itemsTotalLen);
    vec.set_offet(OffsetsByte.get(), (itemsTotalLen + 1) * sizeof(int32_t));
}

int Splitter::protoSpillPartition(int32_t partition_id, std::unique_ptr<BufferedOutputStream> &bufferStream) {
    LogsDebug(" Spill Pid:%d.", partition_id);
    SplitRowInfo splitRowInfoTmp;
    splitRowInfoTmp.copyedRow = 0;
    splitRowInfoTmp.remainCopyRow = partition_id_cnt_cache_[partition_id];
    splitRowInfoTmp.cacheBatchIndex.resize(fixed_width_array_idx_.size());
    splitRowInfoTmp.cacheBatchCopyedLen.resize(fixed_width_array_idx_.size());
    LogsDebug(" remainCopyRow %d ", splitRowInfoTmp.remainCopyRow);
    auto partition_cache_batch_num = partition_cached_vectorbatch_[partition_id].size();
    LogsDebug(" partition_cache_batch_num %lu ", partition_cache_batch_num);
    int curBatch = 0; // 变长cache batch下标，split已按照options_.spill_batch_row_num切割完成
    total_spill_row_num_ += splitRowInfoTmpremainCopyRow;
    while (0 < splitRowInfoTmp.remainCopyRow) {
        if (options_spill_batch_row_num < splitRowInfoTmp.remainCopyRow) {
            splitRowInfoTmp.onceCopyRow = options_.spill_batch_row_num;
        } else {
            splitRowInfoTmp.onceCopyRow = splitRowInfoTmp.remainCopyRow;
        }

        vecBatchProto->set_rowcnt(splitRowInfoTmp.onceCopyRow);
        vecBatchProto->set_veccnt(column_type_id_.size());
        int fixColIndexTmp = 0;
        for (size_t indexSchema = 0; indexSchema < column_type_id_.size(); indexSchema++) {
            spark::Vec * vec = vecBatchProto->add_vecs();
            switch (column_type_id_[indexSchema]) {
                case ShuffleTypeId::SHUFFLE_1BYTE:
                case SHuffleTypeId::SHUFFLE_2BYTE:
            }
        }
    }
}

