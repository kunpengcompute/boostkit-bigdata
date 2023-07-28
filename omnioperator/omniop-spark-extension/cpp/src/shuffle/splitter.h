/**
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef CPP_SPLITTER_H
#define CPP_SPLITTER_H

#include <vector/vector_common.h>
#include <cstring>
#include <vector>
#include <chrono>
#include <memory>
#include <list>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>

#include "type.h"
#include "../io/ColumnWriter.hh"
#include "../common/common.h"
#include "vec_data.pb.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"

using namespace std;
using namespace spark;
using namespace google::protobuf::io;
using namespace omniruntime::vec;
using namespace omniruntime::type;

struct SplitRowInfo {
    uint32_t copyedRow = 0;
    uint32_t onceCopyRow = 0;
    uint64_t remainCopyRow = 0;
    vector<uint32_t> cacheBatchIndex; // 记录各定长列的溢写Batch下标
    vector<uint32_t> cacheBatchCopyedLen; // 记录各定长列的溢写Batch内部偏移
};

class Splitter {

    virtual int DoSplit(VectorBatch& vb);

    int WriteDataFileProto();

    std::shared_ptr<Buffer> CaculateSpilledTmpFilePartitionOffsets();

    void SerializingFixedColumns(int32_t partitionId,
                                spark::Vec& vec,
                                int fixColIndexTmp,
                                SplitRowInfo* splitRowInfoTmp);

    void SerializingBinaryColumns(int32_t partitionId,
                                spark::Vec& vec,
                                int colIndex,
                                int curBatch);

    int protoSpillPartition(int32_t partition_id, std::unique_ptr<BufferedOutputStream> &bufferStream);

    int32_t ProtoWritePartition(int32_t partition_id, std::unique_ptr<BufferedOutputStream> &bufferStream, void *bufferOut, int32_t &sizeOut);

    int ComputeAndCountPartitionId(VectorBatch& vb);

    int AllocatePartitionBuffers(int32_t partition_id, int32_t new_size);

    int SplitFixedWidthValueBuffer(VectorBatch& vb);

    int SplitFixedWidthValidityBuffer(VectorBatch& vb);

    int SplitBinaryArray(VectorBatch& vb);

    int CacheVectorBatch(int32_t partition_id, bool reset_buffers);

    void ToSplitterTypeId(int num_cols);

    void CastOmniToShuffleType(DataTypeId omniType, ShuffleTypeId shuffleType);

    void MergeSpilled();

    void WriteSplit();

    bool isSpill = false;
    std::vector<int32_t> partition_id_; // 记录当前vb每一行的pid
    std::vector<int32_t> partition_id_cnt_cur_; // 统计不同partition记录的行数(当前处理中的vb)
    std::vector<uint64_t> partition_id_cnt_cache_; // 统计不同partition记录的行数，cache住的
    // column number
    uint32_t num_row_splited_; // cached row number
    uint64_t cached_vectorbatch_size_; // cache total vectorbatch size in bytes
    uint64_t current_fixed_alloc_buffer_size_ = 0;
    std::vector<uint32_t> fixed_valueBuffer_size_; // 当前定长omniAlloc已经分配value内存大小byte
    std::vector<uint32_t> fixed_nullBuffer_size_; // 当前定长omniAlloc已分配null内存大小byte
    // int32_t num_cache_vector_;
    std::vector<ShuffleTypeId> column_type_id_; // 各列映射SHUFFLE类型，schema列id序列
    std::vector<std::vector<uint8_t*>> partition_fixed_width_validity_addrs_;
    std::vector<std::vector<uint8_t*>> partition_fixed_width_value_addrs_; //
    std::vector<std::vector<std::vector<std::shared_ptr<Buffer>>>> partition_fixed_width_buffers_;
    std::vector<std::vector<std::shared_ptr<Buffer>>> partition_binary_builders_;
    std::vector<int32_t> partition_buffer_size_; // 各分区的buffer大小
    std::vector<int32_t> fixed_width_array_idx_; // 记录各定长类型列的序号，VB 列id序列
    std::vector<int32_t> binary_array_idx_; //记录各变长类型列序号
    std::vector<int32_t> partition_buffer_idx_base_; //当前已缓存的各partition行数据记录，用于定位缓冲buffer当前可用位置
    std::vector<int32_t> partition_buffer_idx_offset_; //split定长列时用于统计offset的临时变量
    std::vector<uint32_t> partition_serialization_size_; // 记录序列化后的各partition大小，用于stop返回partition偏移 in bytes

    std::vector<bool> input_fixed_width_has_null_; // 定长列是否含有null标志数组

    // configured local dirs for spilled file
    int32_t dir_selection_ = 0;
    std::vector<int32_t> sub_dir_selection_;
    std::vector<std::string> configured_dirs_;

    std::vector<std::vector<std::vector<std::vector<std::shared_ptr<Buffer>>>>> partition_cached_vectorbatch_;
    /*
     * varchar buffers:
     *  partition_array_buffers_[partition_id][col_id][varcharBatch_id]
     * 
     */
    std::vector<std::vector<std::vector<VCBatchInfo>>> vc_partition_array_buffers_;

    int64_t total_bytes_written_ = 0;
    int64_t total_bytes_spilled_ = 0;
    int64_t total_write_time_ = 0;
    int64_t total_spill_time_ = 0;
    int64_t total_compute_pid_time_ = 0;
    int64_t total_spill_row_num_ = 0;
    std::vector<int64_t> partition_lengths_;

private:
    void ReleaseVarcharVector()
    {
        std::set<BaseVector *>::iterator it;
        for (it = varcharVectorCache.begin(); it != varcharVectorCache.end(); it++) {
            delete *it;
        }
        varcharVectorCache.clear();
    }

    void ReleaseVectorBatch(VectorBatch *vb)
    {
        int vectorCnt = vb->GetVectorCount();
        std::set<BaseVector *> vectorAddress; // vector deduplication
        for (int vecIndex = 0; vecIndex < vectorCnt; vecIndex++) {
            BaseVector *vector = vb->Get(vecIndex);
            // not varchar vector can be released;
            if (varcharVectorCache.find(vector) == varcharVectorCache.end() &&
                vectorAddress.find(vector) == vectorAddress.end()) {
                vectorAddress.insert(vector);
                delete vector;
            }
        }
        vectorAddress.clear();
        delete vb;
    }

    std::set<BaseVector *> varcharVectorCache;
    bool first_vector_batch_ = false;
    std::vector<DataTypeId> vector_batch_col_types_;
    InputDataTypes input_col_types;
    std::vector<int32_t> binary_array_empirical_size_;

public:
    bool singlePartitionFlag = false;
    int32_t num_partitions_;
    SplitOptions options_;
    // 分区数
    int32_t num_fields_;

    std::map<std::string, std::shared_ptr<Buffer>> spilled_tmp_files_info_;

    spark::VecBatch *vecBatchProto = new VecBatch(); // protobuf 序列化对象结构

    virtual int Split_Init();

    virtual int Split(VectorBatch& vb);

    int Stop();

    int SpillToTmpFile();

    Splitter(InputDataTypes inputDataTypes,
             int32_t num_cols,
             int32_t num_partitions,
             SplitOptions options,
             bool flag);

    static std::shared_ptr<Splitter> Make(
            const std::string &short_name,
            InputDataTypes inputDataTypes,
            int32_t num_cols,
            int num_partitions,
            SplitOptions options); 
    
    std::string NextSpilledFileDir();

    int DeleteSpilledTmpFile();

    int64_t TotalBytesWritten() const { return total_bytes_written_; }

    int64_t TotalBytesSpilled() const { return total_bytes_spilled_; }

    int64_t TotalWriteTime() const { return total_write_time_; }

    int64_t TotalSpillTime() const { return total_spill_time_; }

    int64_t TotalComputePidTime() const { return total_compute_pid_time_; }

    const std::vector<int64_t>& PartitionLengths() const { return partition_lengths_; }
};


#endif // CPP_SPLITTER_H
