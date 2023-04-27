/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "common.h"

using namespace omniruntime::vec;

int32_t BytesGen(uint64_t offsetsAddr, uint64_t nullsAddr, uint64_t valuesAddr, VCBatchInfo& vcb)
{
    int32_t* offsets = reinterpret_cast<int32_t *>(offsetsAddr);
    char *nulls = reinterpret_cast<char *>(nullsAddr);
    char* values = reinterpret_cast<char *>(valuesAddr);
    std::vector<VCLocation> &lst = vcb.getVcList();
    int itemsTotalLen = lst.size();
    int valueTotalLen = 0;
    for (int i = 0; i < itemsTotalLen; i++) {
        char* addr = reinterpret_cast<char *>(lst[i].get_vc_addr());
        int len = lst[i].get_vc_len();
        if (i == 0) {
            offsets[0] = 0;
        } else {
            offsets[i] = offsets[i -1] + lst[i - 1].get_vc_len();
        }
        if (lst[i].get_is_null()) {
            nulls[i] = 1;
        } else {
            nulls[i] = 0;
        }
        if (len != 0) {
            memcpy((char *) (values + offsets[i]), addr, len);
            valueTotalLen += len;
        }
    }
    offsets[itemsTotalLen] = offsets[itemsTotalLen -1] + lst[itemsTotalLen - 1].get_vc_len();
    return valueTotalLen;
}

uint32_t reversebytes_uint32t(uint32_t const value)
{
    return (value & 0x000000FFU) << 24 | (value & 0x0000FF00U) << 8 | (value & 0x00FF0000U) >> 8 | (value & 0xFF000000U) >> 24;
}

spark::CompressionKind GetCompressionType(const std::string& name) {
    if (name == "uncompressed") {
        return spark::CompressionKind::CompressionKind_NONE;
    } else if (name == "zlib") {
        return spark::CompressionKind::CompressionKind_ZLIB;
    } else if (name == "snappy") {
        return spark::CompressionKind::CompressionKind_SNAPPY;
    } else if (name == "lz4") {
        return spark::CompressionKind::CompressionKind_LZ4;
    } else if (name == "zstd") {
        return spark::CompressionKind::CompressionKind_ZSTD;
    } else {
        throw std::logic_error("compression codec not supported");
    }
}

// return: 1 文件存在可访问
//         0 文件不存在或不能访问
int IsFileExist(const std::string path)
{
    return !access(path.c_str(), F_OK);
}
