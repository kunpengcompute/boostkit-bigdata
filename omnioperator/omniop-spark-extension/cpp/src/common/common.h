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

#ifndef CPP_COMMON_H
#define CPP_COMMON_H

#include <vector/vector_common.h>
#include <cstring>
#include <chrono>
#include <memory>
#include <list>
#include <set>
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>

#include "../io/Common.hh"
#include "../utils/macros.h"
#include "BinaryLocation.h"
#include "debug.h"
#include "Buffer.h"
#include "BinaryLocation.h"

template<bool hasNull>
int32_t BytesGen(uint64_t offsetsAddr, std::string &nullStr, uint64_t valuesAddr, VCBatchInfo& vcb)
{
    int32_t* offsets = reinterpret_cast<int32_t *>(offsetsAddr);
    char *nulls = nullptr;
    char* values = reinterpret_cast<char *>(valuesAddr);
    std::vector<VCLocation> &lst = vcb.getVcList();
    int itemsTotalLen = lst.size();

    int valueTotalLen = 0;
    if constexpr (hasNull) {
        nullStr.resize(itemsTotalLen, 0);
        nulls = nullStr.data();
    }

    for (int i = 0; i < itemsTotalLen; i++) {
        char* addr = reinterpret_cast<char *>(lst[i].get_vc_addr());
        int len = lst[i].get_vc_len();
        if (i == 0) {
            offsets[0] = 0;
        } else {
            offsets[i] = offsets[i -1] + lst[i - 1].get_vc_len();
        }
        if constexpr(hasNull) {
            if (lst[i].get_is_null()) {
                nulls[i] = 1;
            }
        }

        if (len != 0) {
            memcpy_s((char *) (values + offsets[i]), len, addr, len);
            valueTotalLen += len;
        }
    }
    offsets[itemsTotalLen] = offsets[itemsTotalLen -1] + lst[itemsTotalLen - 1].get_vc_len();
    return valueTotalLen;
}

uint32_t reversebytes_uint32t(uint32_t value);

spark::CompressionKind GetCompressionType(const std::string& name);

int IsFileExist(const std::string path);

#endif //CPP_COMMON_H