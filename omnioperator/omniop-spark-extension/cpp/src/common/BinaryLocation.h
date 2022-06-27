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

#ifndef SPARK_THESTRAL_PLUGIN_BINARYLOCATION_H
#define SPARK_THESTRAL_PLUGIN_BINARYLOCATION_H
class VCLocation {
public:
    VCLocation(uint64_t vc_addr, uint32_t vc_len, bool isnull)
            : vc_len(vc_len), vc_addr(vc_addr), is_null(isnull) {
            }
    ~VCLocation() {
    }
    uint32_t get_vc_len() {
        return vc_len;
    }

    uint64_t get_vc_addr() {
        return vc_addr;
    }

    bool get_is_null() {
        return is_null;
    }

public:
    uint32_t vc_len;
    uint64_t vc_addr;
    bool is_null;
};

class VCBatchInfo {
public:
    VCBatchInfo(uint32_t vcb_capacity) {
        this->vc_list.reserve(vcb_capacity);
        this->vcb_capacity =  vcb_capacity;
        this->vcb_total_len = 0;
    }

    ~VCBatchInfo() {
        vc_list.clear();
    }

    uint32_t getVcbCapacity() {
        return vcb_capacity;
    }

    uint32_t getVcbTotalLen() {
        return vcb_total_len;
    }

    std::vector<VCLocation> &getVcList() {
        return vc_list;
    }

public:
    uint32_t vcb_capacity;
    uint32_t vcb_total_len;
    std::vector<VCLocation> vc_list;


};
#endif //SPARK_THESTRAL_PLUGIN_BINARYLOCATION_H