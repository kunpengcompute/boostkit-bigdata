/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#ifndef SPARK_THESTRAL_PLUGIN_BINARYLOCATION_H
#define SPARK_THESTRAL_PLUGIN_BINARYLOCATION_H
class VCLocation {
public:
    VCLocation(uint64_t vc_addr, uint32_t vc_len)
            : vc_len(vc_len), vc_addr(vc_addr) {
            }
    ~VCLocation() {
    }
    uint32_t get_vc_len() {
        return vc_len;
    }

    uint64_t get_vc_addr() {
        return vc_addr;
    }

 public:
     uint32_t vc_len;
     uint64_t vc_addr;
};

class VCBatchInfo {
public:
    VCBatchInfo() {
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