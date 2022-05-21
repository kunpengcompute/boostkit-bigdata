/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
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
using namespace omniruntime::mem;

struct SplitRowInfo {
    uint32_t copyedRow = 0;
    uint32_t onceCopyRow = 0;
    uint32_t remainCopyRow = 0;
    vector<uint32_t> cacheBatchIndex; // 记录各定长列的溢写Batch下标
    vector<uint32_t> cacheBatchCopyedLen; // 记录各定长列的溢写Batch内部偏移
};


