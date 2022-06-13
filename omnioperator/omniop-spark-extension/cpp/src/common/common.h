/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
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

int32_t BytesGen(uint64_t offsets, uint64_t nulls, uint64_t values, VCBatchInfo& vcb);

uint32_t reversebytes_uint32t(uint32_t value);

spark::CompressionKind GetCompressionType(const std::string& name);

int IsFileExist(const std::string path);

void ReleaseVectorBatch(omniruntime::vec::VectorBatch& vb);

#endif //CPP_COMMON_H