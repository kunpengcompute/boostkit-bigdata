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

int32_t BytesGen(uint64_t offsets, uint64_t nulls, uint64_t values, VCBatchInfo& vcb);

uint32_t reversebytes_uint32t(uint32_t value);

spark::CompressionKind GetCompressionType(const std::string& name);

int IsFileExist(const std::string path);

#endif //CPP_COMMON_H