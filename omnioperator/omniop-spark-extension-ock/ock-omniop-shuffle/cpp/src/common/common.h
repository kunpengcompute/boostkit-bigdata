/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
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

#include "debug.h"

#ifndef LIKELY
#define LIKELY(x) __builtin_expect(!!(x), 1)
#endif

#ifndef UNLIKELY
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#endif

#endif // CPP_COMMON_H