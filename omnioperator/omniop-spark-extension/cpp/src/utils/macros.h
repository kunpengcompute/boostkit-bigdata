/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#ifndef SPARK_THESTRAL_PLUGIN_MACROS_H
#define SPARK_THESTRAL_PLUGIN_MACROS_H

#pragma once

#include <time.h>
#include <chrono>

#define TIME_NANO_OR_RAISE(time, expr)                                                     \
    do {                                                                                   \
        auto start = std::chrono::steady_clock::now();                                     \
        (expr);                                                                            \
        auto end = std::chrono::steady_clock::now();                                       \
        time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count(); \
    } while (false);

#endif //SPARK_THESTRAL_PLUGIN_MACROS_H