/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef DEBUG_H
#define DEBUG_H

#include <chrono>
#include <stdexcept>

#ifdef TRACE_RUNTIME
#define LOG_TRACE(format, ...)                                                                       \
    do {                                                                                             \
        printf("[TRACE][%s][%s][%d]:" format "\n", __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__); \
    } while (0)
#else
#define LOG_TRACE(format, ...)
#endif

#if defined(DEBUG_RUNTIME) || defined(TRACE_RUNTIME)
#define LOG_DEBUG(format, ...)                                                                       \
    do {                                                                                             \
        printf("[DEBUG][%s][%s][%d]:" format "\n", __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__); \
    } while (0)
#else
#define LOG_DEBUG(format, ...)
#endif

#define LOG_INFO(format, ...)                                                                       \
    do {                                                                                            \
        printf("[INFO][%s][%s][%d]:" format "\n", __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__); \
    } while (0)

#define LOG_WARN(format, ...)                                                                       \
    do {                                                                                            \
        printf("[WARN][%s][%s][%d]:" format "\n", __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__); \
    } while (0)

#define LOG_ERROR(format, ...)                                                                       \
    do {                                                                                             \
        printf("[ERROR][%s][%s][%d]:" format "\n", __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__); \
    } while (0)

#endif // DEBUG_H