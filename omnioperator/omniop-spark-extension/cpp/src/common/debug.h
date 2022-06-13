/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#include <chrono>
#include <stdexcept>
#include "../../config.h"
#include "util/global_log.h"

#ifdef TRACE_RUNTIME
#define LogsTrace(format, ...)                                                                       \
    do {                                                                                             \
        printf("[TRACE][%s][%s][%d]:" format "\n", __FILE__, __FUNCTION__, __LINE__, ##__VA_ARGS__); \
    } while (0)
#else
#define LogsTrace(format, ...)
#endif


#define LogsDebug(format, ...)                                                                       \
    do {                                                                                             \
        if (static_cast<int>(LogType::LOG_DEBUG) >= GetLogLevel()) {                                 \
            char logBuf[GLOBAL_LOG_BUF_SIZE];                                                        \
            LogsInfoVargMacro(logBuf, format, ##__VA_ARGS__);                                        \
            std::string logString(logBuf);                                                           \
            Log(logString, LogType::LOG_DEBUG);                                                      \
        }                                                                                            \
    } while (0)


#define LogsInfo(format, ...)                                                                        \
    do {                                                                                             \
        if (static_cast<int>(LogType::LOG_INFO) >= GetLogLevel()) {                                  \
            char logBuf[GLOBAL_LOG_BUF_SIZE];                                                        \
            LogsInfoVargMacro(logBuf, format, ##__VA_ARGS__);                                        \
            std::string logString(logBuf);                                                           \
            Log(logString, LogType::LOG_INFO);                                                       \
        }                                                                                            \
    } while (0)

#define LogsWarn(format, ...)                                                                        \
    do {                                                                                             \
        if (static_cast<int>(LogType::LOG_WARN) >= GetLogLevel()) {                                  \
            char logBuf[GLOBAL_LOG_BUF_SIZE];                                                        \
            LogsInfoVargMacro(logBuf, format, ##__VA_ARGS__);                                        \
            std::string logString(logBuf);                                                           \
            Log(logString, LogType::LOG_WARN);                                                       \
        }                                                                                            \
    } while (0)

#define LogsError(format, ...)                                                                       \
    do {                                                                                             \
        if (static_cast<int>(LogType::LOG_ERROR) >= GetLogLevel()) {                                 \
            char logBuf[GLOBAL_LOG_BUF_SIZE];                                                        \
            LogsInfoVargMacro(logBuf, format, ##__VA_ARGS__);                                        \
            std::string logString(logBuf);                                                           \
            Log(logString, LogType::LOG_ERROR);                                                      \
        }                                                                                            \
    } while (0)