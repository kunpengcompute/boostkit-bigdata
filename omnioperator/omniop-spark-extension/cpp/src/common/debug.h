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

#if defined(TRACE_RUNTIME) || defined(DEBUG_RUNTIME)
#define LogsDebug(format, ...)                                                                       \
    do {                                                                                             \
        if (static_cast<int>(LogType::LOG_DEBUG) >= GetLogLevel()) {                                 \
            char logBuf[GLOBAL_LOG_BUF_SIZE];                                                        \
            LogsInfoVargMacro(logBuf, format, ##__VA_ARGS__);                                        \
            std::string logString(logBuf);                                                           \
            Log(logString, LogType::LOG_DEBUG);                                                      \
        }                                                                                            \
    } while (0)
#else
#define LogsDebug(format, ...)
#endif

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