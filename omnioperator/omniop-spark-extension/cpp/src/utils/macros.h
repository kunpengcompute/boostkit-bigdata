/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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