/**
 * Copyright (C) 2025-2025. Huawei Technologies Co., Ltd. All rights reserved.
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

#include "Directories.h"
#include "sys/stat.h"

namespace common {
    int createDirectories(const std::string &path) {
        size_t pos = path.find_first_of("/");
        while (pos != std::string::npos) {
            std::string dir = path.substr(0, pos);
            if (dir.length() > 0) {
                struct stat st = {0};
                if (stat(dir.c_str(), &st) == -1) {
                    if (mkdir(dir.c_str(), 0755) == -1 && errno != EEXIST) {
                        return -1;
                    }
                }
            }
            pos = path.find_first_of("/", pos + 1);
        }
        struct stat st = {0};
        if (stat(path.c_str(), &st) == -1) {
            if (mkdir(path.c_str(), 0755) == -1 && errno != EEXIST) {
                return -1;
            }
        }
        return 0;
    }

    std::string getParentPath(const std::string &path) {
        size_t found = path.find_last_of("/");
        if (found != std::string::npos) {
            return path.substr(0, found);
        }
        return "";
    }
}