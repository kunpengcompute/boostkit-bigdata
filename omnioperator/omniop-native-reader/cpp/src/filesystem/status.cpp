/**
 * Copyright (C) 2022-2023. Huawei Technologies Co., Ltd. All rights reserved.
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

#include "status.h"

namespace fs {

std::string Status::ToString() const {
    std::string result(CodeAsString(state_->code));
    result += ": ";
    result += state_->msg;
    return result;
}

std::string Status::CodeAsString(StatusCode code) {
    const char *type;
    switch (code) {
        case StatusCode::OK:
            type = "OK";
            break;
        case StatusCode::FSError:
            type = "FileSystem error";
            break;
        case StatusCode::IOError:
            type = "IO error";
            break;
        default:
            type = "Unknown";
            break;
    }
    return std::string(type);
}


}