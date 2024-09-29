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

#ifndef SPARK_THESTRAL_PLUGIN_STATUS_H
#define SPARK_THESTRAL_PLUGIN_STATUS_H

#include <string>

namespace fs {

// Enum to represent different status codes
enum class StatusCode : char {
    OK = 0,
    FSError = 1,
    IOError = 2,
    UnknownError = 3
};

// Struct to hold status code and message
struct State {
    StatusCode code; // Status code
    std::string msg; // Status message
};

// Class to represent status
class Status {

public:
    // Default constructor
    Status() noexcept: state_(nullptr) {}

    // Constructor with status code and message
    Status(StatusCode code, const std::string &msg) {
        State *state = new State();
        state->code = code;
        state->msg = msg;
        this->state_ = state;
    }

    // Destructor
    ~Status() noexcept {
        delete state_;
        state_ = nullptr;
    }

    // Create a status from status code and message
    static Status FromMsg(StatusCode code, const std::string &msg) {
        return Status(code, msg);
    }

    // Create a file system error status with message
    static Status FSError(const std::string &msg) {
        return Status::FromMsg(StatusCode::FSError, msg);
    }

    // Create an I/O error status with message
    static Status IOError(const std::string &msg) {
        return Status::FromMsg(StatusCode::IOError, msg);
    }

    // Create an unknown error status with message
    static Status UnknownError(const std::string &msg) {
        return Status::FromMsg(StatusCode::UnknownError, msg);
    }

    // Create an OK status
    static Status OK() {
        return Status();
    }

    // Check if the status is OK
    constexpr bool IsOk() const {
        if (state_ == nullptr || state_->code == StatusCode::OK) {
            return true;
        }
        return false;
    }

    // Get the status as a string
    std::string ToString() const;

    // Get the status code as a string
    static std::string CodeAsString(StatusCode);

private:
    // Pointer to the status state
    State *state_;
};
}

#endif //SPARK_THESTRAL_PLUGIN_STATUS_H