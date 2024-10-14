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

#ifndef SPARK_THESTRAL_PLUGIN_FILE_INTERFACE_H
#define SPARK_THESTRAL_PLUGIN_FILE_INTERFACE_H

#include "status.h"

namespace fs {

class ReadableFile {
public:
    // Virtual destructor
    virtual ~ReadableFile() = default;

    // Close the file
    virtual Status Close() = 0;

    // Open the file
    virtual Status OpenFile() = 0;

    // Read data from the specified offset into the buffer with the given length
    virtual int64_t ReadAt(void *buffer, int32_t length, int64_t offset) = 0;

    // Get the size of the file
    virtual int64_t GetFileSize() = 0;

    // Set the read position within the file
    virtual Status Seek(int64_t position) = 0;

    // Read data from the current position into the buffer with the given length
    virtual int64_t Read(void *buffer, int32_t length) = 0;
};

class WriteableFile {
public:
    // Virtual destructor
    virtual ~WriteableFile() = default;

    // Close the file
    virtual Status Close() = 0;

    // Open the file
    virtual Status OpenFile() = 0;

    // Get the size of the file
    virtual int64_t GetFileSize() = 0;

    // Write data from the current position into the buffer with the given
    // length
    virtual int64_t Write(const void *buffer, int32_t length) = 0;
};

} // namespace fs


#endif //SPARK_THESTRAL_PLUGIN_FILE_INTERFACE_H
