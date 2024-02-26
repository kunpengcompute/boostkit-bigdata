/**
 * Copyright (C) 2023. Huawei Technologies Co., Ltd. All rights reserved.
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

#ifndef PARQURTOBSFILE_H
#define PARQURTOBSFILE_H

#include "eSDKOBS.h"
#include "common/debug.h"
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/buffer.h>
#include <arrow/memory_pool.h>

#define OBS_READ_SIZE 1024
#define OBS_KEY_SIZE 2048
#define OBS_TOKEN_SIZE 8192
#define OBS_PROTOCOL_SIZE 6

using namespace arrow::io;
using namespace arrow;

namespace omniruntime::reader {
    typedef struct ObsConfig {
        char hostName[OBS_KEY_SIZE];
        char accessKey[OBS_KEY_SIZE];
        char secretKey[OBS_KEY_SIZE];
        char token[OBS_TOKEN_SIZE];
        char bucket[OBS_KEY_SIZE];
        char objectKey[OBS_KEY_SIZE];
        uint32_t hostLen;
    } ObsConfig;

    std::shared_ptr<RandomAccessFile> readObsFile(const std::string& path, ObsConfig *obsInfo);

    class ObsReadableFile : public RandomAccessFile {
    private:
        obs_options option;
        obs_object_info objectInfo;
        obs_get_conditions conditions;
        ObsConfig obsInfo;

        std::string filename;
        uint64_t totalLength;
        const uint64_t READ_SIZE = OBS_READ_SIZE * OBS_READ_SIZE;

        static obs_status obsInitStatus;

        static obs_status obsInit();

        bool is_open_ = true;

        void getObsInfo(ObsConfig *obsInfo);

    public:
        ObsReadableFile(std::string _filename, ObsConfig *obsInfo);

        Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override;

        Result<int64_t> ReadAt(int64_t offset, int64_t length, void* buf) override;

        Status Close() override {
            if (is_open_) {
                is_open_ = false;
                return Status::OK();
            }
            return Status::OK();
        }

        bool closed() const override {
            return !is_open_;
        }

        Status CheckClosed() {
            if (!is_open_) {
                return Status::Invalid("Operation on closed OBS file");
            }
            return Status::OK();
        }

        Result<int64_t> GetSize() override {
            return totalLength;
        }

        Result<int64_t> Read(int64_t nbytes, void* out) override {
            return Result<int64_t>(Status::NotImplemented("Not implemented"));
        }

        Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
            return Result<std::shared_ptr<arrow::Buffer>>(Status::NotImplemented("Not implemented"));
        }

        Status Seek(int64_t position) override {
            return Status::NotImplemented("Not implemented");
        }

        Result<int64_t> Tell() const override {
            return Result<int64_t>(Status::NotImplemented("Not implemented"));
        }

        ~ObsReadableFile() {}
    };
}

#endif
