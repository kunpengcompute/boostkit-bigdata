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

#include "orc/OrcFile.hh"

#include "eSDKOBS.h"

#define OBS_READ_SIZE 1024
#define OBS_KEY_SIZE 2048
#define OBS_TOKEN_SIZE 8192
#define OBS_PROTOCOL_SIZE 6

namespace orc {
    typedef struct ObsConfig {
        char hostName[OBS_KEY_SIZE];
        char accessKey[OBS_KEY_SIZE];
        char secretKey[OBS_KEY_SIZE];
        char token[OBS_TOKEN_SIZE];
        char bucket[OBS_KEY_SIZE];
        char objectKey[OBS_KEY_SIZE];
        uint32_t hostLen;
    } ObsConfig;

    std::unique_ptr<InputStream> readObsFile(const std::string& path, ObsConfig *obsInfo);

    class ObsFileInputStream : public InputStream {
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

        void getObsInfo(ObsConfig *obsInfo);

    public:
        ObsFileInputStream(std::string _filename, ObsConfig *obsInfo);

        uint64_t getLength() const override {
            return totalLength;
        }

        uint64_t getNaturalReadSize() const override {
            return READ_SIZE;
        }

        void read(void* buf, uint64_t length, uint64_t offset) override;

        const std::string& getName() const override {
            return filename;
        }

        ~ObsFileInputStream() override {
        }
    };
}
