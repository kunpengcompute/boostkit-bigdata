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

#include "OrcObsFile.hh"

#include <cstdio>

#include "../common/debug.h"
#include "securec.h"

namespace orc {
    std::unique_ptr<InputStream> readObsFile(const std::string& path, ObsConfig *obsInfo) {
        return std::unique_ptr<InputStream>(new ObsFileInputStream(path, obsInfo));
    }

    typedef struct CallbackData {
        char *buf;
        uint64_t length;
        uint64_t readLength;
        obs_status retStatus;
    } CallbackData;

    obs_status responsePropertiesCallback(const obs_response_properties *properties, void *data) {
        if (NULL == properties) {
            LogsError("OBS error, obs_response_properties is null!");
            return OBS_STATUS_ErrorUnknown;
        }
        CallbackData *ret = (CallbackData *)data;
        ret->length = properties->content_length;
        return OBS_STATUS_OK;
    }

    void commonErrorHandle(const obs_error_details *error) {
        if (!error) {
            return;
        }
        if (error->message) {
            LogsError("OBS error message: %s", error->message);
        }
        if (error->resource) {
            LogsError("OBS error resource: %s", error->resource);
        }
        if (error->further_details) {
            LogsError("OBS error further details: %s", error->further_details);
        }
        if (error->extra_details_count) {
            LogsError("OBS error extra details:");
            for (int i = 0; i < error->extra_details_count; i++) {
                LogsError("[name] %s: [value] %s", error->extra_details[i].name, error->extra_details[i].value);
            }
        }
    }

    void responseCompleteCallback(obs_status status, const obs_error_details *error, void *data) {
        if (data) {
            CallbackData *ret = (CallbackData *)data;
            ret->retStatus = status;
        }
        commonErrorHandle(error);
    }

    obs_status getObjectDataCallback(int buffer_size, const char *buffer, void *data) {
        CallbackData *callbackData = (CallbackData *)data;
        int read = buffer_size;
        if (callbackData->readLength + buffer_size > callbackData->length) {
            LogsError("OBS get object failed, read buffer size(%d) is bigger than the remaining buffer\
                        (totalLength[%ld] - readLength[%ld] = %ld).\n",
                        buffer_size, callbackData->length, callbackData->readLength,
                        callbackData->length - callbackData->readLength);
            return OBS_STATUS_InvalidParameter;
        }
        memcpy_s(callbackData->buf + callbackData->readLength, read, buffer, read);
        callbackData->readLength += read;
        return OBS_STATUS_OK;
    }

    obs_status ObsFileInputStream::obsInit() {
        obs_status status = OBS_STATUS_BUTT;
        status = obs_initialize(OBS_INIT_ALL);
        if (OBS_STATUS_OK != status) {
            LogsError("OBS initialize failed(%s).", obs_get_status_name(status));
            throw ParseError("OBS initialize failed.");
        }
        return status;
    }

    obs_status ObsFileInputStream::obsInitStatus = obsInit();

    void ObsFileInputStream::getObsInfo(ObsConfig *obsConf) {
        memcpy_s(&obsInfo, sizeof(ObsConfig), obsConf, sizeof(ObsConfig));

        std::string obsFilename = filename.substr(OBS_PROTOCOL_SIZE);
        uint64_t splitNum = obsFilename.find_first_of("/");
        std::string bucket = obsFilename.substr(0, splitNum);
        uint32_t bucketLen = bucket.length();
        strcpy_s(obsInfo.bucket, bucketLen + 1, bucket.c_str());
        option.bucket_options.bucket_name = obsInfo.bucket;

        memset_s(&objectInfo, sizeof(obs_object_info), 0, sizeof(obs_object_info));
        std::string key = obsFilename.substr(splitNum + 1);
        strcpy_s(obsInfo.objectKey, key.length() + 1, key.c_str());
        objectInfo.key = obsInfo.objectKey;

        if (obsInfo.hostLen > bucketLen && strncmp(obsInfo.hostName, obsInfo.bucket, bucketLen) == 0) {
            obsInfo.hostLen = obsInfo.hostLen - bucketLen - 1;
            memcpy_s(obsInfo.hostName, obsInfo.hostLen, obsInfo.hostName + bucketLen + 1, obsInfo.hostLen);
            obsInfo.hostName[obsInfo.hostLen - 1] = '\0';
        }

        option.bucket_options.host_name = obsInfo.hostName;
        option.bucket_options.access_key = obsInfo.accessKey;
        option.bucket_options.secret_access_key = obsInfo.secretKey;
        option.bucket_options.token = obsInfo.token;
    }

    ObsFileInputStream::ObsFileInputStream(std::string _filename, ObsConfig *obsInfo) {
        filename = _filename;
        init_obs_options(&option);

        getObsInfo(obsInfo);

        CallbackData data;
        data.retStatus = OBS_STATUS_BUTT;
        data.length = 0;
        obs_response_handler responseHandler = {
            &responsePropertiesCallback,
            &responseCompleteCallback
        };

        get_object_metadata(&option, &objectInfo, 0, &responseHandler, &data);
        if (OBS_STATUS_OK != data.retStatus) {
            throw ParseError("get obs object(" + filename + ") metadata failed.");
        }
        totalLength = data.length;

        memset_s(&conditions, sizeof(obs_get_conditions), 0, sizeof(obs_get_conditions));
        init_get_properties(&conditions);
    }

    void ObsFileInputStream::read(void *buf, uint64_t length, uint64_t offset) {
        if (!buf) {
            throw ParseError("Buffer is null.");
        }
        conditions.start_byte = offset;
        conditions.byte_count = length;

        obs_get_object_handler handler = {
            { &responsePropertiesCallback,
              &responseCompleteCallback},
            &getObjectDataCallback
        };

        CallbackData data;
        data.retStatus = OBS_STATUS_BUTT;
        data.length = length;
        data.readLength = 0;
        data.buf = reinterpret_cast<char*>(buf);
        do {
            // the data.buf offset is processed in the callback function getObjectDataCallback
            uint64_t tmpRead = data.readLength;
            get_object(&option, &objectInfo, &conditions, 0, &handler, &data);
            if (OBS_STATUS_OK != data.retStatus) {
                LogsError("get obs object failed, length=%ld, readLength=%ld, offset=%ld",
                            data.length, data.readLength, offset);
                throw ParseError("get obs object(" + filename + ") failed.");
            }

            // read data buffer size = 0, no more remaining data need to read
            if (tmpRead == data.readLength) {
                break;
            }
            conditions.start_byte = offset + data.readLength;
            conditions.byte_count = length - data.readLength;
        } while (data.readLength < length);
    }
}
