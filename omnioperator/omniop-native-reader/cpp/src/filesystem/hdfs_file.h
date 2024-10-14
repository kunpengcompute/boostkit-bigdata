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

#ifndef SPARK_THESTRAL_PLUGIN_HDFS_FILE_H
#define SPARK_THESTRAL_PLUGIN_HDFS_FILE_H

#include "file_interface.h"
#include "hdfs_filesystem.h"

namespace fs {

class HdfsReadableFile : public ReadableFile {

public:
    HdfsReadableFile(std::shared_ptr<HadoopFileSystem> fileSystemPtr, const std::string &path,
                     int64_t bufferSize = 0);

    ~HdfsReadableFile();

    Status Close() override;

    Status OpenFile() override;

    int64_t ReadAt(void *buffer, int32_t length, int64_t offset) override;

    int64_t GetFileSize() override;

    Status Seek(int64_t position) override;

    int64_t Read(void *buffer, int32_t length) override;

private:
    Status TryClose();

    std::shared_ptr<HadoopFileSystem> fileSystem_;

    const std::string &path_;

    int64_t bufferSize_;

    bool isOpen_ = false;

    hdfsFile file_;
};

class HdfsWriteableFile : public WriteableFile {

public:
    HdfsWriteableFile(std::shared_ptr<HadoopFileSystem> fileSystemPtr, const std::string &path, int64_t bufferSize = 0);

    ~HdfsWriteableFile();

    Status Close() override;

    Status OpenFile() override;

    int64_t Write(const void *buffer, int32_t length) override;

    int64_t GetFileSize() override;

private:
    Status TryClose();

    std::shared_ptr<HadoopFileSystem> fileSystem_;

    const std::string &path_;

    int64_t bufferSize_;

    bool isOpen_ = false;

    hdfsFile file_{};
};

} // namespace fs

#endif //SPARK_THESTRAL_PLUGIN_HDFS_FILE_H
