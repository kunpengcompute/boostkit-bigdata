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

#include "iostream"
#include "chrono"
#include "map"
#include "mutex"
#include "hdfs_filesystem.h"
#include "io_exception.h"

namespace fs {

void HdfsOptions::ConfigureHost(const std::string &host) {
    this->host_ = host;
}

void HdfsOptions::ConfigurePort(int port) {
    this->port_ = port;
}

bool HdfsOptions::Equals(const HdfsOptions &other) const {
    return (this->host_ == other.host_ && this->port_ == other.port_);
}

HadoopFileSystem::HadoopFileSystem(HdfsOptions &options) {
    this->options_ = options;
    Status st = this->Init();
    if (!st.IsOk()) {
        throw IOException(st.ToString());
    }
}

HadoopFileSystem::~HadoopFileSystem() = default;

hdfsFS HadoopFileSystem::getFileSystem() {
    return this->fs_;
}

HdfsOptions HadoopFileSystem::getOptions() const {
    return this->options_;
}

bool HadoopFileSystem::Equals(const FileSystem &other) const {
    if (this == &other) {
        return true;
    }
    if (other.type_name() != type_name()) {
        return false;
    }
    // todo reinterpret_cast 能不能转换类型，多态场景
    const auto &hdfs = reinterpret_cast<const HadoopFileSystem &>(other);
    return getOptions().Equals(hdfs.getOptions());
}

FileInfo HadoopFileSystem::GetFileInfo(const std::string &path) {
    hdfsFileInfo *fileInfo = hdfsGetPathInfo(fs_, path.c_str());
    if (fileInfo == nullptr) {
        throw IOException(Status::FSError("Fail to get file info").ToString());
    }
    FileInfo info;
    if (fileInfo->mKind == kObjectKindFile) {
        info.setType(FileType::File);
    } else if (fileInfo->mKind == kObjectKindDirectory) {
        info.setType(FileType::Directory);
    } else {
        info.setType(FileType::Unknown);
    }
    info.setPath(path);
    info.setSize(fileInfo->mSize);
    info.setMtime(std::chrono::system_clock::from_time_t(fileInfo->mLastMod));
    return info;
}

Status HadoopFileSystem::Close() {
    if (hdfsDisconnect(fs_) == 0) {
        return Status::OK();
    }
    return Status::FSError("Fail to close hdfs filesystem");
}

Status HadoopFileSystem::Init() {
    struct hdfsBuilder *bld = hdfsNewBuilder();
    if (bld == nullptr) {
        return Status::FSError("Fail to create hdfs builder");
    }
    hdfsBuilderSetNameNode(bld, options_.host_.c_str());
    hdfsBuilderSetNameNodePort(bld, options_.port_);
    hdfsBuilderSetForceNewInstance(bld);
    hdfsFS fileSystem = hdfsBuilderConnect(bld);
    if (fileSystem == nullptr) {
        return Status::FSError("Fail to connect hdfs filesystem");
    }
    this->fs_ = fileSystem;
    return Status::OK();
}

// the cache of hdfs filesystem
static std::map<std::string, std::shared_ptr<HadoopFileSystem>> fsMap_;
static std::mutex mutex_;

std::shared_ptr<HadoopFileSystem> getHdfsFileSystem(const std::string &host, const std::string &port) {
    std::shared_ptr<HadoopFileSystem> fileSystemPtr;

    mutex_.lock();
    std::string key = host + ":" + port;
    auto iter = fsMap_.find(key);
    if (iter != fsMap_.end()) {
        fileSystemPtr = fsMap_[key];
        mutex_.unlock();
        return fileSystemPtr;
    }

    HdfsOptions options;
    options.ConfigureHost(host);
    int portInt = 0;
    if (!port.empty()) {
        portInt = std::stoi(port);
    }
    if (portInt > 0) {
        options.ConfigurePort(portInt);
    }

    std::shared_ptr<HadoopFileSystem> fs(new HadoopFileSystem(options));
    fileSystemPtr = fs;
    fsMap_[key] = fs;
    mutex_.unlock();

    return fileSystemPtr;
}

}