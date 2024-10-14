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

#include "hdfs_file.h"
#include "iostream"

namespace fs {

HdfsReadableFile::HdfsReadableFile(std::shared_ptr<HadoopFileSystem> fileSystemPtr,
                                   const std::string &path, int64_t bufferSize)
        : fileSystem_(fileSystemPtr), path_(path), bufferSize_(bufferSize) {
}

HdfsReadableFile::~HdfsReadableFile() {
    this->TryClose();
}

Status HdfsReadableFile::Close() {
    return TryClose();
}

Status HdfsReadableFile::TryClose() {
    if (!isOpen_) {
        return Status::OK();
    }
    int st = hdfsCloseFile(fileSystem_->getFileSystem(), file_);
    if (st == -1) {
        return Status::IOError("Fail to close hdfs file, path is " + path_);
    }
    this->isOpen_ = false;
    return Status::OK();
}

Status HdfsReadableFile::OpenFile() {
    if (isOpen_) {
        return Status::OK();
    }
    hdfsFile handle = hdfsOpenFile(fileSystem_->getFileSystem(), path_.c_str(), O_RDONLY, bufferSize_, 0, 0);
    if (handle == nullptr) {
        return Status::IOError("Fail to open hdfs file, path is " + path_);
    }

    this->file_ = handle;
    this->isOpen_ = true;
    return Status::OK();
}

int64_t HdfsReadableFile::ReadAt(void *buffer, int32_t length, int64_t offset) {
    if (!OpenFile().IsOk()) {
        return -1;
    }

    return hdfsPread(fileSystem_->getFileSystem(), file_, offset, buffer, length);
}

int64_t HdfsReadableFile::GetFileSize() {
    if (!OpenFile().IsOk()) {
        return -1;
    }

    FileInfo fileInfo = fileSystem_->GetFileInfo(path_);
    return fileInfo.size();
}

Status HdfsReadableFile::Seek(int64_t position) {
    if (!OpenFile().IsOk()) {
        return Status::IOError("Fail to open and seek hdfs file, path is " + path_);
    }
    int st = hdfsSeek(fileSystem_->getFileSystem(), file_, position);
    if (st == -1) {
        return Status::IOError("Fail to seek hdfs file, path is " + path_);
    }
    return Status::OK();
}

int64_t HdfsReadableFile::Read(void *buffer, int32_t length) {
    if (!OpenFile().IsOk()) {
        return -1;
    }

    return hdfsRead(fileSystem_->getFileSystem(), file_, buffer, length);
}

HdfsWriteableFile::HdfsWriteableFile(std::shared_ptr<HadoopFileSystem> fileSystemPtr, const std::string &path,
                                     int64_t bufferSize) :
    fileSystem_(std::move(fileSystemPtr)),
    path_(path), bufferSize_(bufferSize)
{
}

HdfsWriteableFile::~HdfsWriteableFile() { this->TryClose(); }

Status HdfsWriteableFile::Close() { return TryClose(); }

Status HdfsWriteableFile::OpenFile() {
    if (isOpen_) {
        return Status::OK();
    }
    hdfsFile handle = hdfsOpenFile(fileSystem_->getFileSystem(), path_.c_str(), O_WRONLY, bufferSize_, 0, 0);
    if (handle == nullptr) {
        return Status::IOError("Fail to open hdfs file, path is " + path_);
    }

    this->file_ = handle;
    this->isOpen_ = true;
    return Status::OK();
}

int64_t HdfsWriteableFile::Write(const void *buffer, int32_t length) {
    if (!OpenFile().IsOk()) {
        return -1;
    }
    hdfsWrite(fileSystem_->getFileSystem(), file_, buffer, length);
    return hdfsHFlush(fileSystem_->getFileSystem(), file_);
}

Status HdfsWriteableFile::TryClose() {
    if (!isOpen_) {
        return Status::OK();
    }
    int st = hdfsCloseFile(fileSystem_->getFileSystem(), file_);
    if (st == -1) {
        return Status::IOError("Fail to close hdfs file, path is " + path_);
    }
    this->isOpen_ = false;
    return Status::OK();
}

int64_t HdfsWriteableFile::GetFileSize() {
    if (!OpenFile().IsOk()) {
        return -1;
    }
    FileInfo fileInfo = fileSystem_->GetFileInfo(path_);
    return fileInfo.size();
}

} // namespace fs