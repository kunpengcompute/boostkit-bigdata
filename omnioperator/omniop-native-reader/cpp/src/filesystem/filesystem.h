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

#ifndef SPARK_THESTRAL_PLUGIN_FILESYSTEM_H
#define SPARK_THESTRAL_PLUGIN_FILESYSTEM_H

#include <string>
#include <memory>
#include <chrono>
#include "status.h"

namespace fs {

using TimePoint =
        std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>;

static const int64_t kNoSize = -1;
static const TimePoint kNoTime = TimePoint(TimePoint::duration(-1));

enum class FileType : int8_t {
    /// Entry is not found
    NotFound,
    /// Entry exists but its type is unknown
    ///
    /// This can designate a special file such as a Unix socket or character
    /// device, or Windows NUL / CON / ...
    Unknown,
    /// Entry is a regular file
    File,
    /// Entry is a directory
    Directory
};

std::string ToString(FileType);

struct FileInfo {
    /// The full file path in the filesystem
    const std::string &path() const { return path_; }

    void setPath(std::string path) { path_ = std::move(path); }

    /// The file type
    FileType type() const { return type_; }

    void setType(FileType type) { type_ = type; }

    /// The size in bytes, if available
    int64_t size() const { return size_; }

    void setSize(int64_t size) { size_ = size; }

    /// The time of last modification, if available
    TimePoint mtime() const { return mtime_; }

    void setMtime(TimePoint mtime) { mtime_ = mtime; }

    bool IsFile() const { return type_ == FileType::File; }

    bool IsDirectory() const { return type_ == FileType::Directory; }

    bool Equals(const FileInfo &other) const {
        return type() == other.type() && path() == other.path() && size() == other.size() &&
               mtime() == other.mtime();
    }

protected:
    std::string path_;
    FileType type_ = FileType::Unknown;
    int64_t size_ = kNoSize;
    TimePoint mtime_ = kNoTime;

};

}

namespace fs {

class FileSystem {
public:
    // Virtual destructor
    virtual ~FileSystem() = default;

    // Get the type name of the file system
    virtual std::string type_name() const = 0;

    /**
     * Get information about the file at the specified path
     * @param path the file path
     */
    virtual FileInfo GetFileInfo(const std::string &path) = 0;

    /**
     * Check if this file system is equal to another file system
     * @param other the other filesystem
     */
    virtual bool Equals(const FileSystem &other) const = 0;

    /**
     * Check if this file system is equal to a shared pointer to another file system
     * @param other the other filesystem pointer
     */
    virtual bool Equals(const std::shared_ptr<FileSystem> &other) const {
        return Equals(*other);
    }

    // Close the file system
    virtual Status Close() = 0;
};

} // fs




#endif //SPARK_THESTRAL_PLUGIN_FILESYSTEM_H
