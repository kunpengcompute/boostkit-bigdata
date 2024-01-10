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

#ifndef SPARK_THESTRAL_PLUGIN_HDFS_FILESYSTEM_H
#define SPARK_THESTRAL_PLUGIN_HDFS_FILESYSTEM_H

#include "filesystem.h"
#include "hdfs.h"
#include "status.h"

namespace fs {

struct HdfsOptions {
    HdfsOptions() = default;

    ~HdfsOptions() = default;

    std::string host_;
    int port_ = 0;

    void ConfigureHost(const std::string &host);

    void ConfigurePort(int port);

    bool Equals(const HdfsOptions &other) const;
};

class HadoopFileSystem : public FileSystem {
private:
    // Hadoop file system handle
    hdfsFS fs_;
    // Options for Hadoop file system
    HdfsOptions options_;

public:
    // Constructor with Hadoop options
    HadoopFileSystem(HdfsOptions &options);

    // Destructor
    ~HadoopFileSystem();

    // Get the type name of the file system
    std::string type_name() const override { return "HdfsFileSystem"; }

    /**
     * Check if this file system is equal to another file system
     * @param other the other filesystem
     */
    bool Equals(const FileSystem &other) const override;

    /**
     * Get file info from file system
     * @param path the file path
     */
    FileInfo GetFileInfo(const std::string &path) override;

    // Close the file system
    Status Close();

    // Get the Hadoop file system handle
    hdfsFS getFileSystem();

    // Get the Hadoop file system options
    HdfsOptions getOptions() const;

private:
    // Initialize the Hadoop file system
    Status Init();
};

/**
* Get a shared pointer to a Hadoop file system
* @param host the host of hdfs filesystem
* @param port the port of hdfs filesystem
*/
std::shared_ptr<HadoopFileSystem> getHdfsFileSystem(const std::string &host, const std::string &port);

}

#endif //SPARK_THESTRAL_PLUGIN_HDFS_FILESYSTEM_H
