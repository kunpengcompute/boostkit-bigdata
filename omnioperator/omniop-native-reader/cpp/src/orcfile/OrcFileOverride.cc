/**
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

#include "OrcFileOverride.hh"
#include "sys/stat.h"

#define O_BINARY 0

namespace orc
{
    int createDirectories(const std::string &path) {
        size_t pos = path.find_first_of("/");
        while (pos != std::string::npos) {
            std::string dir = path.substr(0, pos);
            if (dir.length() > 0) {
                struct stat st = {0};
                if (stat(dir.c_str(), &st) == -1) {
                    if (mkdir(dir.c_str(), 0755) == -1 && errno != EEXIST) {
                        return -1;
                    }
                }
            }
            pos = path.find_first_of("/", pos + 1);
        }
        struct stat st = {0};
        if (stat(path.c_str(), &st) == -1) {
            if (mkdir(path.c_str(), 0755) == -1 && errno != EEXIST) {
                return -1;
            }
        }
        return 0;
    }

    std::string getParentPath(const std::string &path) {
        size_t found = path.find_last_of("/");
        if (found != std::string::npos) {
            return path.substr(0, found);
        }
        return "";
    }

    std::unique_ptr<InputStream> readFileOverride(const UriInfo &uri) {
        if (uri.Scheme() == "hdfs") {
            return orc::createHdfsFileInputStream(uri);
        }
        else {
            return orc::readLocalFile(std::string(uri.Path()));
        }
    }

    std::unique_ptr<OutputStream> writeFileOverride(const UriInfo &uri)
    {
        if (uri.Scheme() == "hdfs") {
            return orc::createHdfsFileOutputStream(uri);
        }
        else {
            auto res = createDirectories(getParentPath(uri.Path()));
            if (res != 0) {
                throw std::runtime_error("Create local directories fail");
            }
            return orc::writeLocalFile(std::string(uri.Path()));
        }
    }
} // namespace orc
