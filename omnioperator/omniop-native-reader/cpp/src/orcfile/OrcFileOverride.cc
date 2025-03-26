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
#include "common/Directories.h"
#include "sys/stat.h"

#define O_BINARY 0

namespace orc
{
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
            auto res = common::createDirectories(common::getParentPath(uri.Path()));
            if (res != 0) {
                throw std::runtime_error("Create local directories fail");
            }
            return orc::writeLocalFile(std::string(uri.Path()));
        }
    }
}
