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

#include <unordered_map>
#include <common/UriInfo.h>

#include "arrow/filesystem/hdfs.h"
#include "arrow/util/value_parsing.h"
#include "HdfsAdapter.h"

namespace arrow_adapter {

    using arrow::internal::ParseValue;

    using arrow::Result;
    using arrow::fs::HdfsOptions;

    Result<HdfsOptions> buildHdfsOptionsFromUri(const UriInfo &uri) {
        HdfsOptions options;

        std::string host;
        host = uri.Scheme() + "://" + uri.Host();

        // configure endpoint
        int32_t port;
        if (uri.Port().empty() || (port = atoi(uri.Port().c_str())) == -1) {
            // default Port will be determined by hdfs FileSystem impl
            options.ConfigureEndPoint(host, 0);
        } else {
            options.ConfigureEndPoint(host, port);
        }

        return options;
    }

}
// namespace arrow