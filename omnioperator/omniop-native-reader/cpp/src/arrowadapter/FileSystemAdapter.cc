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

#include <sstream>
#include <utility>

#include "FileSystemAdapter.h"
#include "arrow/filesystem/hdfs.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/io/slow.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/macros.h"
#include "arrow/util/parallel.h"
#include "HdfsAdapter.h"
#include "LocalfsAdapter.h"
#include "UtilInternal.h"

namespace arrow_adapter {

using arrow::internal::Uri;
using arrow::fs::internal::RemoveLeadingSlash;
using arrow::fs::internal::ToSlashes;
using arrow::fs::FileSystem;
using arrow::fs::HadoopFileSystem;
using arrow::fs::LocalFileSystem;
using arrow::fs::internal::MockFileSystem;
using arrow::Result;

namespace {

Result<std::shared_ptr<FileSystem>>
FileSystemFromUriReal(const UriInfo &uri, const arrow::io::IOContext &io_context, std::string *out_path)
{
    const auto scheme = uri.Scheme();

    if (scheme == "file") {
        std::string path;
        ARROW_ASSIGN_OR_RAISE(auto options, buildLocalfsOptionsFromUri(uri, &path));
        if (out_path != nullptr) {
            *out_path = path;
        }
        return std::make_shared<LocalFileSystem>(options, io_context);
    }

    if (scheme == "hdfs" || scheme == "viewfs") {
        ARROW_ASSIGN_OR_RAISE(auto options, buildHdfsOptionsFromUri(uri));
        if (out_path != nullptr) {
            *out_path = uri.Path();
        }
        ARROW_ASSIGN_OR_RAISE(auto hdfs, HadoopFileSystem::Make(options, io_context));
        return hdfs;
    }

    if (scheme == "mock") {
        // MockFileSystem does not have an absolute / relative path distinction,
        // normalize path by removing leading slash.
        if (out_path != nullptr) {
            *out_path = std::string(RemoveLeadingSlash(uri.Path()));
        }
        return std::make_shared<MockFileSystem>(CurrentTimePoint(),
                                                io_context);
    }

    return arrow::fs::FileSystemFromUri(uri.ToString(), io_context, out_path);
}

}  // namespace


Result<std::shared_ptr<FileSystem>> FileSystemFromUriOrPath(const UriInfo &uri,
                                                            std::string *out_path)
{
    return FileSystemFromUriOrPath(uri, arrow::io::IOContext(), out_path);
}

Result<std::shared_ptr<FileSystem>> FileSystemFromUriOrPath(
        const UriInfo &uri, const arrow::io::IOContext &io_context,
        std::string *out_path)
{
    const auto &uri_string = uri.ToString();
    if (arrow::fs::internal::DetectAbsolutePath(uri_string)) {
        // Normalize path separators
        if (out_path != nullptr) {
            *out_path = ToSlashes(uri_string);
        }
        return std::make_shared<LocalFileSystem>();
    }
    return FileSystemFromUriReal(uri, io_context, out_path);
}

}
// namespace arrow