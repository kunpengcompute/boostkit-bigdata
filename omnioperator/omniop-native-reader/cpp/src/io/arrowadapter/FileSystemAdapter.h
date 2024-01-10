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

#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/filesystem/type_fwd.h"
#include "arrow/io/interfaces.h"
#include "arrow/type_fwd.h"
#include "arrow/util/compare.h"
#include "arrow/util/macros.h"
#include "arrow/util/type_fwd.h"
#include "arrow/util/visibility.h"
#include "arrow/util/windows_fixup.h"
#include "io/UriInfo.h"

namespace arrow_adapter {

    using arrow::Result;

    using arrow::fs::FileSystem;

/// \defgroup filesystem-factories Functions for creating FileSystem instances

/// @{

/// \brief Create a new FileSystem by URI
///
/// Same as FileSystemFromUriOrPath, but it use uri that constructed by client
    ARROW_EXPORT
    Result<std::shared_ptr<FileSystem>>

    FileSystemFromUriOrPath(const UriInfo &uri,
                            std::string *out_path = NULLPTR);


/// \brief Create a new FileSystem by URI with a custom IO context
///
/// Recognized schemes are "file", "mock", "hdfs", "viewfs", "s3",
/// "gs" and "gcs".
///
/// \param[in] uri a URI-based path, ex: file:///some/local/Path
/// \param[in] io_context an IOContext which will be associated with the filesystem
/// \param[out] out_path (optional) Path inside the filesystem.
/// \return out_fs FileSystem instance.


/// \brief Create a new FileSystem by URI with a custom IO context
///
/// Same as FileSystemFromUri, but in addition also recognize non-URIs
/// and treat them as local filesystem paths.  Only absolute local filesystem
/// paths are allowed.
    ARROW_EXPORT
    Result<std::shared_ptr<FileSystem>> FileSystemFromUriOrPath(
            const UriInfo &uri, const arrow::io::IOContext &io_context,
            std::string *out_path = NULLPTR);

/// @}

// namespace fs
}
// namespace arrow_adapter