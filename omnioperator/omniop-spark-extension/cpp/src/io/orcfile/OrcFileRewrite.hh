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

#ifndef ORC_FILE_REWRITE_HH
#define ORC_FILE_REWRITE_HH

#include <string>

#include "hdfspp/options.h"
#include "orc/OrcFile.hh"

/** /file orc/OrcFile.hh
    @brief The top level interface to ORC.
*/

namespace orc {

  /**
   * Create a stream to a local file or HDFS file if path begins with "hdfs://"
   * @param path the name of the file in the local file system or HDFS
   */
  ORC_UNIQUE_PTR<InputStream> readFileRewrite(const std::string& path, std::vector<hdfs::Token*>& tokens);

  /**
   * Create a stream to an HDFS file.
   * @param path the uri of the file in HDFS
   */
  ORC_UNIQUE_PTR<InputStream> readHdfsFileRewrite(const std::string& path, std::vector<hdfs::Token*>& tokens);
}

#endif
