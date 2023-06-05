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

#include "OrcFileRewrite.hh"
#include "orc/Exceptions.hh"
#include "io/Adaptor.hh"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <string.h>

#ifdef _MSC_VER
#include <io.h>
#define S_IRUSR _S_IREAD
#define S_IWUSR _S_IWRITE
#define stat _stat64
#define fstat _fstat64
#else
#include <unistd.h>
#define O_BINARY 0
#endif

namespace orc {
  std::unique_ptr<InputStream> readFileRewrite(const std::string& path, std::vector<hdfs::Token*>& tokens) {
    if (strncmp(path.c_str(), "hdfs://", 7) == 0) {
      return orc::readHdfsFileRewrite(std::string(path), tokens);
    } else if (strncmp(path.c_str(), "file:", 5) == 0) {
      return orc::readLocalFile(std::string(path.substr(5)));
    } else {
      return orc::readLocalFile(std::string(path));
    }
  }
}
