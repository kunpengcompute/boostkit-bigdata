/**
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

#ifndef NATIVE_READER_TEST_UTILS_H
#define NATIVE_READER_TEST_UTILS_H

#include <vector>
#include <stdlib.h>
#include <stdio.h>
#include <dirent.h>
#include "arrow/filesystem/filesystem.h"
#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/filesystem/type_fwd.h"

using arrow::fs::FileSystem;
using arrow::fs::FileInfo;
using arrow::fs::FileType;

void CreateFile(FileSystem *fs, const std::string &path, const std::string &data) {
    ASSERT_OK_AND_ASSIGN(auto stream, fs->OpenOutputStream(path));
    ASSERT_OK(stream->Write(data));
    ASSERT_OK(stream->Close());
}

void AssertFileInfo(const FileInfo &info, const std::string &path, FileType type) {
    ASSERT_EQ(info.path(), path);
    ASSERT_EQ(info.type(), type) << "For path '" << info.path() << "'";
}

void AssertFileInfo(FileSystem *fs, const std::string &path, FileType type) {
    ASSERT_OK_AND_ASSIGN(FileInfo info, fs->GetFileInfo(path));
    AssertFileInfo(info, path, type);
}

#endif //NATIVE_READER_TEST_UTILS_H
