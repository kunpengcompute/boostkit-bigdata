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


#include <arrow/filesystem/hdfs.h>
#include "gtest/gtest.h"
#include "arrowadapter/FileSystemAdapter.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/util/checked_cast.h"
#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/localfs.h"
#include "../../utils/test_utils.h"
#include "arrow/util/uri.h"

using namespace arrow::fs::internal;
using arrow::fs::TimePoint;
using arrow::fs::FileSystem;
using arrow_adapter::FileSystemFromUriOrPath;
using arrow::internal::TemporaryDir;
using arrow::fs::LocalFileSystem;
using arrow::fs::LocalFileSystemOptions;
using arrow::internal::PlatformFilename;
using arrow::internal::FileDescriptor;
using arrow::Result;
using arrow::fs::HadoopFileSystem;
using arrow::fs::HdfsOptions;

class TestMockFS : public ::testing::Test {
public:
    void SetUp() override {
        time_ = TimePoint(TimePoint::duration(42));
        fs_ = std::make_shared<MockFileSystem>(time_);
    }

    std::vector<MockDirInfo> AllDirs() {
        return arrow::internal::checked_pointer_cast<MockFileSystem>(fs_)->AllDirs();
    }

    void CheckDirs(const std::vector<MockDirInfo>& expected) {
        ASSERT_EQ(AllDirs(), expected);
    }

protected:
    TimePoint time_;
    std::shared_ptr<FileSystem> fs_;
};

TEST_F(TestMockFS, FileSystemFromUriOrPath) {
    std::string path;
    UriInfo uri1("mock", "", "", "-1");
    ASSERT_OK_AND_ASSIGN(fs_, FileSystemFromUriOrPath(uri1, &path));
    ASSERT_EQ(path, "");
    CheckDirs({});  // Ensures it's a MockFileSystem

    UriInfo uri2("mock", "foo/bar", "", "-1");
    ASSERT_OK_AND_ASSIGN(fs_, FileSystemFromUriOrPath(uri2, &path));
    ASSERT_EQ(path, "foo/bar");
    CheckDirs({});

    UriInfo ur3("mock", "/foo/bar", "", "-1");
    ASSERT_OK_AND_ASSIGN(fs_, FileSystemFromUriOrPath(ur3, &path));
    ASSERT_EQ(path, "foo/bar");
    CheckDirs({});
}

struct CommonPathFormatter {
    std::string operator()(std::string fn) { return fn; }
    bool supports_uri() { return true; }
};

using PathFormatters = ::testing::Types<CommonPathFormatter>;

// Non-overloaded version of FileSystemFromUri, for template resolution
Result<std::shared_ptr<FileSystem>> FSFromUriOrPath(const UriInfo& uri,
                                                    std::string* out_path = NULLPTR) {
    return arrow_adapter::FileSystemFromUriOrPath(uri, out_path);
}


template <typename PathFormatter>
class TestLocalFs : public ::testing::Test {
public:
    void SetUp() override {
        ASSERT_OK_AND_ASSIGN(temp_dir_, TemporaryDir::Make("test-localfs-"));
        local_path_ = EnsureTrailingSlash(path_formatter_(temp_dir_->path().ToString()));
        MakeFileSystem();
    }

    void MakeFileSystem() {
        local_fs_ = std::make_shared<LocalFileSystem>(options_);
    }

    template <typename FileSystemFromUriFunc>
    void CheckFileSystemFromUriFunc(const UriInfo& uri,
                                    FileSystemFromUriFunc&& fs_from_uri) {
        if (!path_formatter_.supports_uri()) {
            return;  // skip
        }
        std::string path;
        ASSERT_OK_AND_ASSIGN(fs_, fs_from_uri(uri, &path));
        ASSERT_EQ(path, local_path_);

        // Test that the right location on disk is accessed
        CreateFile(fs_.get(), local_path_ + "abc", "some data");
        CheckConcreteFile(this->temp_dir_->path().ToString() + "abc", 9);
    }

    void TestFileSystemFromUri(const UriInfo& uri) {
        CheckFileSystemFromUriFunc(uri, FSFromUriOrPath);
    }

    void CheckConcreteFile(const std::string& path, int64_t expected_size) {
        ASSERT_OK_AND_ASSIGN(auto fn, PlatformFilename::FromString(path));
        ASSERT_OK_AND_ASSIGN(FileDescriptor fd, ::arrow::internal::FileOpenReadable(fn));
        auto result = ::arrow::internal::FileGetSize(fd.fd());
        ASSERT_OK_AND_ASSIGN(int64_t size, result);
        ASSERT_EQ(size, expected_size);
    }

    void TestLocalUri(const UriInfo& uri, const std::string& expected_path) {
        CheckLocalUri(uri, expected_path, FSFromUriOrPath);
    }

    template <typename FileSystemFromUriFunc>
    void CheckLocalUri(const UriInfo& uri, const std::string& expected_path,
                       FileSystemFromUriFunc&& fs_from_uri) {
        if (!path_formatter_.supports_uri()) {
            return;  // skip
        }
        std::string path;
        ASSERT_OK_AND_ASSIGN(fs_, fs_from_uri(uri, &path));
        ASSERT_EQ(fs_->type_name(), "local");
        ASSERT_EQ(path, expected_path);
    }

    void TestInvalidUri(const UriInfo& uri) {
        if (!path_formatter_.supports_uri()) {
            return;  // skip
        }
        ASSERT_RAISES(Invalid, FSFromUriOrPath(uri));
    }

protected:
    std::unique_ptr<TemporaryDir> temp_dir_;
    std::shared_ptr<FileSystem> fs_;
    std::string local_path_;
    PathFormatter path_formatter_;
    std::shared_ptr<LocalFileSystem> local_fs_;
    LocalFileSystemOptions options_ = LocalFileSystemOptions::Defaults();
};

TYPED_TEST_SUITE(TestLocalFs, PathFormatters);

TYPED_TEST(TestLocalFs, FileSystemFromUriFile){
    std::string path;
    ASSERT_OK_AND_ASSIGN(auto uri_string, arrow::internal::UriFromAbsolutePath(this->local_path_));
    UriInfo uri1(uri_string, "", uri_string, "", "-1");
    this->TestFileSystemFromUri(uri1);

    path = "/foo/bar";
    UriInfo uri2("file", path, "", "-1");
    this->TestLocalUri(uri2, path);

    path = "/some path/%percent";
    UriInfo uri3("file", path, "", "-1");
    this->TestLocalUri(uri3, path);

    path = "/some path/%中文魑魅魍魉";
    UriInfo uri4("file", path, "", "-1");
    this->TestLocalUri(uri4, path);

    path = "/foo/bar";
    UriInfo uri5("file:" + path, "file" , path, "", "-1");
    this->TestLocalUri(uri5, path);
}

TYPED_TEST(TestLocalFs, FileSystemFromUriNoScheme){

    UriInfo uri1(this->local_path_, "", "", "", "-1");
    this->TestFileSystemFromUri(uri1);

    UriInfo uri2("foo/bar", "", "", "", "-1");
    this->TestInvalidUri(uri2);
}
