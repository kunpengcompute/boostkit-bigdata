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

#include <gtest/gtest.h>
#include "filesystem/hdfs_filesystem.h"
#include "filesystem/hdfs_file.h"

namespace omniruntime::fs {

// Test HdfsOptions
class HdfsOptionsTest : public ::testing::Test {
protected:
    HdfsOptions options;
};

// Test HdfsOptions::ConfigureHost
TEST_F(HdfsOptionsTest, ConfigureHost) {
    options.ConfigureHost("server1");
    ASSERT_EQ(options.host_, "server1");
}

// Test HdfsOptions::ConfigurePort
TEST_F(HdfsOptionsTest, ConfigurePort) {
    options.ConfigurePort(9000);
    ASSERT_EQ(options.port_, 9000);
}

// Test HdfsOptions::Equals
TEST_F(HdfsOptionsTest, Equals) {
    HdfsOptions options;
    options.ConfigureHost("server1");
    options.ConfigurePort(9000);

    HdfsOptions otherOptions;
    otherOptions.ConfigureHost("server1");
    otherOptions.ConfigurePort(9000);
    ASSERT_TRUE(options.Equals(otherOptions));
}

}