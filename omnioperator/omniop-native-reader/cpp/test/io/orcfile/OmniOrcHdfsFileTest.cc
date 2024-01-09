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

#include "gtest/gtest.h"
#include "io/orcfile/OmniOrcFile.hh"
#include "orcfile_test.h"

TEST(OrcReader, createLocalFileReader) {
    std::string filename = "/resources/orc_data_all_type";
    filename =  PROJECT_PATH + filename;

    std::unique_ptr<orc::Reader> reader;
    std::unique_ptr<orc::RowReader> rowReader;
    std::unique_ptr<orc::ColumnVectorBatch> batch;
    orc::ReaderOptions readerOpts;
    orc::RowReaderOptions rowReaderOpts;
    std::list<uint64_t> cols;

    cols.push_back(1);
    rowReaderOpts.include(cols);
    UriInfo uriInfo("file", filename, "", "");
    reader = orc::createReader(orc::readOmniFile(uriInfo), readerOpts);
    EXPECT_NE(nullptr, reader);
}
