/**
 * Copyright (C) 2023-2023. Huawei Technologies Co., Ltd. All rights reserved.
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
#include <orc/sargs/SearchArgument.hh>
#include "jni/OrcColumnarBatchJniReader.h"
#include "scan_test.h"
#include <vector/vector_common.h>

class ScanArrayTest : public testing::Test {
protected:
    // run before each case...
    virtual void SetUp() override
    {
        orc::ReaderOptions readerOpts;
        orc::RowReaderOptions rowReaderOptions;
        std::string filename = "/../resources/orc_nested_type";
        filename = PROJECT_PATH + filename;
        UriInfo uriInfo("file", filename, "", "-1");
        std::unique_ptr<orc::Reader> reader = omniruntime::reader::omniCreateReader(orc::readFileOverride(uriInfo, false), readerOpts);

        std::list<std::string> includedColumns = {"c1", "c2", "c3", "c4", "c5", "c17"};
        rowReaderOptions.include(includedColumns);

        std::unique_ptr<common::JulianGregorianRebase> julianPtr;
        std::unique_ptr<common::PredicateCondition> predicatePtr;
        auto readerPtr = static_cast<omniruntime::reader::OmniReaderImpl*>(reader.get());
        rowReader = readerPtr->createRowReader(rowReaderOptions, julianPtr, predicatePtr).release();
        omniruntime::reader::OmniRowReaderImpl *rowReaderPtr = (omniruntime::reader::OmniRowReaderImpl*) rowReader;
        rowReaderPtr->next(&recordBatch, nullptr, 4096);
    }

    // run after each case...
    virtual void TearDown() override {
        for (auto vec : recordBatch) {
            delete vec;
        }
        recordBatch.clear();
        delete rowReader;
        rowReader = nullptr;
    }

    orc::RowReader *rowReader;
    std::vector<omniruntime::vec::BaseVector*> recordBatch;
};

TEST_F(ScanArrayTest, array_test_correctness_stringVec)
{
    // string type, "c3"
    auto *olbStr = (omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>> *)(
            recordBatch[2]);
    std::string_view actualStr = olbStr->GetValue(0);
    ASSERT_EQ(actualStr, "string_type_1");
}