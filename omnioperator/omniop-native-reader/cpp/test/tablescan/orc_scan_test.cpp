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

/* 
 * CREATE TABLE `orc_test` ( `c1` int, `c2` varChar(60), `c3` string, `c4` bigint,
 * `c5` char(40), `c6` float, `c7` double, `c8` decimal(9,8), `c9` decimal(18,5),
 * `c10` boolean, `c11` smallint, `c12` timestamp, `c13` date)stored as orc;
 * 
 * insert into  `orc_test` values (10, "varchar_1", "string_type_1", 10000, "char_1",
 * 11.11, 1111.1111, 121.1111, 131.1111, true, 11, '2021-12-01 01:00:11', '2021-12-01');
 * insert into  `orc_test` values (20, "varchar_2", NULL, 20000, "char_2",
 * 11.22, 1111.2222, 121.2222, 131.2222, true, 12, '2021-12-01 01:22:11', '2021-12-02');
 * insert into  `orc_test` values (30, "varchar_3", "string_type_3", NULL, "char_2",
 * 11.33, 1111.333, 121.3333, 131.2222, NULL, 13, '2021-12-01 01:33:11', '2021-12-03');
 * insert into  `orc_test` values (40, "varchar_4", "string_type_4", 40000, NULL,
 * 11.44, NULL, 121.2222, 131.44, false, 14, '2021-12-01 01:44:11', '2021-12-04');
 * insert into  `orc_test` values (50, "varchar_5", "string_type_5", 50000, "char_5",
 * 11.55, 1111.55, 121.55, 131.55, true, 15, '2021-12-01 01:55:11', '2021-12-05');
 * 
 */
class ScanTest : public testing::Test {
protected:
    // run before each case...
    virtual void SetUp() override
    {
        orc::ReaderOptions readerOpts;
        orc::RowReaderOptions rowReaderOptions;
        std::string filename = "/../resources/orc_data_all_type";
        filename = PROJECT_PATH + filename;
        UriInfo uriInfo("file", filename, "", "-1");
        std::unique_ptr<orc::Reader> reader = omniruntime::reader::omniCreateReader(orc::readFileOverride(uriInfo, false), readerOpts);

        std::list<std::string> includedColumns = {"c1", "c2", "c3", "c4", "c5", "c7", "c8", "c9", "c10", "c11", "c13"};
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

TEST_F(ScanTest, test_literal_get_long)
{
    orc::Literal tmpLit(0L);

    // test get long
    GetLiteral(tmpLit, (int)(orc::PredicateDataType::LONG), "655361");
    ASSERT_EQ(tmpLit.getLong(), 655361);
    GetLiteral(tmpLit, (int)(orc::PredicateDataType::LONG), "-655361");
    ASSERT_EQ(tmpLit.getLong(), -655361);
    GetLiteral(tmpLit, (int)(orc::PredicateDataType::LONG), "0");
    ASSERT_EQ(tmpLit.getLong(), 0);
}

TEST_F(ScanTest, test_literal_get_float)
{
    orc::Literal tmpLit(0L);

    // test get float
    GetLiteral(tmpLit, (int)(orc::PredicateDataType::FLOAT), "12345.6789");
    ASSERT_EQ(tmpLit.getFloat(), 12345.6789);
    GetLiteral(tmpLit, (int)(orc::PredicateDataType::FLOAT), "-12345.6789");
    ASSERT_EQ(tmpLit.getFloat(), -12345.6789);
    GetLiteral(tmpLit, (int)(orc::PredicateDataType::FLOAT), "0");
    ASSERT_EQ(tmpLit.getFloat(), 0);
}

TEST_F(ScanTest, test_literal_get_string)
{
    orc::Literal tmpLit(0L);

    // test get string
    GetLiteral(tmpLit, (int)(orc::PredicateDataType::STRING), "testStringForLit");
    ASSERT_EQ(tmpLit.getString(), "testStringForLit");
    GetLiteral(tmpLit, (int)(orc::PredicateDataType::STRING), "");
    ASSERT_EQ(tmpLit.getString(), "");
}

TEST_F(ScanTest, test_literal_get_date)
{
    orc::Literal tmpLit(0L);

    // test get date
    GetLiteral(tmpLit, (int)(orc::PredicateDataType::DATE), "987654321");
    ASSERT_EQ(tmpLit.getDate(), 987654321);
}

TEST_F(ScanTest, test_literal_get_decimal)
{
    orc::Literal tmpLit(0L);

    // test get decimal
    GetLiteral(tmpLit, (int)(orc::PredicateDataType::DECIMAL), "199999999999998.998000 22 6");
    ASSERT_EQ(tmpLit.getDecimal().toString(), "199999999999998.998000");
    GetLiteral(tmpLit, (int)(orc::PredicateDataType::DECIMAL), "10.998000 10 6");
    ASSERT_EQ(tmpLit.getDecimal().toString(), "10.998000");
    GetLiteral(tmpLit, (int)(orc::PredicateDataType::DECIMAL), "-10.998000 10 6");
    ASSERT_EQ(tmpLit.getDecimal().toString(), "-10.998000");
    GetLiteral(tmpLit, (int)(orc::PredicateDataType::DECIMAL), "9999.999999 10 6");
    ASSERT_EQ(tmpLit.getDecimal().toString(), "9999.999999");
    GetLiteral(tmpLit, (int)(orc::PredicateDataType::DECIMAL), "-0.000000 10 6");
    ASSERT_EQ(tmpLit.getDecimal().toString(), "0.000000");
}

TEST_F(ScanTest, test_literal_get_bool)
{
    orc::Literal tmpLit(0L);

    // test get bool
    GetLiteral(tmpLit, (int)(orc::PredicateDataType::BOOLEAN), "true");
    ASSERT_EQ(tmpLit.getBool(), true);
    GetLiteral(tmpLit, (int)(orc::PredicateDataType::BOOLEAN), "True");
    ASSERT_EQ(tmpLit.getBool(), true);
    GetLiteral(tmpLit, (int)(orc::PredicateDataType::BOOLEAN), "false");
    ASSERT_EQ(tmpLit.getBool(), false);
    GetLiteral(tmpLit, (int)(orc::PredicateDataType::BOOLEAN), "False");
    ASSERT_EQ(tmpLit.getBool(), false);
    std::string tmpStr = "";
    try {
        GetLiteral(tmpLit, (int)(orc::PredicateDataType::BOOLEAN), "exception");
    } catch (std::exception &e) {
        tmpStr = e.what();
    }
    ASSERT_EQ(tmpStr, "Invalid input for stringToBool.");
}

TEST_F(ScanTest, test_correctness_intVec)
{
    // int type, "c1"
    auto *olbInt = (omniruntime::vec::Vector<int32_t> *)(recordBatch[0]);
    ASSERT_EQ(olbInt->GetValue(0), 10);
}

TEST_F(ScanTest, test_correctness_varCharVec)
{
    // varchar type, "c2"
    auto *olbVc = (omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>> *)(
            recordBatch[1]);
    std::string_view actualStr = olbVc->GetValue(0);
    ASSERT_EQ(actualStr, "varchar_1");
}

TEST_F(ScanTest, test_correctness_stringVec)
{
    // string type, "c3"
    auto *olbStr = (omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>> *)(
            recordBatch[2]);
    std::string_view actualStr = olbStr->GetValue(0);
    ASSERT_EQ(actualStr, "string_type_1");
}

TEST_F(ScanTest, test_correctness_longVec)
{
    // bigint type, "c4"
    auto *olbLong = (omniruntime::vec::Vector<int64_t> *)(recordBatch[3]);
    ASSERT_EQ(olbLong->GetValue(0), 10000);
}

TEST_F(ScanTest, test_correctness_charVec)
{
    // char type, "c5"
    auto *olbChar = (omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>> *)(
            recordBatch[4]);
    std::string_view actualStr = olbChar->GetValue(0);
    ASSERT_EQ(actualStr, "char_1");
}

TEST_F(ScanTest, test_correctness_doubleVec)
{
    // double type, "c7"
    auto *olbDouble = (omniruntime::vec::Vector<double> *)(recordBatch[5]);
    ASSERT_EQ(olbDouble->GetValue(0), 1111.1111);
}

TEST_F(ScanTest, test_correctness_booleanVec)
{
    // boolean type, "c10"
    auto *olbBoolean = (omniruntime::vec::Vector<bool> *)(recordBatch[8]);
    ASSERT_EQ(olbBoolean->GetValue(0), true);
}

TEST_F(ScanTest, test_correctness_shortVec)
{
    // short type, "c11"
    auto *olbShort = (omniruntime::vec::Vector<short> *)(recordBatch[9]);
    ASSERT_EQ(olbShort->GetValue(0), 11);
}

TEST_F(ScanTest, test_build_leafs)
{
    std::vector<orc::Literal> litList;
    std::string leafNameString;
    std::unique_ptr<orc::SearchArgumentBuilder> builder = orc::SearchArgumentFactory::newBuilder();
    (*builder).startAnd();
    orc::Literal lit(100L);

    // test EQUALS
    BuildLeaves(PredicateOperatorType::EQUALS, litList, lit, "leaf-0", orc::PredicateDataType::LONG, *builder);

    // test LESS_THAN
    BuildLeaves(PredicateOperatorType::LESS_THAN, litList, lit, "leaf-1", orc::PredicateDataType::LONG, *builder);

    // test LESS_THAN_EQUALS
    BuildLeaves(PredicateOperatorType::LESS_THAN_EQUALS, litList, lit, "leaf-1", orc::PredicateDataType::LONG,
        *builder);

    // test NULL_SAFE_EQUALS
    BuildLeaves(PredicateOperatorType::NULL_SAFE_EQUALS, litList, lit, "leaf-1", orc::PredicateDataType::LONG,
        *builder);

    // test IS_NULL
    BuildLeaves(PredicateOperatorType::IS_NULL, litList, lit, "leaf-1", orc::PredicateDataType::LONG, *builder);

    // test BETWEEN
    std::string tmpStr = "";
    try {
        BuildLeaves(PredicateOperatorType::BETWEEN, litList, lit, "leaf-1", orc::PredicateDataType::LONG, *builder);
    } catch (std::exception &e) {
        tmpStr = e.what();
    }
    ASSERT_EQ(tmpStr, "table scan buildLeaves BETWEEN is not supported!");

    std::string result = ((*builder).end().build())->toString();
    std::string buildString =
        "leaf-0 = (leaf-0 = 100), leaf-1 = (leaf-1 < 100), leaf-2 = (leaf-1 <= 100), leaf-3 = (leaf-1 null_safe_= "
        "100), leaf-4 = (leaf-1 is null), expr = (and leaf-0 leaf-1 leaf-2 leaf-3 leaf-4)";

    ASSERT_EQ(buildString, result);
}
