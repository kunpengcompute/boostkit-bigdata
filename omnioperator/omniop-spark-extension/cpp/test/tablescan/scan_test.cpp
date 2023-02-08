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
#include <stdint.h>
#include <string.h>
#include "../../src/jni/OrcColumnarBatchJniReader.h"
#include "scan_test.h"
#include "orc/sargs/SearchArgument.hh"

static std::string filename = "/resources/orc_data_all_type";
static orc::ColumnVectorBatch *batchPtr;
static orc::StructVectorBatch *root;

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
        std::unique_ptr<orc::Reader> reader = orc::createReader(orc::readFile(PROJECT_PATH + filename), readerOpts);
        std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader();
        std::unique_ptr<orc::ColumnVectorBatch> batch = rowReader->createRowBatch(4096);
        rowReader->next(*batch);
        batchPtr = batch.release();
        root = static_cast<orc::StructVectorBatch *>(batchPtr);
    }

    // run after each case...
    virtual void TearDown() override {
        delete batchPtr;
    }
};

TEST_F(ScanTest, test_literal_get_long)
{
    orc::Literal tmpLit(0L);

    // test get long
    getLiteral(tmpLit, (int)(orc::PredicateDataType::LONG), "655361");
    ASSERT_EQ(tmpLit.getLong(), 655361);
    getLiteral(tmpLit, (int)(orc::PredicateDataType::LONG), "-655361");
    ASSERT_EQ(tmpLit.getLong(), -655361);
    getLiteral(tmpLit, (int)(orc::PredicateDataType::LONG), "0");
    ASSERT_EQ(tmpLit.getLong(), 0);
}

TEST_F(ScanTest, test_literal_get_float)
{
    orc::Literal tmpLit(0L);

    // test get float
    getLiteral(tmpLit, (int)(orc::PredicateDataType::FLOAT), "12345.6789");
    ASSERT_EQ(tmpLit.getFloat(), 12345.6789);
    getLiteral(tmpLit, (int)(orc::PredicateDataType::FLOAT), "-12345.6789");
    ASSERT_EQ(tmpLit.getFloat(), -12345.6789);
    getLiteral(tmpLit, (int)(orc::PredicateDataType::FLOAT), "0");
    ASSERT_EQ(tmpLit.getFloat(), 0);
}

TEST_F(ScanTest, test_literal_get_string)
{
    orc::Literal tmpLit(0L);

    // test get string
    getLiteral(tmpLit, (int)(orc::PredicateDataType::STRING), "testStringForLit");
    ASSERT_EQ(tmpLit.getString(), "testStringForLit");
    getLiteral(tmpLit, (int)(orc::PredicateDataType::STRING), "");
    ASSERT_EQ(tmpLit.getString(), "");
}

TEST_F(ScanTest, test_literal_get_date)
{
    orc::Literal tmpLit(0L);

    // test get date
    getLiteral(tmpLit, (int)(orc::PredicateDataType::DATE), "987654321");
    ASSERT_EQ(tmpLit.getDate(), 987654321);
}

TEST_F(ScanTest, test_literal_get_decimal)
{
    orc::Literal tmpLit(0L);

    // test get decimal
    getLiteral(tmpLit, (int)(orc::PredicateDataType::DECIMAL), "199999999999998.998000 22 6");
    ASSERT_EQ(tmpLit.getDecimal().toString(), "199999999999998.998000");
    getLiteral(tmpLit, (int)(orc::PredicateDataType::DECIMAL), "10.998000 10 6");
    ASSERT_EQ(tmpLit.getDecimal().toString(), "10.998000");
    getLiteral(tmpLit, (int)(orc::PredicateDataType::DECIMAL), "-10.998000 10 6");
    ASSERT_EQ(tmpLit.getDecimal().toString(), "-10.998000");
    getLiteral(tmpLit, (int)(orc::PredicateDataType::DECIMAL), "9999.999999 10 6");
    ASSERT_EQ(tmpLit.getDecimal().toString(), "9999.999999");
    getLiteral(tmpLit, (int)(orc::PredicateDataType::DECIMAL), "-0.000000 10 6");
    ASSERT_EQ(tmpLit.getDecimal().toString(), "0.000000");
}

TEST_F(ScanTest, test_literal_get_bool)
{
    orc::Literal tmpLit(0L);

    // test get bool
    getLiteral(tmpLit, (int)(orc::PredicateDataType::BOOLEAN), "true");
    ASSERT_EQ(tmpLit.getBool(), true);
    getLiteral(tmpLit, (int)(orc::PredicateDataType::BOOLEAN), "True");
    ASSERT_EQ(tmpLit.getBool(), true);
    getLiteral(tmpLit, (int)(orc::PredicateDataType::BOOLEAN), "false");
    ASSERT_EQ(tmpLit.getBool(), false);
    getLiteral(tmpLit, (int)(orc::PredicateDataType::BOOLEAN), "False");
    ASSERT_EQ(tmpLit.getBool(), false);
    std::string tmpStr = "";
    try {
        getLiteral(tmpLit, (int)(orc::PredicateDataType::BOOLEAN), "exception");
    } catch (std::exception &e) {
        tmpStr = e.what();
    }
    ASSERT_EQ(tmpStr, "Invalid input for stringToBool.");
}

TEST_F(ScanTest, test_copy_intVec)
{
    int omniType = 0;
    uint64_t omniVecId = 0;
    // int type
    copyToOmniVec(orc::TypeKind::INT, omniType, omniVecId, root->fields[0]);
    ASSERT_EQ(omniType, omniruntime::type::OMNI_INT);
    auto *olbInt = (omniruntime::vec::Vector<int32_t> *)(omniVecId);
    ASSERT_EQ(olbInt->GetValue(0), 10);
    delete olbInt;
}

TEST_F(ScanTest, test_copy_varCharVec)
{
    int omniType = 0;
    uint64_t omniVecId = 0;
    // varchar type
    copyToOmniVec(orc::TypeKind::VARCHAR, omniType, omniVecId, root->fields[1], 60);
    ASSERT_EQ(omniType, omniruntime::type::OMNI_VARCHAR);
    auto *olbVc = (omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>> *)(
            omniVecId);
    std::string_view actualStr = olbVc->GetValue(0);
    ASSERT_EQ(actualStr, "varchar_1");
    delete olbVc;
}

TEST_F(ScanTest, test_copy_stringVec)
{
    int omniType = 0;
    uint64_t omniVecId = 0;
    // string type
    copyToOmniVec(orc::TypeKind::STRING, omniType, omniVecId, root->fields[2]);
    ASSERT_EQ(omniType, omniruntime::type::OMNI_VARCHAR);
    auto *olbStr = (omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>> *)(
            omniVecId);
    std::string_view actualStr = olbStr->GetValue(0);
    ASSERT_EQ(actualStr, "string_type_1");
    delete olbStr;
}

TEST_F(ScanTest, test_copy_longVec)
{
    int omniType = 0;
    uint64_t omniVecId = 0;
    // bigint type
    copyToOmniVec(orc::TypeKind::LONG, omniType, omniVecId, root->fields[3]);
    ASSERT_EQ(omniType, omniruntime::type::OMNI_LONG);
    auto *olbLong = (omniruntime::vec::Vector<int64_t> *)(omniVecId);
    ASSERT_EQ(olbLong->GetValue(0), 10000);
    delete olbLong;
}

TEST_F(ScanTest, test_copy_charVec)
{
    int omniType = 0;
    uint64_t omniVecId = 0;
    // char type
    copyToOmniVec(orc::TypeKind::CHAR, omniType, omniVecId, root->fields[4], 40);
    ASSERT_EQ(omniType, omniruntime::type::OMNI_VARCHAR);
    auto *olbChar = (omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>> *)(
            omniVecId);
    std::string_view actualStr = olbChar->GetValue(0);
    ASSERT_EQ(actualStr, "char_1");
    delete olbChar;
}

TEST_F(ScanTest, test_copy_doubleVec)
{
    int omniType = 0;
    uint64_t omniVecId = 0;
    // double type
    copyToOmniVec(orc::TypeKind::DOUBLE, omniType, omniVecId, root->fields[6]);
    ASSERT_EQ(omniType, omniruntime::type::OMNI_DOUBLE);
    auto *olbDouble = (omniruntime::vec::Vector<double> *)(omniVecId);
    ASSERT_EQ(olbDouble->GetValue(0), 1111.1111);
    delete olbDouble;
}

TEST_F(ScanTest, test_copy_booleanVec)
{
    int omniType = 0;
    uint64_t omniVecId = 0;
    // boolean type
    copyToOmniVec(orc::TypeKind::BOOLEAN, omniType, omniVecId, root->fields[9]);
    ASSERT_EQ(omniType, omniruntime::type::OMNI_BOOLEAN);
    auto *olbBoolean = (omniruntime::vec::Vector<bool> *)(omniVecId);
    ASSERT_EQ(olbBoolean->GetValue(0), true);
    delete olbBoolean;
}

TEST_F(ScanTest, test_copy_shortVec)
{
    int omniType = 0;
    uint64_t omniVecId = 0;
    // short type
    copyToOmniVec(orc::TypeKind::SHORT, omniType, omniVecId, root->fields[10]);
    ASSERT_EQ(omniType, omniruntime::type::OMNI_SHORT);
    auto *olbShort = (omniruntime::vec::Vector<short> *)(omniVecId);
    ASSERT_EQ(olbShort->GetValue(0), 11);
    delete olbShort;
}

TEST_F(ScanTest, test_build_leafs)
{
    std::vector<orc::Literal> litList;
    std::string leafNameString;
    std::unique_ptr<orc::SearchArgumentBuilder> builder = orc::SearchArgumentFactory::newBuilder();
    (*builder).startAnd();
    orc::Literal lit(100L);

    // test EQUALS
    buildLeaves(PredicateOperatorType::EQUALS, litList, lit, "leaf-0", orc::PredicateDataType::LONG, *builder);

    // test LESS_THAN
    buildLeaves(PredicateOperatorType::LESS_THAN, litList, lit, "leaf-1", orc::PredicateDataType::LONG, *builder);

    // test LESS_THAN_EQUALS
    buildLeaves(PredicateOperatorType::LESS_THAN_EQUALS, litList, lit, "leaf-1", orc::PredicateDataType::LONG, *builder);

    // test NULL_SAFE_EQUALS
    buildLeaves(PredicateOperatorType::NULL_SAFE_EQUALS, litList, lit, "leaf-1", orc::PredicateDataType::LONG, *builder);

    // test IS_NULL
    buildLeaves(PredicateOperatorType::IS_NULL, litList, lit, "leaf-1", orc::PredicateDataType::LONG, *builder);

    // test BETWEEN
    std::string tmpStr = "";
    try {
        buildLeaves(PredicateOperatorType::BETWEEN, litList, lit, "leaf-1", orc::PredicateDataType::LONG, *builder);
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
