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

TEST_F(ScanTest, test_get_literal)
{
    orc::Literal tmpLit(0L);
    // test get long
    getLiteral(tmpLit, 0, "123456789");
    ASSERT_EQ(tmpLit.toString(), "123456789");

    // test get string
    getLiteral(tmpLit, 2, "testStringForLit");
    ASSERT_EQ(tmpLit.toString(), "testStringForLit");

    // test get date
    getLiteral(tmpLit, 3, "987654321");
    ASSERT_EQ(tmpLit.toString(), "987654321");
}

TEST_F(ScanTest, test_copy_intVec)
{
    int omniType = 0;
    uint64_t omniVecId = 0;
    // int type
    copyToOmniVec(orc::TypeKind::INT, omniType, omniVecId, root->fields[0]);
    ASSERT_EQ(omniType, omniruntime::type::OMNI_INT);
    omniruntime::vec::IntVector *olbInt = (omniruntime::vec::IntVector *)(omniVecId);
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
    uint8_t *actualChar = nullptr;
    omniruntime::vec::VarcharVector *olbVc = (omniruntime::vec::VarcharVector *)(omniVecId);
    int len = olbVc->GetValue(0, &actualChar);
    std::string actualStr(reinterpret_cast<char *>(actualChar), 0, len);
    ASSERT_EQ(actualStr, "varchar_1");
    delete olbVc;
}

TEST_F(ScanTest, test_copy_stringVec)
{
    int omniType = 0;
    uint64_t omniVecId = 0;
    uint8_t *actualChar = nullptr;
    // string type
    copyToOmniVec(orc::TypeKind::STRING, omniType, omniVecId, root->fields[2]);
    ASSERT_EQ(omniType, omniruntime::type::OMNI_VARCHAR);
    omniruntime::vec::VarcharVector *olbStr = (omniruntime::vec::VarcharVector *)(omniVecId);
    int len = olbStr->GetValue(0, &actualChar);
    std::string actualStr2(reinterpret_cast<char *>(actualChar), 0, len);
    ASSERT_EQ(actualStr2, "string_type_1");
    delete olbStr;
}

TEST_F(ScanTest, test_copy_longVec)
{
    int omniType = 0;
    uint64_t omniVecId = 0;
    // bigint type
    copyToOmniVec(orc::TypeKind::LONG, omniType, omniVecId, root->fields[3]);
    ASSERT_EQ(omniType, omniruntime::type::OMNI_LONG);
    omniruntime::vec::LongVector *olbLong = (omniruntime::vec::LongVector *)(omniVecId);
    ASSERT_EQ(olbLong->GetValue(0), 10000);
    delete olbLong;
}

TEST_F(ScanTest, test_copy_charVec)
{
    int omniType = 0;
    uint64_t omniVecId = 0;
    uint8_t *actualChar = nullptr;
    // char type
    copyToOmniVec(orc::TypeKind::CHAR, omniType, omniVecId, root->fields[4], 40);
    ASSERT_EQ(omniType, omniruntime::type::OMNI_VARCHAR);
    omniruntime::vec::VarcharVector *olbChar40 = (omniruntime::vec::VarcharVector *)(omniVecId);
    int len = olbChar40->GetValue(0, &actualChar);
    std::string actualStr3(reinterpret_cast<char *>(actualChar), 0, len);
    ASSERT_EQ(actualStr3, "char_1");
    delete olbChar40;
}

TEST_F(ScanTest, test_copy_doubleVec)
{
    int omniType = 0;
    uint64_t omniVecId = 0;
    // double type
    copyToOmniVec(orc::TypeKind::DOUBLE, omniType, omniVecId, root->fields[6]);
    ASSERT_EQ(omniType, omniruntime::type::OMNI_DOUBLE);
    omniruntime::vec::DoubleVector *olbDouble = (omniruntime::vec::DoubleVector *)(omniVecId);
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
    omniruntime::vec::BooleanVector *olbBoolean = (omniruntime::vec::BooleanVector *)(omniVecId);
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
    omniruntime::vec::ShortVector *olbShort = (omniruntime::vec::ShortVector *)(omniVecId);
    ASSERT_EQ(olbShort->GetValue(0), 11);
    delete olbShort;
}

TEST_F(ScanTest, test_build_leafs)
{
    int leafOp = 0;
    std::vector<orc::Literal> litList;
    std::string leafNameString;
    int leafType = 0;
    std::unique_ptr<orc::SearchArgumentBuilder> builder = orc::SearchArgumentFactory::newBuilder();
    (*builder).startAnd();
    orc::Literal lit(100L);


    // test equal
    buildLeafs(0, litList, lit, "leaf-0", 0, *builder);

    // test LESS_THAN
    buildLeafs(2, litList, lit, "leaf-1", 0, *builder);

    // test LESS_THAN_EQUALS
    buildLeafs(3, litList, lit, "leaf-1", 0, *builder);

    // test NULL_SAFE_EQUALS
    buildLeafs(1, litList, lit, "leaf-1", 0, *builder);

    // test IS_NULL
    buildLeafs(6, litList, lit, "leaf-1", 0, *builder);

    std::string result = ((*builder).end().build())->toString();
    std::string buildString =
        "leaf-0 = (leaf-0 = 100), leaf-1 = (leaf-1 < 100), leaf-2 = (leaf-1 <= 100), leaf-3 = (leaf-1 null_safe_= "
        "100), leaf-4 = (leaf-1 is null), expr = (and leaf-0 leaf-1 leaf-2 leaf-3 leaf-4)";

    ASSERT_EQ(buildString, result);
}
