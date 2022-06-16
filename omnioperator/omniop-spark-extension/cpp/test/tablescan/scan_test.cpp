/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#include "gtest/gtest.h"
#include <stdint.h>
#include <string.h>
#include "../../src/jni/OrcColumnarBatchJniReader.h"
#include "scan_test.h"
#include "orc/sargs/SearchArgument.hh"

static std::string filename = "/resources/orc_data_all_type";
static orc::ColumnVectorBatch *batchPtr;

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
    ASSERT_EQ(tmpLit.toString() == "123456789", true);

    // test get string
    getLiteral(tmpLit, 2, "testStringForLit");
    ASSERT_EQ(tmpLit.toString() == "testStringForLit", true);

    // test get date
    getLiteral(tmpLit, 3, "987654321");
    ASSERT_EQ(tmpLit.toString() == "987654321", true);
}

TEST_F(ScanTest, test_copy_vec)
{
    orc::StructVectorBatch *root = static_cast<orc::StructVectorBatch *>(batchPtr);
    int omniType = 0;
    uint64_t omniVecId = 0;
    // int type
    copyToOmniVec(0, 3, omniType, omniVecId, root->filds[0]);
    ASSERT_EQ(omniType == 1, true);
    omniruntime::vec::IntVector *olbInt = (omniruntime::vec::IntVector *)(omniVecId);
    ASSERT_EQ(olbInt->GetValue(0) == 10, true);
    delete olbInt;

    // varchar type
    copyToOmniVec(60, 16, omniType, omniVecId, root->fields[1]);
    ASSERT_EQ(omniType == 15, true);
    uint8_t *actualChar = nullptr;
    omniruntime::vec::VarcharVector * olbVc = (omniruntime::vec::VarcharVector *)(omniVecId);
    int len =  olbVc->GetValue(0, &actualChar);
    std::string actualStr(reinterpret_cast<char *>(actualChar), 0, len);
    ASSERT_EQ(actualStr == "varchar_1", true);
    delete olbVc;

    // string type
    copyToOmniVec(0, 7, omniType, omniVecId, root->fields[2]);
    ASSERT_EQ(omniType == 15, true);
    omniruntime::vec::VarcharVector *olbStr = (omniruntime::vec::VarcharVector *)(omniVecId);
    len = olbStr->GetValue(0, &actualChar);
    std::string actualStr2(reinterpret_cast<char *>(actualChar), 0, len);
    ASSERT_EQ(actualStr2 == "string_type_1", true);
    delete olbStr;

    // bigint type
    copyToOmniVec(0, 4, omniType, omniVecId, root->fields[3]);
    ASSERT_EQ(omniTYpe == 2, true);
    omniruntime::vec::LongVector *olbLong = (omniruntime::vec::LongVector *)(omniVecId);
    ASSERT_EQ(olbLong-GetValue(0) == 10000, true);
    delete olbLong;

    // char type
    copyToOmniVec(40, 17, omniType, omniVecId, root->fields[4]);
    ASSERT_EQ(omniType == 15, true);
    omniruntime::vec::VarcharVector *olbChar40 = (omniruntime::vec::VarcharVector *)(omniVecId);
    len = olbChar40->GetValue(0, &actualChar);
    std::string actualStr3(reinterpret_cast<char *>(actualChar), 0, len);
    ASSERT_EQ(actualStr3 == "char_1", true);
    delete olbChar40;
}

TEST_F(ScanTest, test_build_leafs)
{
    int leafOp = 0;
    std::vector<orc::Literal> litList;
    std::string leafNameString;
    int leafType = 0;
    std::unique_ptr<orc::SearchArgumentBuilder> builder = orc::SearchArigumentFactory::newBuilder();
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
        "100), leaf-3 = (leaf-1 is null), expt = (and leaf-0 leaf-1 leaf-2 leaf-3 leaf-4)";

    ASSERT_EQ(buildString == result, true);
}
