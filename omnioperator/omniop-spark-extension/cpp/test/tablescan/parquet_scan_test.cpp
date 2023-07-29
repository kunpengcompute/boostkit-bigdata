/**
 * Copyright (C) 2020-2023. Huawei Technologies Co., Ltd. All rights reserved.
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

#include <type/date32.h>
#include <gtest/gtest.h>
#include <cstring>
#include "scan_test.h"
#include "tablescan/ParquetReader.h"

using namespace spark::reader;
using namespace arrow;
using namespace omniruntime::vec;

/*
 * CREATE TABLE `parquet_test` ( `c1` int, `c2` varChar(60), `c3` string, `c4` bigint,
 * `c5` char(40), `c6` float, `c7` double, `c8` decimal(9,8), `c9` decimal(18,5),
 * `c10` boolean, `c11` smallint, `c12` timestamp, `c13` date)stored as parquet;
 *
 * insert into  `parquet_test` values (10, "varchar_1", "string_type_1", 10000, "char_1",
 * 11.11, 1111.1111, null 131.11110, true, 11, '2021-11-30 17:00:11', '2021-12-01');
 */
TEST(read, test_parquet_reader)
{
    std::string filename = "/resources/parquet_data_all_type";
    filename = PROJECT_PATH + filename;
    const std::vector<int> row_group_indices = {0};
    const std::vector<int> column_indices = {0, 1, 3, 6, 7, 8, 9, 10, 12};

    ParquetReader *reader = new ParquetReader();
    std::string ugi = "root@sample";
    auto state1 = reader->InitRecordReader(filename, 1024, row_group_indices, column_indices, ugi);
    ASSERT_EQ(state1, Status::OK());

    std::shared_ptr<arrow::RecordBatch> batch;
    auto state2 = reader->ReadNextBatch(&batch);
    ASSERT_EQ(state2, Status::OK());
    std::cout << "num_rows: " << batch->num_rows() << std::endl;
    std::cout << "num_columns: " << batch->num_columns() << std::endl;
    std::cout << "Print: " << batch->ToString() << std::endl;
    auto pair = TransferToOmniVecs(batch);

    BaseVector *intVector = reinterpret_cast<BaseVector *>(pair.second[0]);
    auto int_result = static_cast<int32_t *>(omniruntime::vec::VectorHelper::UnsafeGetValues(intVector));
    ASSERT_EQ(*int_result, 10);

    auto varCharVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(pair.second[1]);
    std::string str_expected = "varchar_1";
    ASSERT_TRUE(str_expected == varCharVector->GetValue(0));

    BaseVector *longVector = reinterpret_cast<BaseVector *>(pair.second[2]);
    auto long_result = static_cast<int64_t *>(omniruntime::vec::VectorHelper::UnsafeGetValues(longVector));
    ASSERT_EQ(*long_result, 10000);

    BaseVector *doubleVector = reinterpret_cast<BaseVector *>(pair.second[3]);
    auto double_result = static_cast<double *>(omniruntime::vec::VectorHelper::UnsafeGetValues(doubleVector));
    ASSERT_EQ(*double_result, 1111.1111);

    BaseVector *nullVector = reinterpret_cast<BaseVector *>(pair.second[4]);
    ASSERT_TRUE(nullVector->IsNull(0));

    BaseVector *decimal64Vector = reinterpret_cast<BaseVector *>(pair.second[5]);
    auto decimal64_result = static_cast<int64_t *>(omniruntime::vec::VectorHelper::UnsafeGetValues(decimal64Vector));
    ASSERT_EQ(*decimal64_result, 13111110);

    BaseVector *booleanVector = reinterpret_cast<BaseVector *>(pair.second[6]);
    auto boolean_result = static_cast<bool *>(omniruntime::vec::VectorHelper::UnsafeGetValues(booleanVector));
    ASSERT_EQ(*boolean_result, true);

    BaseVector *smallintVector = reinterpret_cast<BaseVector *>(pair.second[7]);
    auto smallint_result = static_cast<int16_t *>(omniruntime::vec::VectorHelper::UnsafeGetValues(smallintVector));
    ASSERT_EQ(*smallint_result, 11);

    BaseVector *dateVector = reinterpret_cast<BaseVector *>(pair.second[8]);
    auto date_result = static_cast<int32_t *>(omniruntime::vec::VectorHelper::UnsafeGetValues(dateVector));
    omniruntime::type::Date32 date32(*date_result);
    char chars[11];
    date32.ToString(chars, 11);
    std::string date_expected(chars);
    ASSERT_TRUE(date_expected == "2021-12-01");

    delete reader;
    delete intVector;
    delete varCharVector;
    delete longVector;
    delete doubleVector;
    delete nullVector;
    delete decimal64Vector;
    delete booleanVector;
    delete smallintVector;
    delete dateVector;
}

TEST(read, test_decimal128_copy)
{
    auto decimal_type = arrow::decimal(20, 1);
    arrow::Decimal128Builder builder(decimal_type);
    arrow::Decimal128 value(20230420);
    auto s1 = builder.Append(value);
    std::shared_ptr<Array> array;
    auto s2 = builder.Finish(&array);

    int omniTypeId = 0;
    uint64_t omniVecId = 0;
    spark::reader::CopyToOmniVec(decimal_type, omniTypeId, omniVecId, array);

    BaseVector *decimal128Vector = reinterpret_cast<BaseVector *>(omniVecId);
    auto decimal128_result =
        static_cast<omniruntime::type::Decimal128 *>(omniruntime::vec::VectorHelper::UnsafeGetValues(decimal128Vector));
    ASSERT_TRUE((*decimal128_result).ToString() == "20230420");

    delete decimal128Vector;
}