/*
 * Copyright (C) 2024-2025. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.forsql

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf

import java.sql.Date


class ColumnarDateTypeSqlSuite extends ColumnarSparkPlanTest {

  import testImplicits._

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key, "LEGACY")

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    Seq[(Int, Date, Date)](
      (1, Date.valueOf("2024-10-10"), Date.valueOf("2024-10-10")),
      (2, Date.valueOf("1024-02-29"), Date.valueOf("1024-03-01")),
      (3, null, Date.valueOf("1024-10-10"))
    ).toDF("int_c", "date_c1", "date_c2").write.format("orc").saveAsTable("date_test_orc")

    Seq[(Int, Date, Date)](
      (1, Date.valueOf("2024-10-10"), Date.valueOf("2024-10-10")),
      (2, Date.valueOf("1024-02-29"), Date.valueOf("1024-03-01")),
      (3, null, Date.valueOf("1024-10-10"))
    ).toDF("int_c", "date_c1", "date_c2").write.format("parquet").saveAsTable("date_test_parquet")
  }

  protected override def afterAll(): Unit = {
    spark.sql("drop table if exists date_test_orc")
    spark.sql("drop table if exists date_test_parquet")
    super.afterAll()
  }

  test("Test rebaseJulianToGregorianDays") {
    val orcRes = spark.sql("select * from date_test_orc")
    var executedPlan = orcRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarFileSourceScanExec]).isDefined, s"ColumnarFileSourceScanExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      orcRes,
      Seq(
        Row(1, Date.valueOf("2024-10-10"), Date.valueOf("2024-10-10")),
        Row(2, Date.valueOf("1024-02-29"), Date.valueOf("1024-03-01")),
        Row(3, null, Date.valueOf("1024-10-10")))
    )

    val parquetRes = spark.sql("select * from date_test_parquet")
    executedPlan = parquetRes.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarFileSourceScanExec]).isDefined, s"ColumnarFileSourceScanExec not happened, executedPlan as follows： \n$executedPlan")

    checkAnswer(
      parquetRes,
      Seq(
        Row(1, Date.valueOf("2024-10-10"), Date.valueOf("2024-10-10")),
        Row(2, Date.valueOf("1024-02-29"), Date.valueOf("1024-03-01")),
        Row(3, null, Date.valueOf("1024-10-10")))
    )
  }
}