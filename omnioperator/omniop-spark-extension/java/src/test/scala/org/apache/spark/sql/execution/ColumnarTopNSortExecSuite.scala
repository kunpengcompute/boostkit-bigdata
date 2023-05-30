/*
 * Copyright (C) 2022-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package org.apache.spark.sql.execution

import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types._

class ColumnarTopNSortExecSuite extends ColumnarSparkPlanTest {

  private var dealer: DataFrame = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()

    dealer = spark.createDataFrame(
      sparkContext.parallelize(Seq(
          Row(1,"shanghai",10),
          Row(2, "chengdu", 1),
          Row(3,"guangzhou", 7),
          Row(4, "beijing", 20),
          Row(5, "hangzhou", 4),
          Row(6, "tianjing", 3),
          Row(7, "shenzhen", 5),
          Row(8, "changsha", 5),
          Row(9,"nanjing", 5),
          Row(10, "wuhan", 6)
      )),new StructType()
          .add("id", IntegerType)
          .add("city", StringType)
          .add("sales", IntegerType))
    dealer.createOrReplaceTempView("dealer")
  }

  test("Test topNSort") {
    val sql1 ="select * from (SELECT city, row_number() OVER (ORDER BY sales) AS rn FROM dealer) where rn<4 order by rn;"
    assertColumnarTopNSortExecAndSparkResultEqual(sql1, true)

    val sql2 = "select * from (SELECT city, row_number() OVER (ORDER BY sales) AS rn FROM dealer) where rn <4 order by rn;"
    assertColumnarTopNSortExecAndSparkResultEqual(sql2, false)

    val sql3 = "select * from (SELECT city, row_number() OVER (PARTITION BY city ORDER BY sales) AS rn FROM dealer) where rn <4 order by rn;"
    assertColumnarTopNSortExecAndSparkResultEqual(sql3, false)
  }

  private def assertColumnarTopNSortExecAndSparkResultEqual(sql: String, hasColumnarTopNSortExec: Boolean = true): Unit = {
    // run ColumnarTopNSortExec config
    spark.conf.set("spark.omni.sql.columnar.topNSort", true)
    spark.conf.set("spark.sql.execution.topNPushDownForWindow.enabled", true)
    spark.conf.set("spark.sql.execution.topNPushDownForWindow.threshold", 100)
    val omniResult = spark.sql(sql)
    val omniPlan = omniResult.queryExecution.executedPlan
    if (hasColumnarTopNSortExec) {
      assert(omniPlan.find(_.isInstanceOf[ColumnarTopNSortExec]).isDefined,
        s"SQL:${sql}\n@OmniEnv no ColumnarTopNSortExec, omniPlan:${omniPlan}")
    }

    // run TopNSortExec config
    spark.conf.set("spark.omni.sql.columnar.topNSort", false)
    val sparkResult = spark.sql(sql)
    val sparkPlan = sparkResult.queryExecution.executedPlan
    assert(sparkPlan.find(_.isInstanceOf[ColumnarTopNSortExec]).isEmpty,
      s"SQL:${sql}\n@SparkEnv have ColumnarTopNSortExec, sparkPlan:${sparkPlan}")
    assert(sparkPlan.find(_.isInstanceOf[TopNSortExec]).isDefined,
      s"SQL:${sql}\n@SparkEnv no TopNSortExec, sparkPlan:${sparkPlan}")
    // DataFrame do not support comparing with equals method, use DataFrame.except instead
    // DataFrame.except can do equal for rows misorder(with and without order by are same)
    assert(omniResult.except(sparkResult).isEmpty,
      s"SQL:${sql}\nomniResult:${omniResult.show()}\nsparkResult:${sparkResult.show()}\n")
    spark.conf.set("spark.omni.sql.columnar.topNSort", true)
    spark.conf.set("spark.sql.execution.topNPushDownForWindow.enabled", false)
  }
}
