/*
 * Copyright (C) 2025-2025. Huawei Technologies Co., Ltd. All rights reserved.
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

class ColumnarWindowGroupLimitExecSuite extends ColumnarSparkPlanTest {

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
          Row(10, "wuhan", 6),
          Row(11, "wuhan", 9),
          Row(12, "wuhan", 9),
          Row(12, "wuhan", 8)
      )),new StructType()
          .add("id", IntegerType)
          .add("city", StringType)
          .add("sales", IntegerType))
    dealer.createOrReplaceTempView("dealer")
  }

  test("Test windowGroupLimit") {
    val sql1 = "select * from (SELECT city, rank() OVER (ORDER BY sales) AS rk FROM dealer) where rk < 4 order by rk;"
    assertColumnarWindowGroupLimitExecAndSparkResultEqual(sql1, true, true)

    val sql2 = "select * from (SELECT city, rank() OVER (PARTITION BY city ORDER BY sales) AS rk FROM dealer) where rk < 4 order by rk;"
    assertColumnarWindowGroupLimitExecAndSparkResultEqual(sql2, true, true)

    val sql3 = "select * from (SELECT city, sales, row_number() OVER (PARTITION BY city ORDER BY sales) AS rn FROM dealer) where rn < 4 order by rn;"
    assertColumnarWindowGroupLimitExecAndSparkResultEqual(sql3, true, true)
  }

  private def assertColumnarWindowGroupLimitExecAndSparkResultEqual(sql: String, hasColumnarWindowGroupLimitExec: Boolean = true,
    hasWindowGroupLimitExec: Boolean = false): Unit = {
    // run WindowGroupLimitExec config
    spark.conf.set("spark.omni.sql.columnar.windowGroupLimit", true)
    spark.conf.set("spark.sql.adaptive.enabled", true)
    val omniResult = spark.sql(sql)
    omniResult.collect()
    val omniPlan = omniResult.queryExecution.executedPlan.toString()
    if (hasColumnarWindowGroupLimitExec) {
      assert(omniPlan.contains("ColumnarWindowGroupLimit"),
        s"SQL:${sql}\n@OmniEnv no ColumnarWindowGroupLimitExec, omniPlan:${omniPlan}")
    }

    spark.conf.set("spark.sql.adaptive.enabled", false)
    val omniResult_withoutAQE = spark.sql(sql)
    omniResult_withoutAQE.collect()
    val omniPlan_withoutAQE = omniResult_withoutAQE.queryExecution.executedPlan.toString()
    if (hasColumnarWindowGroupLimitExec) {
      assert(omniPlan_withoutAQE.contains("ColumnarWindowGroupLimit"),
        s"SQL:${sql}\n@OmniEnv no ColumnarWindowGroupLimitExec, omniPlan_withoutAQE:${omniPlan_withoutAQE}")
    }
    assert(omniResult.except(omniResult_withoutAQE).isEmpty,
      s"SQL:${sql}\nomniResult:${omniResult.show()}\nomniResult_withoutAQE:${omniResult_withoutAQE.show()}\n")

    // run WindowGroupLimitExec config
    spark.conf.set("spark.omni.sql.columnar.windowGroupLimit", false)
    spark.conf.set("spark.sql.adaptive.enabled", true)
    val sparkResult = spark.sql(sql)
    sparkResult.collect()
    val sparkPlan = sparkResult.queryExecution.executedPlan.toString()
    assert(!sparkPlan.contains("ColumnarWindowGroupLimit"),
      s"SQL:${sql}\n@SparkEnv have ColumnarWindowGroupLimitExec, sparkPlan:${sparkPlan}")
    if (hasWindowGroupLimitExec) {
      assert(sparkPlan.contains("WindowGroupLimit"),
        s"SQL:${sql}\n@SparkEnv no WindowGroupLimitExec, sparkPlan:${sparkPlan}")
    }
    // DataFrame do not support comparing with equals method, use DataFrame.except instead
    // DataFrame.except can do equal for rows misorder(with and without order by are same)
    assert(omniResult.except(sparkResult).isEmpty,
      s"SQL:${sql}\nomniResult:${omniResult.show()}\nsparkResult:${sparkResult.show()}\n")
  }
}
