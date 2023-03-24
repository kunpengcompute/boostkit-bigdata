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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StructType}

class ColumnarExecSuite extends ColumnarSparkPlanTest {
  private var dealer: DataFrame = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()

    dealer = spark.createDataFrame(
      sparkContext.parallelize(Seq(
        Row(1, 2.0, false),
        Row(1, 2.0, false),
        Row(2, 1.0, false),
        Row(null, null, false),
        Row(null, 5.0, false),
        Row(6, null, false)
      )), new StructType().add("a", IntegerType).add("b", DoubleType)
        .add("c", BooleanType))
    dealer.createOrReplaceTempView("dealer")
  }

  test("validate columnar transfer exec happened") {
    val sql1 = "SELECT car_model, count(DISTINCT quantity) AS count FROM dealer" +
      " GROUP BY car_model;"
    assertColumnarToRowOmniAndSparkResultEqual(sql1)
  }

  test("spark limit with columnarToRow as child") {

    // fetch parital
    val sql1 = "select * from (select a, b+2 from dealer order by a, b+2) limit 2"
    assertColumnarToRowOmniAndSparkResultEqual(sql1)

    // fetch all
    val sql2 = "select a, b+2 from dealer limit 6"
    assertColumnarToRowOmniAndSparkResultEqual(sql2)

    // fetch all
    val sql3 = "select a, b+2 from dealer limit 10"
    assertColumnarToRowOmniAndSparkResultEqual(sql3)

    // fetch parital
    val sql4 = "select a, b+2 from dealer order by a limit 2"
    assertColumnarToRowOmniAndSparkResultEqual(sql4)

    // fetch all
    val sql5 = "select a, b+2 from dealer order by a limit 6"
    assertColumnarToRowOmniAndSparkResultEqual(sql5)

    // fetch all
    val sql6 = "select a, b+2 from dealer order by a limit 10"
    assertColumnarToRowOmniAndSparkResultEqual(sql6)
  }

  private def assertColumnarToRowOmniAndSparkResultEqual(sql: String): Unit = {

    spark.conf.set("spark.omni.sql.columnar.takeOrderedAndProject", true)
    spark.conf.set("spark.omni.sql.columnar.project", true)
    val omniResult = spark.sql(sql)
    val omniPlan = omniResult.queryExecution.executedPlan
    assert(omniPlan.find(_.isInstanceOf[OmniColumnarToRowExec]).isDefined,
      s"SQL:${sql}\n@OmniEnv no OmniColumnarToRowExec,omniPlan:${omniPlan}")

    spark.conf.set("spark.omni.sql.columnar.takeOrderedAndProject", false)
    spark.conf.set("spark.omni.sql.columnar.project", false)
    val sparkResult = spark.sql(sql)
    val sparkPlan = sparkResult.queryExecution.executedPlan
    assert(sparkPlan.find(_.isInstanceOf[OmniColumnarToRowExec]).isEmpty,
      s"SQL:${sql}\n@SparkEnv have OmniColumnarToRowExec,sparkPlan:${sparkPlan}")

    assert(omniResult.except(sparkResult).isEmpty,
      s"SQL:${sql}\nomniResult:${omniResult.show()}\nsparkResult:${sparkResult.show()}\n")
    spark.conf.set("spark.omni.sql.columnar.takeOrderedAndProject", true)
    spark.conf.set("spark.omni.sql.columnar.project", true)
  }
}
