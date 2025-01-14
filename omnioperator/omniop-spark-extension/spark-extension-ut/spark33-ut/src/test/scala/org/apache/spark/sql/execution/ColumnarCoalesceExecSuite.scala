/*
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

class ColumnarCoalesceExecSuite extends ColumnarSparkPlanTest {

  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  private var dealerDf: DataFrame = _
  private var dealerExpect: Seq[Row] = _
  private var floatDealerDf: DataFrame = _
  private var floatDealerExpect: Seq[Row] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // for normal case
    dealerDf = Seq[(Int, String, String, Int)](
      (100, "Fremont", "Honda Civic", 10),
      (100, "Fremont", "Honda Accord", 15),
      (100, "Fremont", "Honda CRV", 7),
      (200, "Dublin", "Honda Civic", 20),
      (200, "Dublin", "Honda Accord", 10),
      (200, "Dublin", "Honda CRV", 3),
      (300, "San Jose", "Honda Civic", 5),
      (300, "San Jose", "Honda Accord", 8),
    ).toDF("id", "city", "car_model", "quantity")
    dealerDf.createOrReplaceTempView("dealer")

    dealerExpect = Seq(
      Row(100, "Fremont", 10),
      Row(100, "Fremont", 15),
      Row(100, "Fremont", 7),
      Row(200, "Dublin", 20),
      Row(200, "Dublin", 10),
      Row(200, "Dublin", 3),
      Row(300, "San Jose", 5),
      Row(300, "San Jose", 8),
    )

    // for rollback case
    floatDealerDf = Seq[(Int, String, String, Float)](
      (100, "Fremont", "Honda Civic", 10.00F),
      (100, "Fremont", "Honda Accord", 15.00F),
      (100, "Fremont", "Honda CRV", 7.00F),
      (200, "Dublin", "Honda Civic", 20.00F),
      (200, "Dublin", "Honda Accord", 10.00F),
      (200, "Dublin", "Honda CRV", 3.00F),
      (300, "San Jose", "Honda Civic", 5.00F),
      (300, "San Jose", "Honda Accord", 8.00F),
    ).toDF("id", "city", "car_model", "quantity")
    floatDealerDf.createOrReplaceTempView("float_dealer")

    floatDealerExpect = Seq(
      Row(100, "Fremont", 10.00F),
      Row(100, "Fremont", 15.00F),
      Row(100, "Fremont", 7.00F),
      Row(200, "Dublin", 20.00F),
      Row(200, "Dublin", 10.00F),
      Row(200, "Dublin", 3.00F),
      Row(300, "San Jose", 5.00F),
      Row(300, "San Jose", 8.00F),
    )
  }

  test("use ColumnarCoalesceExec with normal input") {
    val result = spark.sql("SELECT /*+ COALESCE(3) */ id, city, quantity FROM dealer")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarCoalesceExec]).isDefined)
    assert(plan.find(_.isInstanceOf[CoalesceExec]).isEmpty)
    checkAnswer(result, dealerExpect)
  }

  test("use ColumnarCoalesceExec with normal input and not enable ColumnarExpandExec") {
    // default is true
    spark.conf.set("spark.omni.sql.columnar.coalesce", false)
    val result = spark.sql("SELECT /*+ COALESCE(3) */ id, city, quantity FROM dealer")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarCoalesceExec]).isEmpty)
    assert(plan.find(_.isInstanceOf[CoalesceExec]).isDefined)
    spark.conf.set("spark.omni.sql.columnar.coalesce", true)
    checkAnswer(result, dealerExpect)
  }

  test("use ColumnarCoalesceExec with input not support and rollback") {
    val result = spark.sql("SELECT /*+ COALESCE(3) */ id, city, quantity FROM float_dealer")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarCoalesceExec]).isEmpty)
    assert(plan.find(_.isInstanceOf[CoalesceExec]).isDefined)
    checkAnswer(result, floatDealerExpect)
  }

  test("ColumnarCoalesceExec and CoalesceExec return the same result") {
    val sql1 = "SELECT /*+ COALESCE(3) */ id, city, car_model, quantity FROM dealer"
    checkCoalesceExecAndColumnarCoalesceExecAgree(sql1)

    val sql2 = "SELECT /*+ COALESCE(3) */ id, city, car_model, quantity FROM float_dealer"
    checkCoalesceExecAndColumnarCoalesceExecAgree(sql2, true)
  }

  // check CoalesceExec and ColumnarCoalesceExec return the same result
  private def checkCoalesceExecAndColumnarCoalesceExecAgree(sql: String,
                                                            rollBackByInputCase: Boolean = false): Unit = {
      spark.conf.set("spark.omni.sql.columnar.coalesce", true)
      val omniResult = spark.sql(sql)
      val omniPlan = omniResult.queryExecution.executedPlan
      if (rollBackByInputCase) {
        assert(omniPlan.find(_.isInstanceOf[ColumnarCoalesceExec]).isEmpty,
          s"SQL:${sql}\n@SparkEnv not have ColumnarCoalesceExec, sparkPlan:${omniPlan}")
        assert(omniPlan.find(_.isInstanceOf[CoalesceExec]).isDefined,
          s"SQL:${sql}\n@SparkEnv have CoalesceExec, sparkPlan:${omniPlan}")
      } else {
        assert(omniPlan.find(_.isInstanceOf[ColumnarCoalesceExec]).isDefined,
          s"SQL:${sql}\n@SparkEnv have ColumnarCoalesceExec, sparkPlan:${omniPlan}")
        assert(omniPlan.find(_.isInstanceOf[CoalesceExec]).isEmpty,
          s"SQL:${sql}\n@SparkEnv not have CoalesceExec, sparkPlan:${omniPlan}")
      }

      spark.conf.set("spark.omni.sql.columnar.coalesce", false)
      val sparkResult = spark.sql(sql)
      val sparkPlan = sparkResult.queryExecution.executedPlan
      assert(sparkPlan.find(_.isInstanceOf[ColumnarCoalesceExec]).isEmpty,
        s"SQL:${sql}\n@SparkEnv not have ColumnarCoalesceExec, sparkPlan:${sparkPlan}")
      assert(sparkPlan.find(_.isInstanceOf[CoalesceExec]).isDefined,
        s"SQL:${sql}\n@SparkEnv have CoalesceExec, sparkPlan:${sparkPlan}")
      // DataFrame do not support comparing with equals method, use DataFrame.except instead
      assert(omniResult.except(sparkResult).isEmpty)
      spark.conf.set("spark.omni.sql.columnar.coalesce", true)
  }

  test("use ColumnarCoalesceExec by RDD api to check repartition") {
    // reinit to 6 partitions
    val dealerDf6P = dealerDf.repartition(6)
    assert(dealerDf6P.rdd.partitions.length == 6)

    // coalesce to 2 partitions
    val dealerDfCoalesce2P = dealerDf6P.coalesce(2)
    assert(dealerDfCoalesce2P.rdd.partitions.length == 2)
    val dealerDfCoalesce2Plan = dealerDfCoalesce2P.queryExecution.executedPlan
    assert(dealerDfCoalesce2Plan.find(_.isInstanceOf[ColumnarCoalesceExec]).isDefined,
      s"sparkPlan:${dealerDfCoalesce2Plan}")
    assert(dealerDfCoalesce2Plan.find(_.isInstanceOf[CoalesceExec]).isEmpty,
      s"sparkPlan:${dealerDfCoalesce2Plan}")
    // always return 8 rows
    assert(dealerDfCoalesce2P.collect().length == 8)
  }
}

