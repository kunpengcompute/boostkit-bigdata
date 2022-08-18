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

import org.apache.spark.sql.functions.{sum, count}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

class ColumnarHashAggregateExecSuite extends ColumnarSparkPlanTest {
  private var df: DataFrame = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    df = spark.createDataFrame(
      sparkContext.parallelize(Seq(
        Row(1, 2.0, 1L, "a"),
        Row(1, 2.0, 2L, null),
        Row(2, 1.0, 3L, "c"),
        Row(null, null, 6L, "e"),
        Row(null, 5.0, 7L, "f")
      )), new StructType().add("a", IntegerType).add("b", DoubleType)
        .add("c", LongType).add("d", StringType))
  }

  test("validate columnar hashAgg exec happened") {
    val res = df.groupBy("a").agg(sum("b"))
    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n${res.queryExecution.executedPlan}")
  }

  test("check columnar hashAgg result") {
    val res = testData2.groupBy("a").agg(sum("b"))
    checkAnswer(
      res,
      Seq(Row(1, 3), Row(2, 3), Row(3, 3))
    )
  }

  test("check columnar hashAgg result with null") {
    val res = df.filter(df("a").isNotNull && df("d").isNotNull).groupBy("a").agg(sum("b"))
    checkAnswer(
      res,
      Seq(Row(1, 2.0), Row(2, 1.0))
    )
  }

  test("Test ColumnarHashAggregateExec happen and result is correct when execute count(*) api") {
    val res = df.agg(count("*"))
    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n${res.queryExecution.executedPlan}")
    checkAnswer(
      res,
      Seq(Row(5))
    )
  }

  test("Test ColumnarHashAggregateExec happen and result " +
    "is correct when execute count(*) api with group by") {
    val res = df.groupBy("a").agg(count("*"))
    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n${res.queryExecution.executedPlan}")
    checkAnswer(
      res,
      Seq(Row(1, 2), Row(2, 1), Row(null, 2))
    )
  }
}
