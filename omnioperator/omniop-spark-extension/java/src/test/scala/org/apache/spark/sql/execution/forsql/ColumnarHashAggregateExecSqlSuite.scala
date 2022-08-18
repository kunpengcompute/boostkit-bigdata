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

package org.apache.spark.sql.execution.forsql

import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.{ColumnarHashAggregateExec, ColumnarSparkPlanTest}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

class ColumnarHashAggregateExecSqlSuite extends ColumnarSparkPlanTest {
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
      )), new StructType().add("intCol", IntegerType).add("doubleCol", DoubleType)
        .add("longCol", LongType).add("stringCol", StringType))
    df.createOrReplaceTempView("test_table")
  }

  test("Test ColumnarHashAggregateExec happen and result is correct when execute count(*)") {
    val res = spark.sql("select count(*) from test_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[HashAggregateExec]).isEmpty, s"HashAggregateExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(Row(5))
    )
  }

  test("Test ColumnarHashAggregateExec happen and result is correct when execute count(1)") {
    val res = spark.sql("select count(1) from test_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[HashAggregateExec]).isEmpty, s"HashAggregateExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(Row(5))
    )
  }

  test("Test ColumnarHashAggregateExec happen and result is correct when execute count(-1)") {
    val res = spark.sql("select count(-1) from test_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[HashAggregateExec]).isEmpty, s"HashAggregateExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(Row(5))
    )
  }

  test("Test ColumnarHashAggregateExec happen and result " +
    "is correct when execute otherAgg-count(*)") {
    val res = spark.sql("select max(intCol), count(*) from test_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[HashAggregateExec]).isEmpty, s"HashAggregateExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(Row(2, 5))
    )
  }

  test("Test ColumnarHashAggregateExec happen and result " +
    "is correct when execute count(*)-otherAgg") {
    val res = spark.sql("select count(*), max(intCol) from test_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[HashAggregateExec]).isEmpty, s"HashAggregateExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(Row(5, 2))
    )
  }

  test("Test ColumnarHashAggregateExec happen and result " +
    "is correct when execute count(*)-count(*)") {
    val res = spark.sql("select count(*), count(*) from test_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[HashAggregateExec]).isEmpty, s"HashAggregateExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(Row(5, 5))
    )
  }

  test("Test ColumnarHashAggregateExec happen and result " +
    "is correct when execute count(*)-otherAgg-count(*)") {
    val res = spark.sql("select count(*), max(intCol), count(*) from test_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[HashAggregateExec]).isEmpty, s"HashAggregateExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(Row(5, 2, 5))
    )
  }

  test("Test ColumnarHashAggregateExec happen and result " +
    "is correct when execute otherAgg-count(*)-otherAgg") {
    val res = spark.sql("select max(intCol), count(*), min(intCol) from test_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[HashAggregateExec]).isEmpty, s"HashAggregateExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(Row(2, 5, 1))
    )
  }

  test("Test ColumnarHashAggregateExec happen and result " +
    "is correct when execute otherAgg-count(*)-count(*)") {
    val res = spark.sql("select max(intCol), count(*), count(*) from test_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[HashAggregateExec]).isEmpty, s"HashAggregateExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(Row(2, 5, 5))
    )
  }

  test("Test ColumnarHashAggregateExec happen and result " +
    "is correct when execute count(*) with group by") {
    val res = spark.sql("select count(*) from test_table group by intCol")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[HashAggregateExec]).isEmpty, s"HashAggregateExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(Row(2), Row(1), Row(2))
    )
  }

  test("Test ColumnarHashAggregateExec happen and result" +
    " is correct when execute count(*) with calculation expr") {
    val res = spark.sql("select count(*) / 2 from test_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[HashAggregateExec]).isEmpty, s"HashAggregateExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(Row(2.5))
    )
  }

  test("Test ColumnarHashAggregateExec happen and result" +
    " is correct when execute count(*) with cast expr") {
    val res = spark.sql("select cast(count(*) as bigint) from test_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[HashAggregateExec]).isEmpty, s"HashAggregateExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(Row(5))
    )
  }

  test("Test ColumnarHashAggregateExec happen and result" +
    " is correct when execute count(*) with subQuery") {
    val res = spark.sql("select count(*) from (select intCol," +
      "count(*) from test_table group by intCol)")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[HashAggregateExec]).isEmpty, s"HashAggregateExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(Row(3))
    )
  }
}