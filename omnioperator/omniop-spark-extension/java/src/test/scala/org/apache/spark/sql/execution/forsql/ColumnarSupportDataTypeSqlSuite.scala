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
import org.apache.spark.sql.execution.joins.{ColumnarBroadcastHashJoinExec, ColumnarShuffledHashJoinExec, ColumnarSortMergeJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.execution.{ColumnarConditionProjectExec, ColumnarExpandExec, ColumnarFilterExec, ColumnarHashAggregateExec, ColumnarProjectExec, ColumnarShuffleExchangeExec, ColumnarSortExec, ColumnarSparkPlanTest, ColumnarTakeOrderedAndProjectExec, ColumnarUnionExec, ColumnarWindowExec, ExpandExec, FilterExec, ProjectExec, SortExec, TakeOrderedAndProjectExec, UnionExec}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

class ColumnarSupportDataTypeSqlSuite extends ColumnarSparkPlanTest {
  private var shortDf: DataFrame = _
  private var joinShort1Df: DataFrame = _
  private var joinShort2Df: DataFrame = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    shortDf = spark.createDataFrame(
      sparkContext.parallelize(Seq(
        Row(2, 10.toShort, null),
        Row(4, 15.toShort, null),
        Row(6, 20.toShort, 3.toShort)
      )), new StructType().add("id", IntegerType).add("short_normal", ShortType)
        .add("short_null", ShortType))

    joinShort1Df = spark.createDataFrame(
      sparkContext.parallelize(Seq(
        Row(100, 10.toShort),
        Row(100, null),
        Row(200, 20.toShort),
        Row(300, 8.toShort)
      )), new StructType().add("id", IntegerType).add("short_col", ShortType))

    joinShort2Df = spark.createDataFrame(
      sparkContext.parallelize(Seq(
        Row(100, 30.toShort),
        Row(200, null),
        Row(300, 80.toShort),
        Row(400, null)
      )), new StructType().add("id", IntegerType).add("short_col", ShortType))

    shortDf.createOrReplaceTempView("short_table")
    joinShort1Df.createOrReplaceTempView("join_short1")
    joinShort2Df.createOrReplaceTempView("join_short2")
  }

  test("Test ColumnarProjectExec not happen and result is correct when support short") {
    val res = spark.sql("select short_normal as col from short_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isDefined, s"ProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isEmpty, s"ColumnarProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(10),
        Row(15),
        Row(20))
    )
  }

  test("Test ColumnarProjectExec not happen and result is correct when support short with expr") {
    val res = spark.sql("select short_normal + 2 from short_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isDefined, s"ProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isEmpty, s"ColumnarProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(12),
        Row(17),
        Row(22))
    )
  }

  test("Test ColumnarProjectExec not happen and result is correct when support short with null") {
    val res = spark.sql("select short_null + 1 from short_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isDefined, s"ProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isEmpty, s"ColumnarProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(null),
        Row(null),
        Row(4))
    )
  }

  test("Test ColumnarFilterExec not happen and result is correct when support short") {
    val res = spark.sql("select short_normal from short_table where short_normal > 3")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[FilterExec]).isDefined, s"FilterExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ColumnarFilterExec]).isEmpty, s"ColumnarFilterExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(10),
        Row(15),
        Row(20))
    )
  }

  test("Test ColumnarFilterExec not happen and result is correct when support short with expr") {
    val res = spark.sql("select short_normal from short_table where short_normal + 2 > 3")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[FilterExec]).isDefined, s"FilterExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ColumnarFilterExec]).isEmpty, s"ColumnarFilterExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(10),
        Row(15),
        Row(20))
    )
  }

  test("Test ColumnarFilterExec not happen and result is correct when support short with null") {
    val res = spark.sql("select short_null from short_table where short_normal > 3")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[FilterExec]).isDefined, s"FilterExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ColumnarFilterExec]).isEmpty, s"ColumnarFilterExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(null),
        Row(null),
        Row(3))
    )
  }

  test("Test ColumnarConditionProjectExec not happen and result is correct when support short") {
    val res = spark.sql("select short_normal + 2 from short_table where short_normal > 3")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarConditionProjectExec]).isEmpty, s"ColumnarConditionProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(12),
        Row(17),
        Row(22))
    )
  }

  test("Test ColumnarUnionExec happen and result is correct when support short") {
    val res = spark.sql("select short_null + 2 from short_table where short_normal > 3 " +
      "union select short_normal from short_table where short_normal > 3")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarUnionExec]).isDefined, s"ColumnarUnionExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[UnionExec]).isEmpty, s"UnionExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(5),
        Row(10),
        Row(15),
        Row(20),
        Row(null)
      )
    )
  }

  test("Test ColumnarHashAggregateExec happen and result is correct when support short") {
    val res = spark.sql("select max(short_normal) from short_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[HashAggregateExec]).isEmpty, s"UnionExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(Row(20))
    )
  }

  test("Test HashAggregateExec happen and result is correct when support short with expr") {
    val res = spark.sql("select max(short_normal) + 2 from short_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[HashAggregateExec]).isDefined, s"HashAggregateExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(Row(22))
    )
  }

  test("Test ColumnarHashAggregateExec happen and result is correct when support short with null") {
    val res = spark.sql("select max(short_null) from short_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(Row(3))
    )
  }

  test("Test ColumnarSortExec happen and result is correct when support short") {
    val res = spark.sql("select short_normal from short_table order by short_normal")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarSortExec]).isDefined, s"ColumnarSortExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[SortExec]).isEmpty, s"SortExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(10),
        Row(15),
        Row(20))
    )
  }

  test("Test ColumnarSortExec not happen and result is correct when support short with expr") {
    val res = spark.sql("select short_normal from short_table order by short_normal + 1")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarSortExec]).isEmpty, s"ColumnarSortExec happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[SortExec]).isDefined, s"SortExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(10),
        Row(15),
        Row(20))
    )
  }

  test("Test ColumnarSortExec happen and result is correct when support short with null") {
    val res = spark.sql("select short_null from short_table order by short_null")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarSortExec]).isDefined, s"ColumnarSortExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[SortExec]).isEmpty, s"SortExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(null),
        Row(null),
        Row(3))
    )
  }

  // window
  test("Test ColumnarWindowExec happen and result is correct when support short") {
    val res = spark.sql("select id, short_normal, RANK() OVER (PARTITION BY short_normal ORDER BY id) AS rank from short_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarWindowExec]).isDefined, s"ColumnarWindowExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[WindowExec]).isEmpty, s"WindowExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(2, 10, 1),
        Row(4, 15, 1),
        Row(6, 20, 1))
    )
  }

  test("Test ColumnarWindowExec not happen and result is correct when support short with expr") {
    val res = spark.sql("select id + 1, short_normal, sum(short_normal) OVER (PARTITION BY short_normal ORDER BY id) AS rank from short_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarWindowExec]).isEmpty, s"ColumnarWindowExec happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[WindowExec]).isDefined, s"WindowExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(3, 10, 10),
        Row(5, 15, 15),
        Row(7, 20, 20))
    )
  }

  test("Test ColumnarWindowExec happen and result is correct when support short with null") {
    val res = spark.sql("select id, short_null, RANK() OVER (PARTITION BY short_null ORDER BY id) AS rank from short_table")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarWindowExec]).isDefined, s"ColumnarWindowExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[WindowExec]).isEmpty, s"WindowExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(2, null, 1),
        Row(4, null, 2),
        Row(6, 3, 1))
    )
  }

  test("Test ColumnarTakeOrderedAndProjectExec happen and result is correct when support short") {
    val res = spark.sql("select short_normal from short_table order by short_normal limit 2")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarTakeOrderedAndProjectExec]).isDefined, s"ColumnarTakeOrderedAndProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[TakeOrderedAndProjectExec]).isEmpty, s"TakeOrderedAndProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(10),
        Row(15))
    )
  }

  test("Test ColumnarTakeOrderedAndProjectExec not happen and result is correct when support short with expr") {
    val res = spark.sql("select short_normal from short_table order by short_normal + 1 limit 2")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarTakeOrderedAndProjectExec]).isEmpty, s"ColumnarTakeOrderedAndProjectExec happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[TakeOrderedAndProjectExec]).isDefined, s"TakeOrderedAndProjectExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(10),
        Row(15))
    )
  }

  test("Test ColumnarTakeOrderedAndProjectExec happen and result is correct when support short with null") {
    val res = spark.sql("select short_null from short_table order by short_null limit 2")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarTakeOrderedAndProjectExec]).isDefined, s"ColumnarTakeOrderedAndProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[TakeOrderedAndProjectExec]).isEmpty, s"TakeOrderedAndProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(null),
        Row(null))
    )
  }

  test("Test ColumnarShuffleExchangeExec happen and result is correct when support short with group by no-short") {
    val res = spark.sql("select id, sum(short_null) from short_table group by id")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarShuffleExchangeExec]).isDefined, s"ColumnarShuffleExchangeExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(2, null),
        Row(4, null),
        Row(6, 3))
    )
  }

  test("Test ColumnarShuffleExchangeExec not happen and result is correct when support short with group by short") {
    val res = spark.sql("select short_null, sum(short_null) from short_table group by short_null")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarShuffleExchangeExec]).isEmpty, s"ColumnarShuffleExchangeExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(null, null),
        Row(3, 3))
    )
  }

  test("Test ColumnarExpandExec not happen and result is correct when support short") {
    val res = spark.sql("select id, short_null, sum(short_normal) as sum from short_table group by " +
      "grouping sets((id, short_null)) order by id, short_null")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarExpandExec]).isEmpty, s"ColumnarExpandExec happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ExpandExec]).isDefined, s"ExpandExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(2, null, 10),
        Row(4, null, 15),
        Row(6, 3, 20))
    )
  }

  test("Test ColumnarSortMergeJoinExec happen and result is correct when support short") {
    val res = spark.sql("select /*+ MERGEJOIN(t2) */  t1.*, t2.* from join_short1 t1, join_short2 t2 where t1.id = t2.id")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarSortMergeJoinExec]).isDefined, s"ColumnarSortMergeJoinExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(100, null, 100, 30),
        Row(100, 10, 100, 30),
        Row(200, 20, 200, null),
        Row(300, 8, 300, 80))
    )
  }

  test("Test ColumnarSortMergeJoinExec not happen and result is correct when support short with expr") {
    val res = spark.sql("select /*+ MERGEJOIN(t2) */  t1.*, t2.* from join_short1 t1, join_short2 t2 where t1.short_col < t2.short_col")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarSortMergeJoinExec]).isEmpty, s"ColumnarSortMergeJoinExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(100, 10, 100, 30),
        Row(100, 10, 300, 80),
        Row(200, 20, 100, 30),
        Row(200, 20, 300, 80),
        Row(300, 8, 100, 30),
        Row(300, 8, 300, 80))
    )
  }

  test("Test ColumnarShuffledHashJoinExec happen and result is correct when support short") {
    val res = spark.sql("select /*+ SHUFFLE_HASH(t2) */ t1.*, t2.* from join_short1 t1, join_short2 t2 where t1.id = t2.id")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarShuffledHashJoinExec]).isDefined, s"ColumnarShuffledHashJoinExec not happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(100, null, 100, 30),
        Row(100, 10, 100, 30),
        Row(200, 20, 200, null),
        Row(300, 8, 300, 80))
    )
  }

  test("Test ColumnarShuffledHashJoinExec not happen and result is correct when support short with expr") {
    val res = spark.sql("select /*+ SHUFFLE_HASH(t2) */ t1.*, t2.* from join_short1 t1, join_short2 t2 where t1.short_col < t2.short_col")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarShuffledHashJoinExec]).isEmpty, s"ColumnarShuffledHashJoinExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(100, 10, 100, 30),
        Row(100, 10, 300, 80),
        Row(200, 20, 100, 30),
        Row(200, 20, 300, 80),
        Row(300, 8, 100, 30),
        Row(300, 8, 300, 80))
    )
  }

  test("Test ColumnarBroadcastHashJoinExec happen and result is correct when support short") {
    val res = spark.sql("select /*+ BROADCAST(t2) */  t1.*, t2.* from join_short1 t1, join_short2 t2 where t1.id = t2.id")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarBroadcastHashJoinExec]).isDefined, s"ColumnarBroadcastHashJoinExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(100, null, 100, 30),
        Row(100, 10, 100, 30),
        Row(200, 20, 200, null),
        Row(300, 8, 300, 80))
    )
  }

  test("Test ColumnarBroadcastHashJoinExec not happen and result is correct when support short with expr") {
    val res = spark.sql("select /*+ BROADCAST(t2) */ t1.*, t2.* from join_short1 t1, join_short2 t2 where t1.short_col < t2.short_col")
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarBroadcastHashJoinExec]).isEmpty, s"ColumnarBroadcastHashJoinExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(100, 10, 100, 30),
        Row(100, 10, 300, 80),
        Row(200, 20, 100, 30),
        Row(200, 20, 300, 80),
        Row(300, 8, 100, 30),
        Row(300, 8, 300, 80))
    )
  }
}