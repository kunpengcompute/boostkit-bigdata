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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ColumnarBroadcastHashJoinExec, ColumnarShuffledHashJoinExec, ColumnarSortMergeJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

// refer to joins package
class ColumnarJoinExecSuite extends ColumnarSparkPlanTest {

  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  private var left: DataFrame = _
  private var right: DataFrame = _
  private var leftWithNull: DataFrame = _
  private var rightWithNull: DataFrame = _
  private var person_test: DataFrame = _
  private var order_test: DataFrame = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    left = Seq[(String, String, java.lang.Integer, java.lang.Double)](
      ("abc", "", 4, 2.0),
      ("", "Hello", 1, 1.0),
      (" add", "World", 8, 3.0),
      (" yeah  ", "yeah", 10, 8.0)
    ).toDF("a", "b", "q", "d")

    right = Seq[(String, String, java.lang.Integer, java.lang.Double)](
      ("abc", "", 4, 1.0),
      ("", "Hello", 2, 2.0),
      (" add", "World", 1, 3.0),
      (" yeah  ", "yeah", 0, 4.0)
    ).toDF("a", "b", "c", "d")

    leftWithNull = Seq[(String, String, java.lang.Integer, java.lang.Double)](
      ("abc", null, 4, 2.0),
      ("", "Hello", null, 1.0),
      (" add", "World", 8, 3.0),
      (" yeah  ", "yeah", 10, 8.0)
    ).toDF("a", "b", "q", "d")

    rightWithNull = Seq[(String, String, java.lang.Integer, java.lang.Double)](
      ("abc", "", 4, 1.0),
      ("", "Hello", 2, 2.0),
      (" add", null, 1, null),
      (" yeah  ", null, null, 4.0)
    ).toDF("a", "b", "c", "d")

    person_test = spark.createDataFrame(
      sparkContext.parallelize(Seq(
        Row(3, "Carter"),
        Row(1, "Adams"),
        Row(2, "Bush")
      )), new StructType()
        .add("id_p", IntegerType)
        .add("name", StringType))
    person_test.createOrReplaceTempView("person_test")

    order_test = spark.createDataFrame(
      sparkContext.parallelize(Seq(
        Row(5, 34764, 65),
        Row(1, 77895, 3),
        Row(2, 44678, 3),
        Row(4, 24562, 1),
        Row(3, 22456, 1)
      )), new StructType()
        .add("id_o", IntegerType)
        .add("order_no", IntegerType)
        .add("id_p", IntegerType))
    order_test.createOrReplaceTempView("order_test")
  }

  test("validate columnar broadcastHashJoin exec happened") {
    val res = left.join(right.hint("broadcast"), col("q") === col("c"))
    assert(
      res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarBroadcastHashJoinExec]).isDefined,
      s"ColumnarBroadcastHashJoinExec not happened, " +
        s"executedPlan as follows： \n${res.queryExecution.executedPlan}")
  }

  test("validate columnar sortMergeJoin exec happened") {
    val res = left.join(right.hint("mergejoin"), col("q") === col("c"))
    assert(
      res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarSortMergeJoinExec]).isDefined,
      s"ColumnarSortMergeJoinExec not happened, " +
        s"executedPlan as follows： \n${res.queryExecution.executedPlan}")
  }

  test("columnar broadcastHashJoin is equal to native") {
    val df = left.join(right.hint("broadcast"), col("q") === col("c"))
    val leftKeys = Seq(left.col("q").expr)
    val rightKeys = Seq(right.col("c").expr)
    checkThatPlansAgreeTemplateForBHJ(df, leftKeys, rightKeys)
  }

  test("columnar sortMergeJoin Inner Join is equal to native") {
    val df = left.join(right.hint("mergejoin"), col("q") === col("c"))
    val leftKeys = Seq(left.col("q").expr)
    val rightKeys = Seq(right.col("c").expr)
    checkThatPlansAgreeTemplateForSMJ(df, leftKeys, rightKeys, Inner)
  }

  test("columnar sortMergeJoin Inner Join is equal to native With NULL") {
    val df = leftWithNull.join(rightWithNull.hint("mergejoin"), col("q") === col("c"))
    val leftKeys = Seq(leftWithNull.col("q").expr)
    val rightKeys = Seq(rightWithNull.col("c").expr)
    checkThatPlansAgreeTemplateForSMJ(df, leftKeys, rightKeys, Inner)
  }

  test("columnar sortMergeJoin LeftOuter Join is equal to native") {
    val df = left.join(right.hint("mergejoin"), col("q") === col("c"))
    val leftKeys = Seq(left.col("q").expr)
    val rightKeys = Seq(right.col("c").expr)
    checkThatPlansAgreeTemplateForSMJ(df, leftKeys, rightKeys, LeftOuter)
  }

  test("columnar sortMergeJoin LeftOuter Join is equal to native With NULL") {
    val df = leftWithNull.join(rightWithNull.hint("mergejoin"), col("q") === col("c"))
    val leftKeys = Seq(leftWithNull.col("q").expr)
    val rightKeys = Seq(rightWithNull.col("c").expr)
    checkThatPlansAgreeTemplateForSMJ(df, leftKeys, rightKeys, LeftOuter)
  }

  test("columnar sortMergeJoin FullOuter Join is equal to native") {
    val df = left.join(right.hint("mergejoin"), col("q") === col("c"))
    val leftKeys = Seq(left.col("q").expr)
    val rightKeys = Seq(right.col("c").expr)
    checkThatPlansAgreeTemplateForSMJ(df, leftKeys, rightKeys, FullOuter)
  }

  test("columnar sortMergeJoin FullOuter Join is equal to native With NULL") {
    val df = leftWithNull.join(rightWithNull.hint("mergejoin"), col("q") === col("c"))
    val leftKeys = Seq(leftWithNull.col("q").expr)
    val rightKeys = Seq(rightWithNull.col("c").expr)
    checkThatPlansAgreeTemplateForSMJ(df, leftKeys, rightKeys, FullOuter)
  }

  test("columnar sortMergeJoin LeftSemi Join is equal to native") {
    val df = left.join(right.hint("mergejoin"), col("q") === col("c"))
    val leftKeys = Seq(left.col("q").expr)
    val rightKeys = Seq(right.col("c").expr)
    checkThatPlansAgreeTemplateForSMJ(df, leftKeys, rightKeys, LeftSemi)
  }

  test("columnar sortMergeJoin LeftSemi Join is equal to native With NULL") {
    val df = leftWithNull.join(rightWithNull.hint("mergejoin"), col("q") === col("c"))
    val leftKeys = Seq(leftWithNull.col("q").expr)
    val rightKeys = Seq(rightWithNull.col("c").expr)
    checkThatPlansAgreeTemplateForSMJ(df, leftKeys, rightKeys, LeftSemi)
  }

  test("columnar sortMergeJoin LeftAnti Join is equal to native") {
    val df = left.join(right.hint("mergejoin"), col("q") === col("c"))
    val leftKeys = Seq(left.col("q").expr)
    val rightKeys = Seq(right.col("c").expr)
    checkThatPlansAgreeTemplateForSMJ(df, leftKeys, rightKeys, LeftAnti)
  }

  test("columnar sortMergeJoin LeftAnti Join is equal to native With NULL") {
    val df = leftWithNull.join(rightWithNull.hint("mergejoin"), col("q") === col("c"))
    val leftKeys = Seq(leftWithNull.col("q").expr)
    val rightKeys = Seq(rightWithNull.col("c").expr)
    checkThatPlansAgreeTemplateForSMJ(df, leftKeys, rightKeys, LeftAnti)
  }

  test("columnar broadcastHashJoin is equal to native with null") {
    val df = leftWithNull.join(rightWithNull.hint("broadcast"),
      col("q").isNotNull === col("c").isNotNull)
    val leftKeys = Seq(leftWithNull.col("q").isNotNull.expr)
    val rightKeys = Seq(rightWithNull.col("c").isNotNull.expr)
    checkThatPlansAgreeTemplateForBHJ(df, leftKeys, rightKeys)
  }

  test("validate columnar broadcastHashJoin left semi join happened") {
    val res = left.join(right.hint("broadcast"), col("q") === col("c"), "leftsemi")
    assert(
      res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarBroadcastHashJoinExec]).isDefined,
      s"ColumnarBroadcastHashJoinExec not happened," +
        s" executedPlan as follows: \n${res.queryExecution.executedPlan}")
  }

  test("columnar broadcastHashJoin LeftSemi Join is equal to native") {
    val df = left.join(right.hint("broadcast"), col("q") === col("c"))
    val leftKeys = Seq(left.col("q").expr)
    val rightKeys = Seq(right.col("c").expr)
    checkThatPlansAgreeTemplateForBHJ(df, leftKeys, rightKeys, LeftSemi)
  }

  test("columnar broadcastHashJoin LeftSemi Join is equal to native with null") {
    val df = leftWithNull.join(rightWithNull.hint("broadcast"),
      col("q").isNotNull === col("c").isNotNull)
    val leftKeys = Seq(leftWithNull.col("q").isNotNull.expr)
    val rightKeys = Seq(rightWithNull.col("c").isNotNull.expr)
    checkThatPlansAgreeTemplateForBHJ(df, leftKeys, rightKeys, LeftSemi)
  }

  def checkThatPlansAgreeTemplateForBHJ(df: DataFrame, leftKeys: Seq[Expression],
                                        rightKeys: Seq[Expression], joinType: JoinType = Inner): Unit = {
    checkThatPlansAgree(
      df,
      (child: SparkPlan) =>
        ColumnarBroadcastHashJoinExec(leftKeys, rightKeys, joinType,
          BuildRight, None, child, child),
      (child: SparkPlan) =>
        BroadcastHashJoinExec(leftKeys, rightKeys, joinType,
          BuildRight, None, child, child),
      sortAnswers = false)
  }

  test("validate columnar broadcastHashJoin left outer join happened") {
    val res = left.join(right.hint("broadcast"), col("q") === col("c"), "leftouter")
    assert(
      res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarBroadcastHashJoinExec]).isDefined,
      s"ColumnarBroadcastHashJoinExec not happened," +
        s" executedPlan as follows: \n${res.queryExecution.executedPlan}")
  }

  test("columnar broadcastHashJoin left outer join is equal to native") {
    val df = left.join(right.hint("broadcast"), col("q") === col("c"), "leftouter")
    checkAnswer(df, _ => df.queryExecution.executedPlan, Seq(
      Row("abc", "", 4, 2.0, "abc", "", 4, 1.0),
      Row("", "Hello", 1, 1.0, " add", "World", 1, 3.0),
      Row(" add", "World", 8, 3.0, null, null, null, null),
      Row(" yeah  ", "yeah", 10, 8.0, null, null, null, null)
    ), false)
  }

  test("columnar broadcastHashJoin left outer join is equal to native with null") {
    val df = leftWithNull.join(rightWithNull.hint("broadcast"),
      col("q").isNotNull === col("c").isNotNull, "leftouter")
    checkAnswer(df, _ => df.queryExecution.executedPlan, Seq(
      Row("abc", null, 4, 2.0, " add", null, 1, null),
      Row("abc", null, 4, 2.0, "", "Hello", 2, 2.0),
      Row("abc", null, 4, 2.0, "abc", "", 4, 1.0),
      Row("", "Hello", null, 1.0, " yeah  ", null, null, 4.0),
      Row(" add", "World", 8, 3.0, " add", null, 1, null),
      Row(" add", "World", 8, 3.0, "", "Hello", 2, 2.0),
      Row(" add", "World", 8, 3.0, "abc", "", 4, 1.0),
      Row(" yeah  ", "yeah", 10, 8.0, " add", null, 1, null),
      Row(" yeah  ", "yeah", 10, 8.0, "", "Hello", 2, 2.0),
      Row(" yeah  ", "yeah", 10, 8.0, "abc", "", 4, 1.0)
    ), false)
  }

  test("validate columnar shuffledHashJoin full outer join happened") {
    val res = left.join(right.hint("SHUFFLE_HASH"), col("q") === col("c"), "fullouter")
    assert(
      res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarShuffledHashJoinExec]).isDefined,
      s"ColumnarShuffledHashJoinExec not happened," +
        s" executedPlan as follows: \n${res.queryExecution.executedPlan}")
  }

  test("columnar shuffledHashJoin full outer join is equal to native") {
    val df = left.join(right.hint("SHUFFLE_HASH"), col("q") === col("c"), "fullouter")
    checkAnswer(df, _ => df.queryExecution.executedPlan, Seq(
      Row(null, null, null, null, " yeah  ", "yeah", 0, 4.0),
      Row("abc", "", 4, 2.0, "abc", "", 4, 1.0),
      Row(" yeah  ", "yeah", 10, 8.0, null, null, null, null),
      Row("", "Hello", 1, 1.0, " add", "World", 1, 3.0),
      Row(" add", "World", 8, 3.0, null, null, null, null),
      Row(null, null, null, null, "", "Hello", 2, 2.0)
    ), false)
  }

  test("columnar shuffledHashJoin full outer join is equal to native with null") {
    val df = leftWithNull.join(rightWithNull.hint("SHUFFLE_HASH"),
      col("q").isNotNull === col("c").isNotNull, "fullouter")
    checkAnswer(df, _ => df.queryExecution.executedPlan, Seq(
      Row("", "Hello", null, 1.0, " yeah  ", null, null, 4.0),
      Row("abc", null, 4, 2.0, " add", null, 1, null),
      Row("abc", null, 4, 2.0, "", "Hello", 2, 2.0),
      Row("abc", null, 4, 2.0, "abc", "", 4, 1.0),
      Row(" add", "World", 8, 3.0, " add", null, 1, null),
      Row(" add", "World", 8, 3.0, "", "Hello", 2, 2.0),
      Row(" add", "World", 8, 3.0, "abc", "", 4, 1.0),
      Row(" yeah  ", "yeah", 10, 8.0, " add", null, 1, null),
      Row(" yeah  ", "yeah", 10, 8.0, "", "Hello", 2, 2.0),
      Row(" yeah  ", "yeah", 10, 8.0, "abc", "", 4, 1.0)
    ), false)
  }

  test("validate columnar shuffledHashJoin left semi join happened") {
    val res = left.join(right.hint("SHUFFLE_HASH"), col("q") === col("c"), "leftsemi")
    assert(
      res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarShuffledHashJoinExec]).isDefined,
      s"ColumnarShuffledHashJoinExec not happened," +
        s" executedPlan as follows: \n${res.queryExecution.executedPlan}")
  }

  test("columnar shuffledHashJoin left semi join is equal to native") {
    val df = left.join(right.hint("SHUFFLE_HASH"), col("q") === col("c"), "leftsemi")
    checkAnswer(df, _ => df.queryExecution.executedPlan, Seq(
      Row("abc", "", 4, 2.0),
      Row("", "Hello", 1, 1.0)
    ), false)
  }

  test("columnar shuffledHashJoin left semi join is equal to native with null") {
    val df = leftWithNull.join(rightWithNull.hint("SHUFFLE_HASH"),
      col("q") === col("c"), "leftsemi")
    checkAnswer(df, _ => df.queryExecution.executedPlan, Seq(
      Row("abc", null, 4, 2.0)
    ), false)
  }

  test("validate columnar shuffledHashJoin left outer join happened") {
    val res = left.join(right.hint("SHUFFLE_HASH"), col("q") === col("c"), "leftouter")
    assert(
      res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarShuffledHashJoinExec]).isDefined,
      s"ColumnarShuffledHashJoinExec not happened," +
        s" executedPlan as follows: \n${res.queryExecution.executedPlan}")
  }

  test("columnar shuffledHashJoin left outer join is equal to native") {
    val df = left.join(right.hint("SHUFFLE_HASH"), col("q") === col("c"), "leftouter")
    checkAnswer(df, _ => df.queryExecution.executedPlan, Seq(
      Row("abc", "", 4, 2.0, "abc", "", 4, 1.0),
      Row(" yeah  ", "yeah", 10, 8.0, null, null, null, null),
      Row("", "Hello", 1, 1.0, " add", "World", 1, 3.0),
      Row(" add", "World", 8, 3.0, null, null, null, null)
    ), false)
  }

  test("columnar shuffledHashJoin left outer join is equal to native with null") {
    val df = leftWithNull.join(rightWithNull.hint("SHUFFLE_HASH"),
      col("q") === col("c"), "leftouter")
    checkAnswer(df, _ => df.queryExecution.executedPlan, Seq(
      Row("abc", null, 4, 2.0, "abc", "", 4, 1.0),
      Row("", "Hello", null, 1.0, null, null, null, null),
      Row(" yeah  ", "yeah", 10, 8.0, null, null, null, null),
      Row(" add", "World", 8, 3.0, null, null, null, null)
    ), false)
  }

  test("ColumnarBroadcastHashJoin is not rolled back with not_equal filter expr") {
    val res = left.join(right.hint("broadcast"), left("a") <=> right("a"))
    assert(
      res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarBroadcastHashJoinExec]).isDefined,
      s"ColumnarBroadcastHashJoinExec not happened, " +
        s"executedPlan as follows： \n${res.queryExecution.executedPlan}")
  }

  def checkThatPlansAgreeTemplateForSMJ(df: DataFrame, leftKeys: Seq[Expression],
                                        rightKeys: Seq[Expression], joinType: JoinType): Unit = {
    checkThatPlansAgree(
      df,
      (child: SparkPlan) =>
        new ColumnarSortMergeJoinExec(leftKeys, rightKeys, joinType,
          None, child, child),
      (child: SparkPlan) =>
        SortMergeJoinExec(leftKeys, rightKeys, joinType,
          None, child, child),
      sortAnswers = true)
  }

  test("BroadcastHashJoin and project funsion test") {
    val omniResult = person_test.join(order_test.hint("broadcast"), person_test("id_p") === order_test("id_p"), "leftouter")
      .select(person_test("name"), order_test("order_no"))
    val omniPlan = omniResult.queryExecution.executedPlan
    assert(omniPlan.find(_.isInstanceOf[ColumnarProjectExec]).isEmpty,
      s"SQL:\n@OmniEnv no ColumnarProjectExec,omniPlan:${omniPlan}")
    checkAnswer(omniResult, _ => omniPlan, Seq(
      Row("Carter", 44678),
      Row("Carter", 77895),
      Row("Adams", 22456),
      Row("Adams", 24562),
      Row("Bush", null)
    ), false)
  }

  test("BroadcastHashJoin and project funsion test for duplicate column") {
    val omniResult = person_test.join(order_test.hint("broadcast"), person_test("id_p") === order_test("id_p"), "leftouter")
      .select(person_test("name"), order_test("order_no"), order_test("id_p"))
    val omniPlan = omniResult.queryExecution.executedPlan
    assert(omniPlan.find(_.isInstanceOf[ColumnarProjectExec]).isEmpty,
      s"SQL:\n@OmniEnv no ColumnarProjectExec,omniPlan:${omniPlan}")
    checkAnswer(omniResult, _ => omniPlan, Seq(
      Row("Carter", 44678, 3),
      Row("Carter", 77895, 3),
      Row("Adams", 22456, 1),
      Row("Adams", 24562, 1),
      Row("Bush", null, null)
    ), false)
  }

  test("BroadcastHashJoin and project funsion test for reorder columns") {
    val omniResult = person_test.join(order_test.hint("broadcast"), person_test("id_p") === order_test("id_p"), "leftouter")
      .select(order_test("order_no"), person_test("name"), order_test("id_p"))
    val omniPlan = omniResult.queryExecution.executedPlan
    assert(omniPlan.find(_.isInstanceOf[ColumnarProjectExec]).isEmpty,
      s"SQL:\n@OmniEnv no ColumnarProjectExec,omniPlan:${omniPlan}")
    checkAnswer(omniResult, _ => omniPlan, Seq(
      Row(44678, "Carter", 3),
      Row(77895, "Carter", 3),
      Row(22456, "Adams", 1),
      Row(24562, "Adams", 1),
      Row(null, "Bush", null)
    ), false)
  }

  test("BroadcastHashJoin and project are not funsed test") {
    val omniResult = person_test.join(order_test.hint("broadcast"), person_test("id_p") === order_test("id_p"), "leftouter")
      .select(order_test("order_no").plus(1), person_test("name"))
    val omniPlan = omniResult.queryExecution.executedPlan
    assert(omniPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined,
      s"SQL:\n@OmniEnv have ColumnarProjectExec,omniPlan:${omniPlan}")
    checkAnswer(omniResult, _ => omniPlan, Seq(
      Row(44679, "Carter"),
      Row(77896, "Carter"),
      Row(22457, "Adams"),
      Row(24563, "Adams"),
      Row(null, "Bush")
    ), false)
  }

  test("BroadcastHashJoin and project funsion test for alias") {
    val omniResult = person_test.join(order_test.hint("broadcast"), person_test("id_p") === order_test("id_p"), "leftouter")
      .select(person_test("name").as("name1"), order_test("order_no").as("order_no1"))
    val omniPlan = omniResult.queryExecution.executedPlan
    assert(omniPlan.find(_.isInstanceOf[ColumnarProjectExec]).isEmpty,
      s"SQL:\n@OmniEnv no ColumnarProjectExec,omniPlan:${omniPlan}")
    checkAnswer(omniResult, _ => omniPlan, Seq(
      Row("Carter", 44678),
      Row("Carter", 77895),
      Row("Adams", 22456),
      Row("Adams", 24562),
      Row("Bush", null)
    ), false)
  }

  test("validate columnar shuffledHashJoin left anti join happened") {
    val res = left.join(right.hint("SHUFFLE_HASH"), col("q") === col("c"), "leftanti")
    assert(
      res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarShuffledHashJoinExec]).isDefined,
      s"ColumnarShuffledHashJoinExec not happened," +
        s" executedPlan as follows: \n${res.queryExecution.executedPlan}")
  }

  test("columnar shuffledHashJoin left anti join is equal to native") {
    val df = left.join(right.hint("SHUFFLE_HASH"), col("q") === col("c"), "leftanti")
    checkAnswer(df, _ => df.queryExecution.executedPlan, Seq(
      Row(" yeah  ", "yeah", 10, 8.0),
      Row(" add", "World", 8, 3.0)
    ), false)
  }

  test("columnar shuffledHashJoin left anti join is equal to native with null") {
    val df = leftWithNull.join(rightWithNull.hint("SHUFFLE_HASH"),
      col("q") === col("c"), "leftanti")
    checkAnswer(df, _ => df.queryExecution.executedPlan, Seq(
      Row("", "Hello", null, 1.0),
      Row(" yeah  ", "yeah", 10, 8.0),
      Row(" add", "World", 8, 3.0)
    ), false)
  }

  test("shuffledHashJoin and project funsion test") {
    val omniResult = person_test.join(order_test.hint("SHUFFLE_HASH"), person_test("id_p") === order_test("id_p"), "inner")
      .select(person_test("name"), order_test("order_no"))
    val omniPlan = omniResult.queryExecution.executedPlan
    assert(omniPlan.find(_.isInstanceOf[ColumnarProjectExec]).isEmpty,
      s"SQL:\n@OmniEnv no ColumnarProjectExec,omniPlan:${omniPlan}")
    checkAnswer(omniResult, _ => omniPlan, Seq(
      Row("Carter", 44678),
      Row("Carter", 77895),
      Row("Adams", 22456),
      Row("Adams", 24562)
    ), false)
  }

  test("ShuffledHashJoin and project funsion test for duplicate column") {
    val omniResult = person_test.join(order_test.hint("SHUFFLE_HASH"), person_test("id_p") === order_test("id_p"), "inner")
      .select(person_test("name"), order_test("order_no"), order_test("id_p"))
    val omniPlan = omniResult.queryExecution.executedPlan
    assert(omniPlan.find(_.isInstanceOf[ColumnarProjectExec]).isEmpty,
      s"SQL:\n@OmniEnv no ColumnarProjectExec,omniPlan:${omniPlan}")
    checkAnswer(omniResult, _ => omniPlan, Seq(
      Row("Carter", 44678, 3),
      Row("Carter", 77895, 3),
      Row("Adams", 22456, 1),
      Row("Adams", 24562, 1)
    ), false)
  }

  test("ShuffledHashJoin and project funsion test for reorder columns") {
    val omniResult = person_test.join(order_test.hint("SHUFFLE_HASH"), person_test("id_p") === order_test("id_p"), "inner")
      .select(order_test("order_no"), person_test("name"), order_test("id_p"))
    val omniPlan = omniResult.queryExecution.executedPlan
    assert(omniPlan.find(_.isInstanceOf[ColumnarProjectExec]).isEmpty,
      s"SQL:\n@OmniEnv no ColumnarProjectExec,omniPlan:${omniPlan}")
    checkAnswer(omniResult, _ => omniPlan, Seq(
      Row(44678, "Carter", 3),
      Row(77895, "Carter", 3),
      Row(22456, "Adams", 1),
      Row(24562, "Adams", 1)
    ), false)
  }

  test("ShuffledHashJoin and project are not funsed test") {
    val omniResult = person_test.join(order_test.hint("SHUFFLE_HASH"), person_test("id_p") === order_test("id_p"), "inner")
      .select(order_test("order_no").plus(1), person_test("name"))
    val omniPlan = omniResult.queryExecution.executedPlan
    assert(omniPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined,
      s"SQL:\n@OmniEnv have ColumnarProjectExec,omniPlan:${omniPlan}")
    checkAnswer(omniResult, _ => omniPlan, Seq(
      Row(44679, "Carter"),
      Row(77896, "Carter"),
      Row(22457, "Adams"),
      Row(24563, "Adams")
    ), false)
  }

  test("ShuffledHashJoin and project funsion test for alias") {
    val omniResult = person_test.join(order_test.hint("SHUFFLE_HASH"), person_test("id_p") === order_test("id_p"), "inner")
      .select(person_test("name").as("name1"), order_test("order_no").as("order_no1"))
    val omniPlan = omniResult.queryExecution.executedPlan
    assert(omniPlan.find(_.isInstanceOf[ColumnarProjectExec]).isEmpty,
      s"SQL:\n@OmniEnv no ColumnarProjectExec,omniPlan:${omniPlan}")
    checkAnswer(omniResult, _ => omniPlan, Seq(
      Row("Carter", 44678),
      Row("Carter", 77895),
      Row("Adams", 22456),
      Row("Adams", 24562)
    ), false)
  }

  test("SortMergeJoin and project funsion test") {
    val omniResult = person_test.join(order_test.hint("MERGEJOIN"), person_test("id_p") === order_test("id_p"), "inner")
      .select(person_test("name"), order_test("order_no"))
    val omniPlan = omniResult.queryExecution.executedPlan
    assert(omniPlan.find(_.isInstanceOf[ColumnarProjectExec]).isEmpty,
      s"SQL:\n@OmniEnv no ColumnarProjectExec,omniPlan:${omniPlan}")
    checkAnswer(omniResult, _ => omniPlan, Seq(
      Row("Carter", 77895),
      Row("Carter", 44678),
      Row("Adams", 24562),
      Row("Adams", 22456)
    ), false)
  }

  test("SortMergeJoin and project funsion test for duplicate column") {
    val omniResult = person_test.join(order_test.hint("MERGEJOIN"), person_test("id_p") === order_test("id_p"), "inner")
      .select(person_test("name"), order_test("order_no"), order_test("id_p"))
    val omniPlan = omniResult.queryExecution.executedPlan
    assert(omniPlan.find(_.isInstanceOf[ColumnarProjectExec]).isEmpty,
      s"SQL:\n@OmniEnv no ColumnarProjectExec,omniPlan:${omniPlan}")
    checkAnswer(omniResult, _ => omniPlan, Seq(
      Row("Carter", 77895, 3),
      Row("Carter", 44678, 3),
      Row("Adams", 24562, 1),
      Row("Adams", 22456, 1)
    ), false)
  }

  test("SortMergeJoin and project funsion test for reorder columns") {
    val omniResult = person_test.join(order_test.hint("MERGEJOIN"), person_test("id_p") === order_test("id_p"), "inner")
      .select(order_test("order_no"), person_test("name"), order_test("id_p"))
    val omniPlan = omniResult.queryExecution.executedPlan
    assert(omniPlan.find(_.isInstanceOf[ColumnarProjectExec]).isEmpty,
      s"SQL:\n@OmniEnv no ColumnarProjectExec,omniPlan:${omniPlan}")
    checkAnswer(omniResult, _ => omniPlan, Seq(
      Row(77895, "Carter", 3),
      Row(44678, "Carter", 3),
      Row(24562, "Adams", 1),
      Row(22456, "Adams", 1)
    ), false)
  }

  test("SortMergeJoin and project are not funsed test") {
    val omniResult = person_test.join(order_test.hint("MERGEJOIN"), person_test("id_p") === order_test("id_p"), "inner")
      .select(order_test("order_no").plus(1), person_test("name"))
    val omniPlan = omniResult.queryExecution.executedPlan
    assert(omniPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined,
      s"SQL:\n@OmniEnv have ColumnarProjectExec,omniPlan:${omniPlan}")
    checkAnswer(omniResult, _ => omniPlan, Seq(
      Row(77896, "Carter"),
      Row(44679, "Carter"),
      Row(24563, "Adams"),
      Row(22457, "Adams")
    ), false)
  }

  test("SortMergeJoin and project funsion test for alias") {
    val omniResult = person_test.join(order_test.hint("MERGEJOIN"), person_test("id_p") === order_test("id_p"), "inner")
      .select(person_test("name").as("name1"), order_test("order_no").as("order_no1"))
    val omniPlan = omniResult.queryExecution.executedPlan
    assert(omniPlan.find(_.isInstanceOf[ColumnarProjectExec]).isEmpty,
      s"SQL:\n@OmniEnv no ColumnarProjectExec,omniPlan:${omniPlan}")
    checkAnswer(omniResult, _ => omniPlan, Seq(
      Row("Carter", 77895),
      Row("Carter", 44678),
      Row("Adams", 24562),
      Row("Adams", 22456)
    ), false)
  }
}