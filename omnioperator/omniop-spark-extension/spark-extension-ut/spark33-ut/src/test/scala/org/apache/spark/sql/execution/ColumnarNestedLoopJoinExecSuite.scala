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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, QueryTest, Row}

// refer to joins package
class ColumnarNestedLoopJoinExecSuite extends ColumnarSparkPlanTest {

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
  }
  test("columnar nestedLoopJoin Inner Join is equal to native") {
    val df = left.join(right, col("q") < col("c"))
    assert(
      df.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarBroadcastNestedLoopJoinExec]).isDefined,
      s"ColumnarBroadcastNestedLoopJoinExec not happened, " +
        s"executedPlan as follows： \n${df.queryExecution.executedPlan}")
    checkAnswer(df, Seq(
      Row("", "Hello", 1, 1.0, "abc", "", 4, 1.0),
      Row("", "Hello", 1, 1.0, "", "Hello", 2, 2.0)
    ))
  }

  test("columnar nestedLoopJoin Inner Join is equal to native With NULL") {
    val df = leftWithNull.join(rightWithNull, col("q") < col("c"))
    assert(
      df.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarBroadcastNestedLoopJoinExec]).isDefined,
      s"ColumnarBroadcastNestedLoopJoinExec not happened, " +
        s"executedPlan as follows： \n${df.queryExecution.executedPlan}")
    checkAnswer(df, Seq())
  }

  test("columnar nestedLoopJoin LeftOuter Join is equal to native") {
    val df = left.join(right, col("q") < col("c"),"leftouter")
    assert(
      df.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarBroadcastNestedLoopJoinExec]).isDefined,
      s"ColumnarBroadcastNestedLoopJoinExec not happened, " +
        s"executedPlan as follows： \n${df.queryExecution.executedPlan}")
    checkAnswer(df, Seq(
      Row("abc", "", 4, 2.0, null, null, null, null),
      Row(" yeah  ", "yeah", 10, 8.0, null, null, null, null),
      Row("", "Hello", 1, 1.0, "abc", "", 4, 1.0),
      Row("", "Hello", 1, 1.0, "", "Hello", 2, 2.0),
      Row(" add", "World", 8, 3.0, null, null, null, null)
    ))
  }

  test("columnar nestedLoopJoin LeftOuter Join is equal to native With NULL") {
    val df = leftWithNull.join(rightWithNull, col("q") < col("c"),"leftouter")
    assert(
      df.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarBroadcastNestedLoopJoinExec]).isDefined,
      s"ColumnarBroadcastNestedLoopJoinExec not happened, " +
        s"executedPlan as follows： \n${df.queryExecution.executedPlan}")
    checkAnswer(df, Seq(
      Row(" add", "World", 8, 3.0, null, null, null, null),
      Row(" yeah  ", "yeah", 10, 8.0, null, null, null, null),
      Row("", "Hello", null, 1.0, null, null, null, null),
      Row("abc", null, 4, 2.0, null, null, null, null)
    ))
  }

  test("columnar nestedLoopJoin right outer join is equal to native") {
    val df = left.join(right, col("q") < col("c"), "rightouter")
    assert(
      df.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarBroadcastNestedLoopJoinExec]).isDefined,
      s"ColumnarBroadcastNestedLoopJoinExec not happened, " +
        s"executedPlan as follows： \n${df.queryExecution.executedPlan}")
    checkAnswer(df, Seq(
      Row("", "Hello", 1, 1.0, "abc", "", 4, 1.0),
      Row("", "Hello", 1, 1.0, "", "Hello", 2, 2.0),
      Row(null, null, null, null, " add", "World", 1, 3.0),
      Row(null, null, null, null, " yeah  ", "yeah", 0, 4.0)
    ))
  }

  test("columnar nestedLoopJoin right outer join is equal to native with null") {
    val df = leftWithNull.join(rightWithNull, col("q") < col("c"), "rightouter")
    assert(
      df.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarBroadcastNestedLoopJoinExec]).isDefined,
      s"ColumnarBroadcastNestedLoopJoinExec not happened, " +
        s"executedPlan as follows： \n${df.queryExecution.executedPlan}")
    assert(QueryTest.sameRows(Seq(
      Row(null, null, null, null, " add", null, 1, null),
      Row(null, null, null, null, " yeah  ", null, null, 4.0),
      Row(null, null, null, null, "", "Hello", 2, 2.0),
      Row(null, null, null, null, "abc", "", 4, 1.0)
    ),df.collect()).isEmpty,"the run value is error")
  }

  test("columnar nestedLoopJoin Cross Join is equal to native") {
    val df = left.join(right)
    assert(
      df.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarBroadcastNestedLoopJoinExec]).isDefined,
      s"ColumnarBroadcastNestedLoopJoinExec not happened, " +
        s"executedPlan as follows： \n${df.queryExecution.executedPlan}")
    checkAnswer(df, Seq(
      Row("abc", "", 4, 2.0, "abc", "", 4, 1.0),
      Row("abc", "", 4, 2.0, "", "Hello", 2, 2.0),
      Row("abc", "", 4, 2.0, " yeah  ", "yeah", 0, 4.0),
      Row("abc", "", 4, 2.0, " add", "World", 1, 3.0),
      Row(" yeah  ", "yeah", 10, 8.0, "abc", "", 4, 1.0),
      Row(" yeah  ", "yeah", 10, 8.0, "","Hello", 2, 2.0),
      Row(" yeah  ", "yeah", 10, 8.0, " yeah  ", "yeah", 0, 4.0),
      Row(" yeah  ", "yeah", 10, 8.0, " add", "World", 1, 3.0),
      Row("", "Hello", 1, 1.0, "abc", "", 4, 1.0),
      Row("", "Hello", 1, 1.0, "", "Hello", 2, 2.0),
      Row("", "Hello", 1, 1.0, " yeah  ", "yeah", 0, 4.0),
      Row("", "Hello", 1, 1.0, " add", "World", 1, 3.0),
      Row(" add", "World", 8, 3.0, "abc", "", 4, 1.0),
      Row(" add", "World", 8, 3.0, "", "Hello", 2, 2.0),
      Row(" add", "World", 8, 3.0, " yeah  ", "yeah", 0, 4.0),
      Row(" add", "World", 8, 3.0, " add", "World", 1, 3.0)
    ))
  }

  test("columnar nestedLoopJoin Cross Join is equal to native With NULL") {
    val df = leftWithNull.join(rightWithNull)
    assert(
      df.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarBroadcastNestedLoopJoinExec]).isDefined,
      s"ColumnarBroadcastNestedLoopJoinExec not happened, " +
        s"executedPlan as follows： \n${df.queryExecution.executedPlan}")
    checkAnswer(df, Seq(
      Row("abc", null, 4, 2.0, " yeah  ", null, null, 4.0),
      Row("abc", null, 4, 2.0, "abc", "", 4, 1.0),
      Row("abc", null, 4, 2.0, "", "Hello", 2, 2.0),
      Row("abc", null, 4, 2.0, " add", null, 1, null),
      Row(" yeah  ", "yeah", 10, 8.0, " yeah  ", null, null, 4.0),
      Row(" yeah  ", "yeah", 10, 8.0, "abc", "", 4, 1.0),
      Row(" yeah  ", "yeah", 10, 8.0, "", "Hello", 2, 2.0),
      Row(" yeah  ", "yeah", 10, 8.0, " add", null, 1, null),
      Row(" add", "World", 8, 3.0, " yeah  ", null, null, 4.0),
      Row(" add", "World", 8, 3.0, "abc", "", 4, 1.0),
      Row(" add", "World", 8, 3.0, "", "Hello", 2, 2.0),
      Row(" add", "World", 8, 3.0, " add", null, 1, null),
      Row("", "Hello", null, 1.0, " yeah  ", null, null, 4.0),
      Row("", "Hello", null, 1.0, "abc", "", 4, 1.0),
      Row("", "Hello", null, 1.0, "", "Hello", 2, 2.0),
      Row("", "Hello", null, 1.0, " add", null, 1, null)
    ))
  }
}