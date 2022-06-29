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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ColumnarBroadcastHashJoinExec, ColumnarSortMergeJoinExec}
import org.apache.spark.sql.functions.col

// refer to joins package
class ColumnarJoinExecSuite extends ColumnarSparkPlanTest {
  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  private var left: DataFrame = _
  private var right: DataFrame = _
  private var leftWithNull: DataFrame = _
  private var rightWithNull: DataFrame = _

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

  test("validate columnar broadcastHashJoin exec happened") {
    val res = left.join(right.hint("broadcast"), col("q") === col("c"))
    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarBroadcastHashJoinExec]).isDefined, s"ColumnarBroadcastHashJoinExec not happened, executedPlan as follows： \n${res.queryExecution.executedPlan}")
  }

  test("validate columnar sortMergeJoin exec happened") {
    val res = left.join(right.hint("mergejoin"), col("q") === col("c"))
    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarSortMergeJoinExec]).isDefined, s"ColumnarSortMergeJoinExec not happened, executedPlan as follows： \n${res.queryExecution.executedPlan}")
  }

  test("columnar broadcastHashJoin is equal to native") {
    val df = left.join(right.hint("broadcast"), col("q") === col("c"))
    val leftKeys = Seq(left.col("q").expr)
    val rightKeys = Seq(right.col("c").expr)
    checkThatPlansAgreeTemplate(df, leftKeys, rightKeys)
  }

  test("columnar sortMergeJoin is equal to native") {
    val df = left.join(right.hint("mergejoin"), col("q") === col("c"))
    val leftKeys = Seq(left.col("q").expr)
    val rightKeys = Seq(right.col("c").expr)
    checkThatPlansAgreeTemplate(df, leftKeys, rightKeys)
  }

  test("columnar broadcastHashJoin is equal to native with null") {
    val df = leftWithNull.join(rightWithNull.hint("broadcast"),
      col("q").isNotNull === col("c").isNotNull)
    val leftKeys = Seq(leftWithNull.col("q").isNotNull.expr)
    val rightKeys = Seq(rightWithNull.col("c").isNotNull.expr)
    checkThatPlansAgreeTemplate(df, leftKeys, rightKeys)
  }

  def checkThatPlansAgreeTemplate(df: DataFrame, leftKeys: Seq[Expression],
                                  rightKeys: Seq[Expression]): Unit = {
    checkThatPlansAgree(
      df,
      (child: SparkPlan) =>
        ColumnarBroadcastHashJoinExec(leftKeys, rightKeys, Inner,
          BuildRight, None, child, child),
      (child: SparkPlan) =>
        BroadcastHashJoinExec(leftKeys, rightKeys, Inner,
          BuildRight, None, child, child),
      sortAnswers = false)
  }
}
