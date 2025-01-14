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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col

class ColumnarLimitExecSuit extends ColumnarSparkPlanTest {

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.omni.sql.columnar.nativefilescan", "false")

  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  private var left: DataFrame = _
  private var right: DataFrame = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    left = Seq[(java.lang.Integer, java.lang.Integer, java.lang.Integer)](
      (1, 1, 1),
      (2, 2, 2),
      (3, 3, 3),
      (4, 5, 6)
    ).toDF("a", "b", "c")
    left.createOrReplaceTempView("left")

    right = Seq[(java.lang.Integer, java.lang.Integer, java.lang.Integer)](
      (1, 1, 1),
      (2, 2, 2),
      (3, 3, 3)
    ).toDF("x", "y", "z")
    right.createOrReplaceTempView("right")
  }

  test("limit with local and global limit columnar exec") {
    val result = spark.sql("SELECT y FROM right WHERE x in " +
      "(SELECT a FROM left WHERE a = 4 LIMIT 2)")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarLocalLimitExec]).isDefined,
      s"not match ColumnarLocalLimitExec, real plan: ${plan}")
    assert(plan.find(_.isInstanceOf[LocalLimitExec]).isEmpty,
      s"real plan: ${plan}")
    assert(plan.find(_.isInstanceOf[ColumnarGlobalLimitExec]).isDefined,
      s"not match ColumnarGlobalLimitExec, real plan: ${plan}")
    assert(plan.find(_.isInstanceOf[GlobalLimitExec]).isEmpty,
      s"real plan: ${plan}")
    // 0 rows return
    assert(result.count() == 0)
  }

  test("limit with rollback global limit to row-based exec") {
    spark.conf.set("spark.omni.sql.columnar.globalLimit", false)
    val result = spark.sql("SELECT a FROM left WHERE a in " +
      "(SELECT x FROM right LIMIT 2)")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarLocalLimitExec]).isDefined,
      s"not match ColumnarLocalLimitExec, real plan: ${plan}")
    assert(plan.find(_.isInstanceOf[LocalLimitExec]).isEmpty,
      s"real plan: ${plan}")
    assert(plan.find(_.isInstanceOf[ColumnarGlobalLimitExec]).isEmpty,
      s"match ColumnarGlobalLimitExec, real plan: ${plan}")
    assert(plan.find(_.isInstanceOf[GlobalLimitExec]).isDefined,
      s"real plan: ${plan}")
    // 2 rows return
    assert(result.count() == 2)
    spark.conf.set("spark.omni.sql.columnar.globalLimit", true)
  }

  test("Push down limit through LEFT SEMI and LEFT ANTI join") {
    withTable("left_table", "nonempty_right_table", "empty_right_table") {
      spark.sql("SET spark.sql.adaptive.enabled=false")
      spark.range(5).toDF().repartition(1).write.saveAsTable("left_table")
      spark.range(3).write.saveAsTable("nonempty_right_table")
      spark.range(0).write.saveAsTable("empty_right_table")
      Seq("LEFT SEMI", "LEFT ANTI").foreach { joinType =>
        val joinWithNonEmptyRightDf = spark.sql(
          s"SELECT * FROM left_table $joinType JOIN nonempty_right_table LIMIT 3")
        val joinWithEmptyRightDf = spark.sql(
          s"SELECT * FROM left_table $joinType JOIN empty_right_table LIMIT 3")

        val expectedAnswer = Seq(Row(0), Row(1), Row(2))
        if (joinType == "LEFT SEMI") {
          checkAnswer(joinWithNonEmptyRightDf, expectedAnswer)
          checkAnswer(joinWithEmptyRightDf, Seq.empty)
        } else {
          checkAnswer(joinWithNonEmptyRightDf, Seq.empty)
          checkAnswer(joinWithEmptyRightDf, expectedAnswer)
        }

        Seq(joinWithNonEmptyRightDf, joinWithEmptyRightDf).foreach { df =>
          val pushedLocalLimits = df.queryExecution.executedPlan.collect {
            case l : ColumnarLocalLimitExec => l
          }
          assert(pushedLocalLimits.length === 1)
        }
      }
    }
  }

  test("Push down limit through left join") {
    val res = left.join(right.hint("broadcast"), col("a") === col("x"), "leftouter").limit(3)
    assert(
      res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarLocalLimitExec]).isDefined,
      s"ColumnarLocalLimitExec not happened," +
        s" executedPlan as follows: \n${res.queryExecution.executedPlan}")
  }
}
