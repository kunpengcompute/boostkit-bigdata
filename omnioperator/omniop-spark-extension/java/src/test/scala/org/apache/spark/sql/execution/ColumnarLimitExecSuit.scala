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

class ColumnarLimitExecSuit extends ColumnarSparkPlanTest {

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
}
