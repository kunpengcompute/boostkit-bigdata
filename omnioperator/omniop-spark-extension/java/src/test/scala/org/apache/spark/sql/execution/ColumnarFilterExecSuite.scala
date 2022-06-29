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

class ColumnarFilterExecSuite extends ColumnarSparkPlanTest {
  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  private var inputDf: DataFrame = _
  private var inputDfWithNull: DataFrame = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    inputDf = Seq[(String, String, java.lang.Integer, java.lang.Double)](
      ("abc", "", 4, 2.0),
      ("", "Hello", 1, 1.0),
      (" add", "World", 8, 3.0),
      (" yeah  ", "yeah", 10, 8.0)
    ).toDF("a", "b", "c", "d")

    inputDfWithNull = Seq[(String, String, java.lang.Integer, java.lang.Double)](
      (null, "", 4, 2.0),
      (null, null, 1, 1.0),
      (" add", "World", 8, null),
      (" yeah  ", "yeah", 10, 8.0),
      (" yeah  ", "yeah", 10, 8.0)
    ).toDF("a", "b", "c", "d")
  }

  test("validate columnar filter exec happened") {
    val res = inputDf.filter("c > 1")
    print(res.queryExecution.executedPlan)
    val isColumnarFilterHappen = res.queryExecution.executedPlan
      .find(_.isInstanceOf[ColumnarFilterExec]).isDefined
    val isColumnarConditionProjectHappen = res.queryExecution.executedPlan
      .find(_.isInstanceOf[ColumnarConditionProjectExec]).isDefined
    assert(isColumnarFilterHappen || isColumnarConditionProjectHappen, s"ColumnarFilterExec not happened, executedPlan as follows： \n${res.queryExecution.executedPlan}")
  }

  test("columnar filter is equal to native") {
    val expr: Expression = (inputDf.col("c") > 3).expr
    checkThatPlansAgreeTemplate(expr = expr, df = inputDf)
  }

  test("columnar filter is equal to native with null") {
    val expr: Expression = (inputDfWithNull.col("c") > 3 && inputDfWithNull.col("d").isNotNull).expr
    checkThatPlansAgreeTemplate(expr = expr, df = inputDfWithNull)
  }

  def checkThatPlansAgreeTemplate(expr: Expression, df: DataFrame): Unit = {
    checkThatPlansAgree(
      df,
      (child: SparkPlan) =>
        ColumnarFilterExec(expr, child = child),
      (child: SparkPlan) =>
        FilterExec(expr, child = child),
      sortAnswers = false)
  }
}
