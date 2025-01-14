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
import org.apache.spark.sql.catalyst.expressions.NamedExpression

class ColumnarProjectExecSuite extends ColumnarSparkPlanTest {
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
      (" add", "World", 8, 3.0),
      (" yeah  ", "yeah", 10, 8.0)
    ).toDF("a", "b", "c", "d")
  }

  test("validate columnar project exec happened") {
    val res = inputDf.selectExpr("a as t")
    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined,
      s"ColumnarProjectExec not happened, executedPlan as follows： \n${res.queryExecution.executedPlan}")
  }

  test("columnar project is equal to native") {
    val projectList: Seq[NamedExpression] = Seq(inputDf.col("a").as("abc").expr.asInstanceOf[NamedExpression])
    checkThatPlansAgreeTemplate(projectList, inputDf)
  }

  test("columnar project is equal to native with null") {
    val projectList: Seq[NamedExpression] = Seq(inputDfWithNull.col("a").as("abc").expr.asInstanceOf[NamedExpression])
    checkThatPlansAgreeTemplate(projectList, inputDfWithNull)
  }

  def checkThatPlansAgreeTemplate(projectList: Seq[NamedExpression], df: DataFrame): Unit = {
    checkThatPlansAgree(
      df,
      (child: SparkPlan) =>
        ColumnarProjectExec(projectList, child = child),
      (child: SparkPlan) =>
        ProjectExec(projectList, child = child),
      sortAnswers = false)
  }
}
