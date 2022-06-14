/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
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
      (" add", "World", 8, null),
      (" yeah  ", "yeah", 10, 8.0)
    ).toDF("a", "b", "c", "d")
  }

  test("validate columnar project exec happened") {
    val res = inputDf.filter("a as t")
    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined,
      s"ColumnarProjectExec not happened, executedPlan as follow: \n${res.queryExecution.executedPlan}")
  }

  test("columnar project is equal to native") {
    val projectList: Seq[NamedExpression] = Seq(inputDf.col("a").as("abc").expr.asInstanceOf[NamedExpression])
    checkTharPlansAgreeTemplate(projectList, inputDf)
  }

  test("columnar project is equal to native with null") {
    val projectList: Seq[NamedExpression] = Seq(inputDfWithNull.col("a").as("abc").expr.asInstanceOf[NamedExpression])
    checkTharPlansAgreeTemplate(projectList, inputDfWithNull)
  }

  def checkTharPlansAgreeTemplate(projectList: Seq[NamedExpression], df: DataFrame): Unit = {
    checkTharPlansAgree(
      df,
      (child: SparkPlan) =>
        ColumnarProjectExec(projectList, child = child),
      (child: SparkPlan) =>
        ProjectExec(projectList, child = child),
      sortAnswers = false)
  }
}
