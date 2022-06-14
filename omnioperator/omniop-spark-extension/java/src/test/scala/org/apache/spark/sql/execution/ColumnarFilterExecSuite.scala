/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
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
      (" yeah  ", "yeah", 10, 8.0)
    ).toDF("a", "b", "c", "d")
  }

  test("validate columnar filter exec happened") {
    val res = inputDf.filter("c > 1")
    print(res.queryExecution.executedPlan)
    val isColumnarFilterHappen = res.queryExecution.executedPlan
      .find(_.isInstanceOf[ColumnarFilterExec]).isDefined
    val isColumnarConditionHappen = res.queryExecution.executedPlan
      .find(_.isInstanceOf[ColumnarConditionProjectExec]).isDefined
    assert(isColumnarFilterHappen || isColumnarConditionHappen, s"ColumnarFilterExec not happened, executedPlan as follow: \n${res.queryExecution.executedPlan}")
  }

  test("columnar filter is equal to native") {
    val expr: Expression = (inputDf.col("c") > 3).expr
    checkTharPlansAgreeTemplate(expr = expr, df = inputDf)
  }

  test("columnar filter is equal to native with null") {
    val expr: Expression = (inputDfWithNull.col("c") > 3 && inputDf.col("d").isNotNull).expr
    checkTharPlansAgreeTemplate(expr = expr, df = inputDfWithNull)
  }

  def checkTharPlansAgreeTemplate(expr: Expression, df: DataFrame): Unit = {
    checkTharPlansAgree(
      df,
      (child: SparkPlan) =>
        ColumnarFilterExec(expr, child = child),
      (child: SparkPlan) =>
        FilterExec(expr, child = child),
      sortAnswers = false)
  }
}
