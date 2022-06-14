/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package org.apache.spark.sql.execution

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.catalyst.expressions.{NamedExpression, SortOrder}

// refer to TakeOrderedAndProjectSuite
class ColumnarTopNExecSuite extends ColumnarSparkPlanTest {
  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  private var inputDf: DataFrame = _
  private var inputDfWithNull: DataFrame = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    inputDf = Seq[(java.lang.Integer, java.lang.Double, String)](
      (4, 2.0, "abc"),
      (4, 2.0, "aaa"),
      (4, 2.0, "ddd"),
      (4, 2.0, "")
    ).toDF("a", "b", "c")

    inputDfWithNull = Seq[(String, String, java.lang.Integer, java.lang.Double)](
      (null, "", 4, 2.0),
      ("", null, 1, 1.0),
      (" add", "World", 8, null),
      (" yeah  ", "yeah", 10, 8.0)
    ).toDF("a", "b", "c", "d")
  }

  test("validate columnar topN exec happened") {
    val res = inputDf.sort("a").limit(2)
    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarTakeOrderedAndProjectExec]).isDefined, s"ColumnarTakeOrderedAndProjectExec not happened, executedPlan as follow: \n${res.queryExecution.executedPlan}")
  }

  test("columnar topN is equal to native") {
    val limit = 3
    val sortOrder = Stream('a.asc, 'b.desc)
    val projectList = Seq(inputDf.col("a").as("abc").expr.asInstanceOf[NamedExpression])
    checkTharPlansAgreeTemplate(inputDf, limit, sortOrder, projectList)
  }

  test("columnar topN is equal to native with null") {
    val res = inputDfWithNull.orderBy("a", "b").selectExpr("c + 1", "d + 2").limit(2)
    checkAnswer(res, Seq(Row(2, 3.0), Row(9, null)))
  }

  def checkTharPlansAgreeTemplate(df: DataFrame, limit: Int, sortOrder: Seq[SortOrder],
                                  projectList: Seq[NamedExpression]): Unit = {
    checkTharPlansAgree(
      df,
      input => ColumnarTakeOrderedAndProjectExec(limit, sortOrder, projectList, input),
      input => TakeOrderedAndProjectExec(limit, sortOrder, projectList, input),
      sortAnswers = false)
  }
}
