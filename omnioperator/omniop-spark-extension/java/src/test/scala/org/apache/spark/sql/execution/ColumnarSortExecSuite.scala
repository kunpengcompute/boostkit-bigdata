/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.sql.execution

import java.lang

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.SortOrder

class ColumnarSortExecSuite extends ColumnarSparkPlanTest {
  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  test("validate columnar sort exec happened") {
    val inputDf = Seq[(String, java.lang.Integer, java.lang.Double)](
      ("Hello", 4, 2.0),
      ("Hello", 1, 1.0),
      ("World", 8, 3.0)
    ).toDF("a", "b", "c")
    val res = inputDf.sort(inputDf("b").asc)
    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarSortExec]).isDefined, s"ColumnarSortExec not happened, executedPlan as follows: \n${res.queryExecution.executedPlan}")
  }

  test("columnar sort is equal to native sort") {
    val df = Seq[(String, java.lang.Integer, java.lang.Double)](
      ("Hello", 4, 2.0),
      ("Hello", 1, 1.0),
      ("World", 8, 3.0)
    ).toDF("a", "b", "c")
    val sortOrder = Stream('a.asc, 'b.asc, 'c.asc)
    checkThatPlansAgreeTemplate(input = df, sortOrder = sortOrder)
  }

  test("columnar sort is equal to native sort with null") {
    val dfWithNull = Seq[(String, Integer, Double)](
      ("Hello", 4, 2.0),
      (null, 1, 1.0),
      ("World", null, 3.0),
      ("World", 8, 3.0),
    ).toDF("a", "b", "c")
    val sortOrder = Stream('a.asc, 'b.asc, 'c.asc)
    checkThatPlansAgreeTemplate(input = dfWithNull, sortOrder = sortOrder)
  }

  def checkThatPlansAgreeTemplate(input: DataFrame, sortOrder: Seq[SortOrder]): Unit = {
    checkThatPlansAgree(
      input,
      (child: SparkPlan) =>
        ColumnarSortExec(sortOrder, global = true, child = child),
      (child: SparkPlan) =>
        SortExec(sortOrder, global = true, child = child),
      sortAnswers = false)
  }
}
