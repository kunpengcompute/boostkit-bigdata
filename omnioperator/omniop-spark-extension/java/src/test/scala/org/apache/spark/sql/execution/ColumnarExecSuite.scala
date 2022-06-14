/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package org.apache.spark.sql.execution

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StructType}

class ColumnarExecSuite extends ColumnarSparkPlanTest {
  private lazy val df = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(1, 2.0, false),
      Row(1, 2.0, false),
      Row(2, 1.0, false),
      Row(null, null, false),
      Row(null, 5.0, false),
      Row(6, null, false)
    )), new StructType().add("a", IntegerType).add("b", DoubleType)
      .add("c", BooleanType))

  test("validate columnar transfer exec happened") {
    val res = df.filter("a > 1")
    print(res.queryExecution.executedPlan)
    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[RowToOmniColumnarExec]).isDefined, s"RowToOmniColumnarExec not happened, executedPlan as follow: \n${res.queryExecution.executedPlan}")
  }

  test("validate data type convert") {
    val res = df.filter("a > 1")
    print(res.queryExecution.executedPlan)

    checkAnswer(
      df.filter("a > 1"),
      Row(2, 1.0, false) :: Row(6, null, false) :: Nil)
  }
}
