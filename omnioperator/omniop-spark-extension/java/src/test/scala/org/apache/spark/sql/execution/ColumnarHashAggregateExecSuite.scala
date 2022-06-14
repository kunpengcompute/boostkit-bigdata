/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package org.apache.spark.sql.execution

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

class ColumnarHashAggregateExecSuite extends ColumnarSparkPlanTest {
  private var df: DataFrame = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    df = spark.createDataFrame(
      sparkContext.parallelize(Seq(
        Row(1, 2.0, 1L, "a"),
        Row(1, 2.0, 2L, null),
        Row(2, 1.0, 3L, "c"),
        Row(null, null, 6L, "e"),
        Row(null, 5.0, 7L, "f")
      )), new StructType().add("a", IntegerType).add("b", DoubleType)
        .add("c", LongType).add("d", StringType)
    )
  }

  test("validate columnar hashAgg exec happened") {
    val res = df.groupBy("a").agg(sum("b"))
    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined, s"ColumnarHashAggregateExec not happened, executedPlan as follow: \n${res.queryExecution.executedPlan}")
  }

  test("check columnar hashAgg result") {
    val res = testData2.groupBy("a").agg(sum("b"))
    checkAnswer(
      res,
      Seq(Row(1, 3), Row(2, 3), Row(3, 3))
    )
  }

  test("check columnar hashAgg result with null") {
    val res = df.filter(df("a").isNotNull && df("d").isNotNull).groupBy("a").agg(sum("b"))
    checkAnswer(
      res,
      Seq(Row(1, 2.0), Row(2, 1.0))
    )
  }
}
