/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.sql.execution

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

// refer to DataFrameWindowFramesSuite
class ColumnarWindowExecSuite extends ColumnarSparkPlanTest with SharedSparkSession {
  import testImplicits._

  private var inputDf: DataFrame = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    inputDf = Seq(
      ("abc", "", 4, 2.0),
      ("", "Hello", 1, 1.0),
      (" add", "World", 8, 3.0),
      (" yeah  ", "yeah", 10, 8.0),
      ("abc", "", 10, 8.0)
    ).toDF("a", "b", "c", "d")
  }

  test("validate columnar window exec happened") {
    val res1 = Window.partitionBy("a").orderBy('c.desc)
    val res2 = inputDf.withColumn("max", max("c").over(res1))
    res2.head(10).foreach(row => println(row))
    assert(res2.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarWindowExec]).isDefined, s"ColumnarWindowExec not happened, executedPlan as follows: \n${res2.queryExecution.executedPlan}")
  }

  // todo: window check answer
  //  test("lead/lag with negative offset") {
  //    val df = Seq((1, "1"), (2, "2"), (1, "3"), (2, "4")).toDF("key", "value")
  //    val window = Window.partitionBy($"key").orderBy($"value")
  //
  //    checkAnswer(
  //      df.select(
  //        $"key",
  //        lead("value", -1).over(window),
  //        lag("value", -1).over(window)),
  //      Row(1, null, "3") :: Row(1, "1", null) :: Row(2, null, "4") :: Row(2, "2", null) :: Nil)
  //  }
}
