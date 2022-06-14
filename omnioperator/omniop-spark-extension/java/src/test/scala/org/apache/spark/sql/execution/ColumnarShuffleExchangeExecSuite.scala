/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.sql.execution

class ColumnarShuffleExchangeExecSuite extends ColumnarSparkPlanTest {
  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  protected override def beforeAll(): Unit = {
    super.beforeAll()
  }

  test("validate columnar shuffleExchange exec worked") {
    val inputDf = Seq[(String, java.lang.Integer, java.lang.Double)] (
      ("Sam", 12, 9.1),
      ("Bob", 13, 9.3),
      ("Ted", 10, 8.9)
    ).toDF("name", "age", "point")
    val res = inputDf.sort(inputDf("age").asc)
    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarSortExec]).isDefined,
      s"ColumnarSortExec not happened, executedPlan as follows: \n${res.queryExecution.executedPlan}")

    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarShuffleExchangeExec]).isDefined,
      s"ColumnarShuffleExchangeExec not happened, executedPlan as follows: \n${res.queryExecution.executedPlan}")
  }
}
