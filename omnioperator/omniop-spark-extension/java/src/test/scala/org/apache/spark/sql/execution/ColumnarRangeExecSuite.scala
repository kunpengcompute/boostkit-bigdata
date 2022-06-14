/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package org.apache.spark.sql.execution

// refer to DataFrameRangeSuite
class ColumnarRangeExecSuite extends ColumnarSparkPlanTest {
  test("validate columnar range exec happened") {
    val res = spark.range(0, 10, 1)
    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarRangeExec]).isDefined, s"ColumnarRangeExec not happened, executedPlan as follow: \n${res.queryExecution.executedPlan}")
  }
}
