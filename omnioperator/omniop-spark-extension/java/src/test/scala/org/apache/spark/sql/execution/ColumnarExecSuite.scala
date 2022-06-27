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
    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[RowToOmniColumnarExec]).isDefined, s"RowToOmniColumnarExec not happened, executedPlan as follows： \n${res.queryExecution.executedPlan}")
  }

  test("validate data type convert") {
    val res = df.filter("a > 1")
    print(res.queryExecution.executedPlan)

    checkAnswer(
      df.filter("a > 1"),
      Row(2, 1.0, false) :: Row(6, null, false) :: Nil)
  }
}
