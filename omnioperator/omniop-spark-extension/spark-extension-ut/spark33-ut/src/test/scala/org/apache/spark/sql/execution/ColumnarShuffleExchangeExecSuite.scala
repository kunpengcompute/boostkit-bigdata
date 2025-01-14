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
      s"ColumnarSortExec not happened, executedPlan as follows： \n${res.queryExecution.executedPlan}")

    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarShuffleExchangeExec]).isDefined,
      s"ColumnarShuffleExchangeExec not happened, executedPlan as follows: \n${res.queryExecution.executedPlan}")
  }
}
