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
    assert(res2.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarWindowExec]).isDefined, s"ColumnarWindowExec not happened, executedPlan as follows： \n${res2.queryExecution.executedPlan}")
  }
}
