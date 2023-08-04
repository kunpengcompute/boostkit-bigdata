/*
 * Copyright (C) 2022-2023. Huawei Technologies Co., Ltd. All rights reserved.
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

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.File

class ColumnarFileScanExecSuite extends ColumnarSparkPlanTest {
  private var load: DataFrame = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
  }

  test("validate columnar filescan exec for parquet happened") {
    val file = new File("src/test/java/com/huawei/boostkit/spark/jni/parquetsrc/date_dim.parquet")
    val path = file.getAbsolutePath
    load = spark.read.parquet(path)
    load.createOrReplaceTempView("parquet_scan_table")
    val res = spark.sql("select * from parquet_scan_table")
    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarFileSourceScanExec]).isDefined,
      s"ColumnarFileSourceScanExec not happened, executedPlan as follows： \n${res.queryExecution.executedPlan}")
    res.show()
  }
}