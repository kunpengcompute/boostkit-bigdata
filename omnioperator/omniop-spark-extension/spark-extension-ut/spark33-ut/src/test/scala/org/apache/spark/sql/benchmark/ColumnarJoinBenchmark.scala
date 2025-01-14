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

package org.apache.spark.sql.benchmark

import org.apache.spark.sql.functions._

object ColumnarJoinBenchmark extends ColumnarBasedBenchmark {
  def broadcastHashJoinLongKey(rowsA: Long): Unit = {
    val rowsB = 1 << 16
    val dim = spark.range(rowsB).selectExpr("id as k", "id as v")
    val df = spark.range(rowsA).join(dim.hint("broadcast"), (col("id") % rowsB) === col("k"))
    df.explain()
    columnarBenchmark(s"broadcastHashJoinLongKey spark.range(${rowsA}).join(spark.range(${rowsB}))", rowsA) {
      df.noop()
    }
  }

  def sortMergeJoin(rowsA: Long, rowsB: Long): Unit = {
    val df1 = spark.range(rowsA).selectExpr(s"id * 2 as k1")
    val df2 = spark.range(rowsB).selectExpr(s"id * 3 as k2")
    val df = df1.join(df2.hint("mergejoin"), col("k1") === col("k2"))
    df.explain()
    columnarBenchmark(s"sortMergeJoin spark.range(${rowsA}).join(spark.range(${rowsB}))", rowsA) {
      df.noop()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val rowsA = if (mainArgs.isEmpty) {
      20 << 20
    } else {
      mainArgs(0).toLong
    }

    val rowsB = if (mainArgs.isEmpty || mainArgs.length < 2) {
      1 << 16
    } else {
      mainArgs(1).toLong
    }

    runBenchmark("Join Benchmark") {
      broadcastHashJoinLongKey(rowsA)
      sortMergeJoin(rowsA, rowsB)
    }
  }
}
