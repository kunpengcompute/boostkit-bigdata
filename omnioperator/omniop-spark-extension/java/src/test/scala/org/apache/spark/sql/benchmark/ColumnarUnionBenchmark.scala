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

object ColumnarUnionBenchmark extends ColumnarBasedBenchmark {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val N = if (mainArgs.isEmpty) {
      5L << 15
    } else {
      mainArgs(0).toLong
    }

    val M = if (mainArgs.isEmpty || mainArgs.length < 2) {
      10L << 15
    } else {
      mainArgs(1).toLong
    }

    runBenchmark("union with API") {
      val rangeM = spark.range(M)
      spark.range(N).union(rangeM).explain()
      columnarBenchmark(s"spark.range(${N}).union(spark.range(${M}))", N) {
        spark.range(N).union(rangeM).noop()
      }
    }
  }
}
