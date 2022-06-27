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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.internal.SQLConf

/**
 * Common basic scenario to run benchmark
 */
abstract class ColumnarBasedBenchmark extends SqlBasedBenchmark {
  /** Runs function `f` with 3 scenario(spark WSCG on, off and omni-columnar processing) */
  final def columnarBenchmark(name: String, cardinality: Long)(f: => Unit): Unit = {
    val benchmark = new Benchmark(name, cardinality, output = output)
    if (getSparkSession.conf.getOption("spark.sql.extensions").isDefined)
    {
      benchmark.addCase(s"$name omniruntime wholestage off", numIters = 5) { _ =>
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          f
        }
      }
    }
    else
    {
      benchmark.addCase(s"$name Spark wholestage off", numIters = 5) { _ =>
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          f
        }
      }
      benchmark.addCase(s"$name Spark wholestage on", numIters = 5) { _ =>
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
          f
        }
      }
    }

    benchmark.run()
  }
}
