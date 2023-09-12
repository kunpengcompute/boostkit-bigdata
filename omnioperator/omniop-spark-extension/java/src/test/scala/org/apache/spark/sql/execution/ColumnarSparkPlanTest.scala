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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, QueryTest, Row}
import org.apache.spark.sql.catalyst.util.stackTraceToString
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

private[sql] abstract class ColumnarSparkPlanTest extends SparkPlanTest with SharedSparkSession {
  // setup basic columnar configuration for columnar exec
  override def sparkConf: SparkConf = super.sparkConf
    .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, "com.huawei.boostkit.spark.ColumnarPlugin")
    .set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
    .set("spark.executorEnv.OMNI_CONNECTED_ENGINE", "Spark")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.OmniColumnarShuffleManager")
    .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")

  protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val analyzedDF = try df catch {
      case ae: AnalysisException =>
        if (ae.plan.isDefined) {
          fail(
            s"""
               |Failed to analyze query: $ae
               |${ae.plan.get}
               |
               |${stackTraceToString(ae)}
               |""".stripMargin)
        } else {
          throw ae
        }
    }
    assertEmptyMissingInput(analyzedDF)
    QueryTest.checkAnswer(analyzedDF, expectedAnswer)
  }

  private def assertEmptyMissingInput(query: Dataset[_]): Unit = {
    assert(query.queryExecution.analyzed.missingInput.isEmpty,
      s"The analyzed logical plan has missing inputs:\n${query.queryExecution.analyzed}")
    assert(query.queryExecution.optimizedPlan.missingInput.isEmpty,
      s"The optimized logical plan has missing inputs:\n${query.queryExecution.optimizedPlan}")
    assert(query.queryExecution.executedPlan.missingInput.isEmpty,
      s"The physical plan has missing inputs:\n${query.queryExecution.executedPlan}")
  }
}
