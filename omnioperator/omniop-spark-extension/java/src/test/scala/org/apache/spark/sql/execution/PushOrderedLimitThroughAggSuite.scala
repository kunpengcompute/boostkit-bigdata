/*
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

import org.apache.spark.sql.functions.{avg, count, first, max, min, sum}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

class PushOrderedLimitThroughAggSuite extends ColumnarSparkPlanTest {
  private var df: DataFrame = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    df = spark.createDataFrame(
      sparkContext.parallelize(Seq(
        Row(1, 2.0, 1L, "a"),
        Row(1, 2.0, 2L, null),
        Row(2, 1.0, 3L, "c"),
        Row(null, null, 6L, "e"),
        Row(null, 5.0, 7L, "f")
      )), new StructType().add("a", IntegerType).add("b", DoubleType)
        .add("c", LongType).add("d", StringType))
    df.createOrReplaceTempView("df_tbl")
  }

  test("Test PushOrderedLimitThroughAgg") {
    spark.conf.set("spark.omni.sql.columnar.topNSort", true)
    spark.conf.set("spark.omni.sql.columnar.pushOrderedLimitThroughAggEnable.enabled", true)
    spark.conf.set("spark.sql.adaptive.enabled", true)
    val sql1 = """
      SELECT a, b, SUM(c) AS sum_agg
      FROM df_tbl
      GROUP BY a, b
      ORDER BY a, sum_agg, b
      LIMIT 10
    """
    try {
      assertPushColumnarTopNSortThroughAggEffective(sql1)
    } finally {
      spark.conf.unset("spark.omni.sql.columnar.topNSort")
      spark.conf.unset("spark.omni.sql.columnar.pushOrderedLimitThroughAggEnable.enabled")
      spark.conf.unset("spark.sql.adaptive.enabled")
    }
  }

  private def assertPushColumnarTopNSortThroughAggEffective(sql: String): Unit = {
    val omniResult = spark.sql(sql)
    omniResult.collect();
    val omniPlan = omniResult.queryExecution.executedPlan.toString()
    val patternOpenTopn = """(?s)OmniColumnarShuffleExchange.*?\n.*?OmniColumnarTopNSort.*?\n.*?OmniColumnarHashAggregate""".r

    assert(patternOpenTopn.findFirstIn(omniPlan).isDefined,
      s"SQL:${sql}\n@OmniEnv with PushColumnarTopNSortThroughAgg and topNSort, omniPlan:${omniPlan}"
    )

    spark.conf.set("spark.omni.sql.columnar.topNSort", false)
    val omniPlanWithSortResult = spark.sql(sql)
    omniPlanWithSortResult.collect()
    val omniPlanWithSort = omniPlanWithSortResult.queryExecution.executedPlan.toString()
    val patternCloseTopn = """(?s)OmniColumnarShuffleExchange.*?\n.*?OmniColumnar.*?Limit.*?\n.*?OmniColumnarSort.*?\n.*?OmniColumnarHashAggregate""".r
    assert(patternCloseTopn.findFirstIn(omniPlanWithSort).isDefined,
      s"SQL:${sql}\n@OmniEnv with PushColumnarTopNSortThroughAgg, omniPlan:${omniPlanWithSort}"
    )
  }
}
