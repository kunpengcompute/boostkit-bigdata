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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.{ColumnarSparkPlanTest, SparkPlan}

class CombineJoinedAggregatesSuite extends ColumnarSparkPlanTest {

  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  private var dfForResin: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    dfForResin = Seq[(Boolean, Short, Int, Long, Float, Double, String)](
      (true, 10.toShort, 100, 1000L, 1.5f, 10.5, "aaa"),
      (false, 20.toShort, 200, 2000L, 2.5f, 20.5, "bbb"),
      (true, 30.toShort, 300, 3000L, 3.5f, 30.5, "ccc"),
      (false, 40.toShort, 400, 4000L, 4.5f, 40.5, "aaa"),
      (true, 50.toShort, 500, 5000L, 5.5f, 50.5, "bbb"),
      (false, 60.toShort, 100, 6000L, 6.5f, 60.5, "ccc"),
      (true, 70.toShort, 200, 7000L, 7.5f, 70.5, "aaa"),
      (false, 80.toShort, 300, 8000L, 8.5f, 80.5, "bbb"),
      (true, 90.toShort, 400, 9000L, 9.5f, 90.5, "ccc"),
      (false, 100.toShort, 500, 10000L, 10.5f, 100.5, "aaa")
    ).toDF("boolean", "short", "int", "long", "float", "double", "string")
    dfForResin.createOrReplaceTempView("dfForResin")
  }

  def countNodes(plan: SparkPlan): Int = {
    plan.nodeName match {
      case "LocalTableScan" => 1 + plan.children.map(countNodes).sum
      case _ => plan.children.map(countNodes).sum
    }
  }

  test("logicalPlan can be combined when ordinary Aggregate Functions") {
    spark.conf.set("spark.omni.sql.columnar.combineJoinedAggregates.enabled", true)
    val result = spark.sql(
      "SELECT * FROM (SELECT MIN(short), SUM(int) FROM dfForResin WHERE boolean = true) AS t1 "
        +
        "JOIN (SELECT MIN(short), SUM(int) FROM dfForResin WHERE boolean = false) AS t2;")
    val plan = result.queryExecution.executedPlan
    val count = countNodes(plan)
    val str = spark.conf.get("spark.omni.sql.columnar.combineJoinedAggregates.enabled")
    assert(count == 1)
  }

  test("logicalPlan can be combined when more than 2 Aggregates ") {
    spark.conf.set("spark.omni.sql.columnar.combineJoinedAggregates.enabled", true)
    val result = spark.sql(
      "SELECT * FROM (SELECT MAX(boolean), MIN(short), SUM(int), AVG(Long), COUNT(float), COUNT(DISTINCT double) FROM dfForResin WHERE string = \"aaa\") AS t1 "
        +
        "JOIN (SELECT MAX(boolean), MIN(short), SUM(int), AVG(long), COUNT(float), COUNT(DISTINCT double) FROM dfForResin WHERE string = \"bbb\") AS t2 "
        +
        "JOIN (SELECT MAX(boolean), MIN(short), SUM(int), AVG(long), COUNT(float), COUNT(DISTINCT double) FROM dfForResin WHERE string = \"ccc\") AS t3;"
    )
    val plan = result.queryExecution.executedPlan
    val count = countNodes(plan)
    assert(count == 1)
  }

  test("logicalPlan can be combined when left and right are different Aggregate types ") {
    spark.conf.set("spark.omni.sql.columnar.combineJoinedAggregates.enabled", true)
    val result = spark.sql(
      "SELECT * FROM (SELECT MAX(short), MIN(int) FROM dfForResin WHERE boolean = true) AS t1 "
        +
        "JOIN (SELECT SUM(short), AVG(int) FROM dfForResin WHERE boolean = false) AS t2;")
    val plan = result.queryExecution.executedPlan
    val count = countNodes(plan)
    assert(count == 1)
  }

  test("logicalPlan can be combined when left and right have different quantities ") {
    spark.conf.set("spark.omni.sql.columnar.combineJoinedAggregates.enabled", true)
    val result = spark.sql(
      "SELECT * FROM (SELECT MAX(short), MIN(int) FROM dfForResin WHERE boolean = true) AS t1 "
        +
        "JOIN (SELECT MAX(short), MIN(int), MAX(long)  FROM dfForResin WHERE boolean = false) AS t2;")
    val plan = result.queryExecution.executedPlan
    val count = countNodes(plan)
    assert(count == 1)
  }

  test("logicalPlan can be combined when one aggregate rollback") {
    spark.conf.set("spark.omni.sql.columnar.combineJoinedAggregates.enabled", true)
    val result = spark.sql(
      "SELECT * FROM (SELECT MAX(short), MIN(int) FROM dfForResin WHERE boolean = true) AS t1 "
        +
        "JOIN (SELECT MAX(short), MIN(int)  FROM dfForResin WHERE pmod(int, 3) = 1) AS t2;")
    val plan = result.queryExecution.executedPlan
    val count = countNodes(plan)
    assert(count == 1)
  }
  test("logicalPlan can not be combined when there is group by") {
    spark.conf.set("spark.omni.sql.columnar.combineJoinedAggregates.enabled", true)
    val result = spark.sql(
      "SELECT * FROM (SELECT MAX(short), MIN(int) FROM dfForResin WHERE boolean = true GROUP BY string) AS t1 "
        +
        "JOIN (SELECT MAX(short), MIN(int)  FROM dfForResin WHERE boolean = false GROUP BY string) AS t2;")
    val plan = result.queryExecution.executedPlan
    val count = countNodes(plan)
    assert(count == 2)
  }

  test("logicalPlan can not be combined when there is window function") {
    spark.conf.set("spark.omni.sql.columnar.combineJoinedAggregates.enabled", true)
    val result = spark.sql(
      "SELECT * FROM (SELECT MAX(short) OVER (PARTITION BY int ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM dfForResin WHERE boolean = true) AS t1 "
        +
        "JOIN (SELECT MAX(short) OVER (PARTITION BY int ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM dfForResin WHERE boolean = false) AS t2;")
    val plan = result.queryExecution.executedPlan
    val count = countNodes(plan)
    assert(count == 2)
  }

  test("logicalPlan can be combined partially when join's left and right are eligible ") {
    spark.conf.set("spark.omni.sql.columnar.combineJoinedAggregates.enabled", true)
    val result = spark.sql(
      "SELECT * FROM (SELECT MAX(short), MIN(int) FROM dfForResin WHERE boolean = true) AS t1 "
        +
        "JOIN (SELECT MAX(short), MIN(int) FROM dfForResin WHERE boolean = false) AS t2 "
        +
        "JOIN (SELECT MAX(short), MIN(int) FROM dfForResin WHERE boolean = false GROUP BY string) AS t3;"
    )
    val plan = result.queryExecution.executedPlan
    val count = countNodes(plan)
    assert(count == 2)
  }

  test("logicalPlan can not be combined when join's left and right are not eligible ") {
    spark.conf.set("spark.omni.sql.columnar.combineJoinedAggregates.enabled", true)
    val result = spark.sql(
      "SELECT * FROM (SELECT MAX(short), MIN(int) FROM dfForResin WHERE boolean = true) AS t1 "
        +
        "JOIN (SELECT MAX(short), MIN(int) FROM dfForResin WHERE boolean = false GROUP BY string) AS t2 "
        +
        "JOIN (SELECT MAX(short), MIN(int) FROM dfForResin WHERE boolean = false) AS t3;"
    )
    val plan = result.queryExecution.executedPlan
    val count = countNodes(plan)
    assert(count == 3)
  }

}
