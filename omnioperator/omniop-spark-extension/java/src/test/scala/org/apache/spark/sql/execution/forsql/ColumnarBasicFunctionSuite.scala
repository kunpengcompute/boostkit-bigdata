/**
 * Copyright (C) 2025-2025. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.forsql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, QueryStageExec}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ColumnarBroadcastHashJoinExec}
import org.apache.spark.sql.execution.{ColumnarBroadcastExchangeExec, ColumnarFilterExec, ColumnarProjectExec, ColumnarTakeOrderedAndProjectExec, LeafExecNode, OmniColumnarToRowExec, ProjectExec, RowToOmniColumnarExec, SparkPlan, TakeOrderedAndProjectExec, UnaryExecNode}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

import scala.concurrent.Future

class ColumnarBasicFunctionSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  override def sparkConf: SparkConf = super.sparkConf
    .setAppName("test columnarBasicFunctionSuite")
    .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, "com.huawei.boostkit.spark.ColumnarPlugin")
    .set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.OmniColumnarShuffleManager")

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  test("Unsupported StaticInvoke function varcharTypeWriteSideCheck") {
    val drop = spark.sql("drop table if exists source_table")
    drop.collect()
    val createTable = spark.sql("create table source_table" +
      "(id int, name string, amount int) using parquet")
    createTable.collect()
    val dropNP = spark.sql("drop table if exists target_table")
    dropNP.collect()
    val createTableNP = spark.sql("create table target_table" +
      "(name varchar(5), total_amount long) using parquet")
    createTableNP.collect()
    var insert = spark.sql("insert into table source_table values" +
      "(1, 'Bob', 250), (2, '測試中文', 250), (3, NULL, 250), (4, 'abide', 250)")
    insert.collect()
    insert = spark.sql("insert into table target_table select UPPER(name) as name, SUM(amount) as total_amount from " +
      "source_table where amount >= 10 GROUP BY UPPER(name)")
    insert.collect()
    assert(insert.queryExecution.executedPlan.toString().contains("OmniColumnarHashAggregate"),
      "use columnar data writing command")
    val columnarDataWrite = insert.queryExecution.executedPlan
      .find({
        case _: HashAggregateExec => true
        case _ => false
      })
    assert(columnarDataWrite.isEmpty, "use columnar data writing command")
    val select = spark.sql("select * from target_table order by name")
    val runRows = select.collect()
    val expectedRows = Seq(Row(null, 250), Row("ABIDE", 250), Row("BOB", 250), Row("測試中文", 250))
    assert(QueryTest.sameRows(runRows, expectedRows).isEmpty, "the run value is error")
  }
}