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

package com.huawei.boostkit.spark

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
import org.apache.spark.sql.execution.{ColumnarBroadcastExchangeExec, ColumnarProjectExec, ColumnarTakeOrderedAndProjectExec, CommandResultExec, LeafExecNode, OmniColumnarToRowExec, ProjectExec, RowToOmniColumnarExec, SparkPlan, TakeOrderedAndProjectExec, UnaryExecNode}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

import scala.concurrent.Future

class TableWriteBasicFunctionSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  override def sparkConf: SparkConf = super.sparkConf
    .setAppName("test tableWriteBasicFunctionSuit")
    .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, "com.huawei.boostkit.spark.ColumnarPlugin")
    .set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.OmniColumnarShuffleManager")

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  test("Insert basic data (Non-partitioned table)") {
    val drop = spark.sql("drop table if exists employees_for_table_write_ut_test")
    drop.collect()
    val employees = Seq[(String, String, Int, Int)](
      ("Lisa", "Sales", 10000, 35),
    ).toDF("name", "dept", "salary", "age")
    employees.write.format("orc").saveAsTable("employees_for_table_write_ut_test")

    val insert = spark.sql("insert into " +
      "employees_for_table_write_ut_test values('Maggie', 'Sales', 1, 2)")
    insert.collect()
    val select = spark.sql("select * from employees_for_table_write_ut_test")
    val runRows = select.collect()
    val expectedRows = Seq(Row("Lisa", "Sales", 10000, 35), Row("Maggie", "Sales", 1, 2))
    assert(QueryTest.sameRows(runRows, expectedRows).isEmpty, "the run value is error")
  }

  test("Insert Basic data (Partitioned table)") {
    val drop = spark.sql("drop table if exists employees_for_table_write_ut_partition_test")
    drop.collect()
    val employees = Seq(("Lisa", "Sales", 10000, 35)).toDF("name", "dept", "salary", "age")
    employees.write.format("orc").partitionBy("age")
      .saveAsTable("employees_for_table_write_ut_partition_test")
    val insert = spark.sql("insert into employees_for_table_write_ut_partition_test " +
      "values('Maggie','Sales',200,30),('Bob','Sales',2000,30),('Tom','Sales',5000,20)")
    insert.collect()
    val select = spark.sql("select * from employees_for_table_write_ut_partition_test")
    val runRows = select.collect()
    val expectedRows = Seq(Row("Lisa", "Sales", 10000, 35), Row("Maggie", "Sales", 200, 30),
      Row("Bob", "Sales", 2000, 30), Row("Tom", "Sales", 5000, 20))
    assert(QueryTest.sameRows(runRows, expectedRows).isEmpty, "the run value is error")
  }

  test("Unsupported Scenarios") {
    val data = Seq[(Int, Int)](
      (10000, 35),
    ).toDF("id", "age")

    data.write.format("parquet").saveAsTable("table_write_ut_parquet_test")
    var insert = spark.sql("insert into table_write_ut_parquet_test values(1,2)")
    insert.collect()
    var columnarDataWrite = insert.queryExecution.executedPlan.asInstanceOf[CommandResultExec]
      .commandPhysicalPlan.find({
        case _: DataWritingCommandExec => true
        case _ => false
      }
      )
    assert(columnarDataWrite.isDefined, "use columnar data writing command")

    val createTable = spark.sql("create table table_write_ut_map_test" +
      " (id int, grades MAP<VARCHAR(50), int>) using orc")
    createTable.collect()
    insert = spark.sql("insert into table_write_ut_map_test (id, grades)" +
      " values(1, MAP('Math',90, 'English', 85))")
    insert.collect()
    columnarDataWrite = insert.queryExecution.executedPlan.asInstanceOf[CommandResultExec]
      .commandPhysicalPlan.find({
        case _: DataWritingCommandExec => true
        case _ => false
      }
      )
  }
}
