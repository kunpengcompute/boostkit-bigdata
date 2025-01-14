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
import org.apache.spark.sql.execution.{ColumnarBroadcastExchangeExec, ColumnarFilterExec, ColumnarProjectExec, ColumnarTakeOrderedAndProjectExec, CommandResultExec, LeafExecNode, OmniColumnarToRowExec, ProjectExec, RowToOmniColumnarExec, SparkPlan, TakeOrderedAndProjectExec, UnaryExecNode}
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
    assert(columnarDataWrite.isDefined, "use columnar data writing command")
  }

  test("Insert of decimal 128") {
    val drop = spark.sql("drop table if exists table_for_decimal_128")
    drop.collect()
    val createTable = spark.sql("create table table_for_decimal_128 " +
      "(amount DECIMAL(38,18)) using orc")
    createTable.collect()

    val insert = spark.sql("insert into table_for_decimal_128 " +
      "values(529314109398732268.884038357697864858)")
    insert.collect()
    val select = spark.sql("select * from table_for_decimal_128")
    val runRows = select.collect()
    assert(runRows(0).getDecimal(0).toString ==
      "529314109398732268.884038357697864858", "the run value is error")
  }

  test("replace child plan to columnar") {
    val drop = spark.sql("drop table if exists test_parquet_int")
    drop.collect()
    val createTable = spark.sql("create table test_parquet_int(id int, age int) using parquet")
    createTable.collect()
    val insert = spark.sql("insert into table test_parquet_int " +
      "values(1,28),(2,29),(3,27),(4,26),(5,28),(6,31)")
    insert.collect()

    val dropNew = spark.sql("drop table if exists test_parquet_int_new")
    dropNew.collect()
    val createTableNew = spark.sql("create table test_parquet_int_new(id int, age int) " +
      "using parquet")
    createTableNew.collect()

    val insertNew = spark.sql("insert into table  test_parquet_int_new select " +
      "* from test_parquet_int where id > 0")

    val columnarFilter = insertNew.queryExecution.executedPlan.asInstanceOf[CommandResultExec]
      .commandPhysicalPlan.find({
        case _: ColumnarFilterExec => true
        case _ => false
      }
      )
    assert(columnarFilter.isDefined, "use columnar data writing command")
  }

  test("rebase date to julian") {
    val drop = spark.sql("drop table if exists test_orc_date")
    drop.collect()
    val createTable = spark.sql("create table test_orc_date(date_col date) using orc")
    createTable.collect()
    val insert = spark.sql("insert into table test_orc_date values(cast('1001-01-04' as date))")
    insert.collect()

    val select = spark.sql("select * from test_orc_date")
    val runRows = select.collect()
    assert(runRows(0).getDate(0).toString ==
      "1001-01-04", "the run value is error")
  }

  test("empty string partition") {
    val drop = spark.sql("drop table if exists table_insert_varchar")
    drop.collect()
    val createTable = spark.sql("create table table_insert_varchar" +
      "(id int, c_varchar varchar(40)) using orc partitioned by (p_varchar varchar(40))")
    createTable.collect()
    val insert = spark.sql("insert into table table_insert_varchar values" +
      "(5,'',''), (13,'6884578', null),  (6,'72135', '666')")
    insert.collect()

    val select = spark.sql("select * from table_insert_varchar order by id, c_varchar, p_varchar")
    val runRows = select.collect()
    val expectedRows = Seq(Row(5, "", null), Row(6, "72135", "666"), Row(13, "6884578", null))
    assert(QueryTest.sameRows(runRows, expectedRows).isEmpty, "the run value is error")

    val dropNP = spark.sql("drop table if exists table_insert_varchar_np")
    dropNP.collect()
    val createTableNP = spark.sql("create table table_insert_varchar_np" +
      "(id int, c_varchar varchar(40)) using orc partitioned by " +
      "(p_varchar1 int, p_varchar2 varchar(40), p_varchar3 varchar(40))")
    createTableNP.collect()
    val insertNP = spark.sql("insert into table table_insert_varchar_np values" +
      "(5,'',1,'',''), (13,'6884578',6, null, null), (1,'abc',1,'',''), " +
      "(3,'abcde',6,null,null), (4,'qqqqq', 8, 'a', 'b'), (6,'ooooo', 8, 'a', 'b')")
    val selectNP = spark.sql("select * from table_insert_varchar_np " +
      "order by id, c_varchar, p_varchar1")
    val runRowsNP = selectNP.collect()
    val expectedRowsNP = Seq(Row(1, "abc", 1, null, null), Row(3, "abcde", 6, null, null),
      Row(4, "qqqqq", 8, "a", "b"), Row(5, "", 1, null, null), Row(6, "ooooo", 8, "a", "b"),
      Row(13, "6884578", 6, null, null))
    assert(QueryTest.sameRows(runRowsNP, expectedRowsNP).isEmpty, "the run value is error")
  }
}
