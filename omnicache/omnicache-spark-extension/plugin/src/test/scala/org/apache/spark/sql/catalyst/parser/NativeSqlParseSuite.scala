/*
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

package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.optimizer.rules.RewriteSuite
import org.apache.spark.sql.catalyst.optimizer.rules.RewriteSuite._
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}

class NativeSqlParseSuite extends RewriteSuite {

  test("create table xxx as select xxx") {
    spark.sql(
      """
        |drop table if exists insert_select1;
        |""".stripMargin)
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_insert_select1;
        |""".stripMargin)
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_insert_select1
        |AS
        |select locationid, state from locations where locationid = 1;
        |""".stripMargin)
    val df = spark.sql(
      """
        |create table insert_select1
        |as select locationid, state from locations where locationid = 1;
        |""".stripMargin)
    val optPlan = df.queryExecution.optimizedPlan
    assert(optPlan.isInstanceOf[CreateHiveTableAsSelectCommand])
    assert(isRewritedByMV("default", "mv_insert_select1",
      optPlan.asInstanceOf[CreateHiveTableAsSelectCommand].query))
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_insert_select1;
        |""".stripMargin)
  }

  test("insert overwrite xxx select xxx") {
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_insert_select2
        |AS
        |select locationid, state from locations where locationid = 2;
        |""".stripMargin)
    val df = spark.sql(
      """
        |insert overwrite insert_select1
        |select locationid, state from locations where locationid = 2;
        |""".stripMargin)
    val optPlan = df.queryExecution.optimizedPlan
    assert(optPlan.isInstanceOf[InsertIntoHiveTable])
    assert(isRewritedByMV("default", "mv_insert_select2",
      optPlan.asInstanceOf[InsertIntoHiveTable].query))
  }

  test("insert into xxx select xxx") {
    val df = spark.sql(
      """
        |insert into insert_select1
        |select locationid, state from locations where locationid = 2;
        |""".stripMargin)
    val optPlan = df.queryExecution.optimizedPlan
    assert(optPlan.isInstanceOf[InsertIntoHiveTable])
    assert(isRewritedByMV("default", "mv_insert_select2",
      optPlan.asInstanceOf[InsertIntoHiveTable].query))
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_insert_select2;
        |""".stripMargin)
  }

  test("clean") {
    spark.sql(
      """
        |drop table if exists insert_select1;
        |""".stripMargin)
  }

}
