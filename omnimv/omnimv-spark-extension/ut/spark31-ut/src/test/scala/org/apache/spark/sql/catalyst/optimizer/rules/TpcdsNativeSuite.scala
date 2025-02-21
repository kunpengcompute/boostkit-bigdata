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

package org.apache.spark.sql.catalyst.optimizer.rules

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog


class TpcdsNativeSuite extends AnyFunSuite {
  lazy val spark_native: SparkSession = SparkSession.builder().master("local")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.ui.port", "4051")
      //      .config("spark.sql.planChangeLog.level", "WARN")
      .config("spark.sql.omnimv.logLevel", "WARN")
      .enableHiveSupport()
      .getOrCreate()
  lazy val catalog: SessionCatalog = spark_native.sessionState.catalog
  createTable()

  def createTable(): Unit = {
    if (catalog.tableExists(TableIdentifier("store_sales"))) {
      return
    }
    val ddls = TpcdsUtils.getResource("/", "tpcds_ddl.sql").split(';')
    ddls.foreach(ddl => spark_native.sql(ddl))
  }

  /**
   * Debug and run native tpcds sql
   * sqlNum: tpcds sql's number
   */
  test("Run the native tpcds sql") {
    val sqlNum = 72
    val sql = TpcdsUtils.getResource("/tpcds", s"q${sqlNum}.sql")
    val df = spark_native.sql(sql)
    val qe = df.queryExecution
    df.explain()
  }
}
