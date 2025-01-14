/*
 * Copyright (C) 2021-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.spark.hive

import java.util.Properties

import com.huawei.boostkit.spark.hive.util.HiveResourceRunner
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.SparkSession

/**
 * @since 2021/12/15
 */
class HiveResourceSuite extends SparkFunSuite {
  private val QUERY_SQLS = "query-sqls"
  private var spark: SparkSession = _
  private var runner: HiveResourceRunner = _

  override def beforeAll(): Unit = {
    val properties = new Properties()
    properties.load(this.getClass.getClassLoader.getResourceAsStream("HiveResource.properties"))

    spark = SparkSession.builder()
      .appName("test-sql-context")
      .master("local[2]")
      .config(readConf(properties))
      .enableHiveSupport()
      .getOrCreate()
    LogManager.getRootLogger.setLevel(Level.WARN)
    runner = new HiveResourceRunner(spark, QUERY_SQLS)

    val hiveDb = properties.getProperty("hive.db")
    spark.sql(if (hiveDb == null) "use default" else s"use $hiveDb")
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("queryBySparkSql-HiveDataSource") {
    runner.runQuery("q1", 1)
    runner.runQuery("q2", 1)
    runner.runQuery("q3", 1)
    runner.runQuery("q4", 1)
    runner.runQuery("q5", 1)
    runner.runQuery("q6", 1)
    runner.runQuery("q7", 1)
    runner.runQuery("q8", 1)
    runner.runQuery("q9", 1)
    runner.runQuery("q10", 1)
    runner.runQuery("q11", 1)
    runner.runQuery("q12", 1)
    runner.runQuery("q13", 1)
    runner.runQuery("q14a", 1)
    runner.runQuery("q14b", 1)
    runner.runQuery("q15", 1)
    runner.runQuery("q16", 1)
    runner.runQuery("q17", 1)
    runner.runQuery("q18", 1)
    runner.runQuery("q19", 1)
    runner.runQuery("q20", 1)
    runner.runQuery("q21", 1)
    runner.runQuery("q22", 1)
    runner.runQuery("q23a", 1)
    runner.runQuery("q23b", 1)
    runner.runQuery("q24a", 1)
    runner.runQuery("q24b", 1)
    runner.runQuery("q25", 1)
    runner.runQuery("q26", 1)
    runner.runQuery("q27", 1)
    runner.runQuery("q28", 1)
    runner.runQuery("q29", 1)
    runner.runQuery("q30", 1)
    runner.runQuery("q31", 1)
    runner.runQuery("q32", 1)
    runner.runQuery("q33", 1)
    runner.runQuery("q34", 1)
    runner.runQuery("q35", 1)
    runner.runQuery("q36", 1)
    runner.runQuery("q37", 1)
    runner.runQuery("q38", 1)
    runner.runQuery("q39a", 1)
    runner.runQuery("q39b", 1)
    runner.runQuery("q40", 1)
    runner.runQuery("q41", 1)
    runner.runQuery("q42", 1)
    runner.runQuery("q43", 1)
    runner.runQuery("q44", 1)
    runner.runQuery("q45", 1)
    runner.runQuery("q46", 1)
    runner.runQuery("q47", 1)
    runner.runQuery("q48", 1)
    runner.runQuery("q49", 1)
    runner.runQuery("q50", 1)
    runner.runQuery("q51", 1)
    runner.runQuery("q52", 1)
    runner.runQuery("q53", 1)
    runner.runQuery("q54", 1)
    runner.runQuery("q55", 1)
    runner.runQuery("q56", 1)
    runner.runQuery("q57", 1)
    runner.runQuery("q58", 1)
    runner.runQuery("q59", 1)
    runner.runQuery("q60", 1)
    runner.runQuery("q61", 1)
    runner.runQuery("q62", 1)
    runner.runQuery("q63", 1)
    runner.runQuery("q64", 1)
    runner.runQuery("q65", 1)
    runner.runQuery("q66", 1)
    runner.runQuery("q67", 1)
    runner.runQuery("q68", 1)
    runner.runQuery("q69", 1)
    runner.runQuery("q70", 1)
    runner.runQuery("q71", 1)
    runner.runQuery("q72", 1)
    runner.runQuery("q73", 1)
    runner.runQuery("q74", 1)
    runner.runQuery("q75", 1)
    runner.runQuery("q76", 1)
    runner.runQuery("q77", 1)
    runner.runQuery("q78", 1)
    runner.runQuery("q79", 1)
    runner.runQuery("q80", 1)
    runner.runQuery("q81", 1)
    runner.runQuery("q82", 1)
    runner.runQuery("q83", 1)
    runner.runQuery("q84", 1)
    runner.runQuery("q85", 1)
    runner.runQuery("q86", 1)
    runner.runQuery("q87", 1)
    runner.runQuery("q88", 1)
    runner.runQuery("q89", 1)
    runner.runQuery("q90", 1)
    runner.runQuery("q91", 1)
    runner.runQuery("q92", 1)
    runner.runQuery("q93", 1)
    runner.runQuery("q94", 1)
    runner.runQuery("q95", 1)
    runner.runQuery("q96", 1)
    runner.runQuery("q97", 1)
    runner.runQuery("q98", 1)
    runner.runQuery("q99", 1)
  }

  def readConf(properties: Properties): SparkConf = {
    val conf = new SparkConf()
    val wholeStage = properties.getProperty("spark.sql.codegen.wholeStage")
    val offHeapSize = properties.getProperty("spark.memory.offHeap.size")
    conf.set("hive.metastore.uris", properties.getProperty("hive.metastore.uris"))
      .set("spark.sql.warehouse.dir", properties.getProperty("spark.sql.warehouse.dir"))
      .set("spark.memory.offHeap.size", if (offHeapSize == null) "8G" else offHeapSize)
      .set("spark.sql.codegen.wholeStage", if (wholeStage == null) "false" else wholeStage)
      .set("spark.sql.extensions", properties.getProperty("spark.sql.extensions"))
      .set("spark.shuffle.manager", properties.getProperty("spark.shuffle.manager"))
      .set("spark.sql.orc.impl", properties.getProperty("spark.sql.orc.impl"))
  }
}
