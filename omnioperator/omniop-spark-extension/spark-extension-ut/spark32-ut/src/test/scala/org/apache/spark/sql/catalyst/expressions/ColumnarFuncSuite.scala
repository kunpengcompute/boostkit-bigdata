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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, ConvertToLocalRelation, NullPropagation}
import org.apache.spark.sql.execution.{ColumnarProjectExec, ColumnarSparkPlanTest, ProjectExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.{DataFrame, Row}

class ColumnarFuncSuite extends ColumnarSparkPlanTest {
  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}
  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName + "," + ConstantFolding.ruleName +
      "," + NullPropagation.ruleName)
  protected override def beforeAll(): Unit = {
    super.beforeAll()

    Seq[(java.lang.Integer, java.lang.Long, java.lang.Double, java.lang.Boolean, String, Decimal)](
      (123, 123L, 123.0, true, "123", Decimal(123.00)),
      (null, null, null, null, null, null),
      (0, 0L, 0.0, false, "", Decimal(0)),
      (50, 50L, 50.0, true, "50", Decimal(50.00))
    ).toDF("int_column", "long_column", "double_column", "bool_column", "str_column", "decimal_column")
      .createOrReplaceTempView("greatest_view")
  }

  test("Test Greatest Function") {
    val intRes = spark.sql("select greatest(int_column, 50), greatest(null, int_column) " +
      "from greatest_view")
    assertOmniProjectHappened(intRes)
    checkAnswer(intRes, Seq(Row(123, 123), Row(50, null), Row(50, 0), Row(50, 50)))

    val longRes = spark.sql("select greatest(long_column, 50L), greatest(null, long_column) " +
      "from greatest_view")
    assertOmniProjectHappened(longRes)
    checkAnswer(longRes, Seq(Row(123L, 123L), Row(50L, null), Row(50L, 0L), Row(50L, 50L)))

    val doubleRes = spark.sql("select greatest(double_column, 50.0), greatest(null, double_column) " +
      "from greatest_view")
    assertOmniProjectHappened(doubleRes)
    checkAnswer(doubleRes, Seq(Row(123.0, 123.0), Row(50.0, null), Row(50.0, 0), Row(50.0, 50.0)))

    val boolRes = spark.sql("select greatest(bool_column, false), greatest(null, bool_column) " +
      "from greatest_view")
    assertOmniProjectHappened(boolRes)
    checkAnswer(boolRes, Seq(Row(true, true), Row(false, null), Row(false, false), Row(true, true)))

    val strRes = spark.sql("select greatest(str_column, '40'), greatest(null, str_column) " +
      "from greatest_view")
    assertOmniProjectHappened(strRes)
    checkAnswer(strRes, Seq(Row("40", "123"), Row("40", null), Row("40", ""), Row("50", "50")))

    val decimalRes = spark.sql("select greatest(cast(decimal_column as decimal(10, 2)), " +
      "cast(50.00 as decimal(10, 2))), greatest(cast(decimal_column as decimal(10, 2)), cast(50.00 as decimal(38, 2))), " +
      "greatest(null, cast(decimal_column as decimal(10, 2))) from greatest_view")
    if (BigDecimal(123.00) == Decimal(123.00, 10, 2).toBigDecimal) {

    }
    assertOmniProjectHappened(decimalRes)
    checkAnswer(decimalRes, Seq(
      Row(Decimal(123.00, 10, 2).toBigDecimal, Decimal(123.00, 38, 2).toBigDecimal, Decimal(123.00, 10, 2).toBigDecimal),
      Row(Decimal(50.00, 10, 2).toBigDecimal, Decimal(50.00, 38, 2).toBigDecimal, null),
      Row(Decimal(50.00, 10, 2).toBigDecimal, Decimal(50.00, 38, 2).toBigDecimal, Decimal(0, 10, 2).toBigDecimal),
      Row(Decimal(50.00, 10, 2).toBigDecimal, Decimal(50.00, 38, 2).toBigDecimal, Decimal(50.00, 10, 2).toBigDecimal)
    ))

    var rollbackRes = spark.sql("select greatest(1, 2, 3)")
    assertOmniProjectNotHappened(rollbackRes)
  }

  test("Test Unix_timestamp Function") {
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ConstantFolding")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Shanghai")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    val res1 = spark.sql("select unix_timestamp('','yyyy-MM-dd'), unix_timestamp('123-abc', " +
      "'yyyy-MM-dd HH:mm:ss'), unix_timestamp(NULL, 'yyyy-MM-dd')")
    assertOmniProjectHappened(res1)
    checkAnswer(res1, Seq(Row(null, null, null)))

    val res2 = spark.sql("select unix_timestamp('2024-10-21', 'yyyy-MM-dd'), " +
      "unix_timestamp('2024-10-21 11:22:33', 'yyyy-MM-dd HH:mm:ss')")
    assertOmniProjectHappened(res2)
    checkAnswer(res2, Seq(Row(1729440000L, 1729480953L)))

    val res3 = spark.sql("select unix_timestamp('1986-08-10 05:05:05','yyyy-MM-dd HH:mm:ss')")
    assertOmniProjectHappened(res3)
    checkAnswer(res3, Seq(Row(524001905L)))

    val res4 = spark.sql("select unix_timestamp('2086-08-10 05:05:05','yyyy-MM-dd HH:mm:ss')")
    assertOmniProjectHappened(res4)
    checkAnswer(res4, Seq(Row(3679765505L)))
  }

  test("Test from_unixtime Function") {
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ConstantFolding")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Shanghai")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    val res1 = spark.sql("select from_unixtime(1, 'yyyy-MM-dd HH:mm:ss'), from_unixtime(1, 'yyyy-MM-dd')")
    assertOmniProjectHappened(res1)
    checkAnswer(res1, Seq(Row("1970-01-01 08:00:01", "1970-01-01")))

    val res2 = spark.sql("select from_unixtime(524001905, 'yyyy-MM-dd HH:mm:ss'), from_unixtime(524001905, 'yyyy-MM-dd')")
    assertOmniProjectHappened(res2)
    checkAnswer(res2, Seq(Row("1986-08-10 05:05:05", "1986-08-10")))

    val res3 = spark.sql("select from_unixtime(3679765505, 'yyyy-MM-dd HH:mm:ss'), from_unixtime(3679765505, 'yyyy-MM-dd')")
    assertOmniProjectHappened(res3)
    checkAnswer(res3, Seq(Row("2086-08-10 05:05:05", "2086-08-10")))
  }

  private def assertOmniProjectHappened(res: DataFrame) = {
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
  }

  private def assertOmniProjectNotHappened(res: DataFrame) = {
    val executedPlan = res.queryExecution.executedPlan
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isEmpty, s"ColumnarProjectExec happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isDefined, s"ProjectExec not happened, executedPlan as follows： \n$executedPlan")
  }
}
