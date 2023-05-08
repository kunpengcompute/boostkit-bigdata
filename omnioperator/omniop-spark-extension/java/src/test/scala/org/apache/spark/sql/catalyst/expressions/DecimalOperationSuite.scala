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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.execution.{ColumnarConditionProjectExec, ColumnarSparkPlanTest}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.{Column, DataFrame}

import java.math.MathContext

class DecimalOperationSuite extends ColumnarSparkPlanTest{

  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  private var deci_overflow: DataFrame = _

  private def newDecimal(deci: String, precision: Int, scale: Int): Decimal = {
    if (deci == null)
      null
    else
      new Decimal().set(BigDecimal(deci, MathContext.UNLIMITED), precision, scale)
  }

  private def newRow(id: Int, c_deci5_0: String, c_deci7_2: String, c_deci17_2: String, c_deci18_6: String,
                     c_deci21_6: String, c_deci22_6: String, c_deci38_0: String, c_deci38_16: String):
  (Int, Decimal, Decimal, Decimal, Decimal, Decimal, Decimal, Decimal, Decimal) = {
    (id,
      newDecimal(c_deci5_0, 5, 0),
      newDecimal(c_deci7_2, 7, 2),
      newDecimal(c_deci17_2, 17, 2),
      newDecimal(c_deci18_6, 18, 6),
      newDecimal(c_deci21_6, 21, 6),
      newDecimal(c_deci22_6, 22, 6),
      newDecimal(c_deci38_0, 38, 0),
      newDecimal(c_deci38_16, 38, 16))
  }

  private def checkResult(sql: String, expect: String): Unit = {
    val result = spark.sql(sql)
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarConditionProjectExec]).isDefined)
    val output = result.collect().toSeq.head.getDecimal(0)
    assertResult(expect, s"sql: ${sql}")(output.toString)
  }

  private def checkResultNull(sql: String): Unit = {
    val result = spark.sql(sql)
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarConditionProjectExec]).isDefined)
    val output = result.collect().toSeq.head.getDecimal(0)
    assertResult(null, s"sql: ${sql}")(output)
  }

  private def checkAnsiResult(sql: String, expect: String): Unit = {
    spark.conf.set("spark.sql.ansi.enabled", true)
    checkResult(sql, expect)
    spark.conf.set("spark.sql.ansi.enabled", false)
  }

  private def checkAnsiResultNull(sql: String): Unit = {
    spark.conf.set("spark.sql.ansi.enabled", true)
    checkResultNull(sql)
    spark.conf.set("spark.sql.ansi.enabled", false)
  }

  private def checkAnsiResultException(sql: String, msg: String): Unit = {
    spark.conf.set("spark.sql.ansi.enabled", true)
    val result = spark.sql(sql)
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarConditionProjectExec]).isDefined)
    val exception = intercept[Exception](
      result.collect().toSeq.head.getDecimal(0)
    )
    assert(exception.getMessage.contains(msg), s"sql: ${sql}")
    spark.conf.set("spark.sql.ansi.enabled", false)
  }

  private def checkAnsiResultOverflowException(sql: String): Unit =
    checkAnsiResultException(sql, "Reason: Decimal overflow")

  private def checkAnsiResultDivideBy0Exception(sql: String): Unit =
    checkAnsiResultException(sql, "Reason: Division by zero")


  override def beforeAll(): Unit = {
    super.beforeAll()

    deci_overflow = Seq[(Int, Decimal, Decimal, Decimal, Decimal,
      Decimal, Decimal, Decimal, Decimal)](
      newRow(1, "12345", "12345.12", "123456789123456.23", "123456789123.34",
        "123456789123456.456789", "1234567891234567.567891",
        "123456789123456789123456789", "1234567891234567891234.6789123456"),
      newRow(2, "99999", "99999.99", "999999999999999.99", "999999999999.999999",
        "-999999999999999.999999", "9999999999999999.999999",
        "99999999999999999999999999999999999999", "9999999999999999999999.9999999999999999"),
      newRow(3, "99999", "0.99", "0.99", "0.999999",
        "0.999999", "9999999999999999.999999",
        "99999999999999999999999999999999999999", "-9999999999999999999999.9999999999999999"),
      newRow(4, "99999", "0", "0.99", "0.999999",
        "0", "9999999999999999.999999",
        "99999999999999999999999999999999999999", "0"),
      newRow(5, "99999", null, "0.99", "0.999999",
        null, "0.999999",
        "99999999999999999999999999999999999999", null),
      newRow(6, "-12345", "12345.12", "-123456789123456.23", "123456789123.34",
        "-123456789123456.456789", "1234567891234567.567891",
        "123456789123456789123456789", "-1234567891234567891234.6789123456"),
    ).toDF("id", "c_deci5_0", "c_deci7_2", "c_deci17_2", "c_deci18_6", "c_deci21_6",
      "c_deci22_6", "c_deci38_0", "c_deci38_16")

    // Decimal in DataFrame is decimal(38,16), so need to cast to the target decimal type
    deci_overflow = deci_overflow.withColumn("c_deci5_0", Column("c_deci5_0").cast("decimal(5,0)"))
      .withColumn("c_deci7_2", Column("c_deci7_2").cast("decimal(7,2)"))
      .withColumn("c_deci17_2", Column("c_deci17_2").cast("decimal(17,2)"))
      .withColumn("c_deci18_6", Column("c_deci18_6").cast("decimal(18,6)"))
      .withColumn("c_deci21_6", Column("c_deci21_6").cast("decimal(21,6)"))
      .withColumn("c_deci22_6", Column("c_deci22_6").cast("decimal(22,6)"))
      .withColumn("c_deci38_0", Column("c_deci38_0").cast("decimal(38,0)"))
      .withColumn("c_deci38_16", Column("c_deci38_16").cast("decimal(38,16)"))

    deci_overflow.createOrReplaceTempView("deci_overflow")
    deci_overflow.printSchema()
  }

  /* normal positive decimal operation test cases */
  // spark.sql.ansi.enabled=false
  test("decimal64+decimal64=decimal64 positive decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci5_0+c_deci7_2 from deci_overflow where id = 1;", "24690.12")
  }

  test("decimal64+decimal64=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci17_2+c_deci18_6 from deci_overflow where id = 1;", "123580245912579.570000")
  }

  test("decimal64+decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci17_2+c_deci22_6 from deci_overflow where id = 1;", "1358024680358023.797891")
  }

  test("decimal128+decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci21_6+c_deci22_6 from deci_overflow where id = 1;", "1358024680358024.024680")
  }

  test("decimal64-decimal64=decimal64 positive decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci7_2-c_deci5_0 from deci_overflow where id = 1;", "0.12")
  }

  test("decimal64-decimal64=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci18_6-c_deci17_2 from deci_overflow where id = 1;", "-123333332334332.890000")
  }

  test("decimal64-decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci22_6-c_deci17_2 from deci_overflow where id = 1;", "1111111102111111.337891")
  }

  test("decimal128-decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci22_6-c_deci21_6 from deci_overflow where id = 1;", "1111111102111111.111102")
  }

  test("decimal64*decimal64=decimal64 positive decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci5_0*c_deci7_2 from deci_overflow where id = 1;", "152400506.40")
  }

  test("decimal64*decimal64=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci17_2*c_deci18_6 from deci_overflow where id = 1;",
      "15241578780659191108332561.40820000")
  }

  test("decimal64*decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci17_2*c_deci22_6 from deci_overflow where id = 1;",
      "152415787806736055266232119611.091911")
  }

  test("decimal128*decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci21_6*c_deci22_6 from deci_overflow where id = 1;",
      "152415787806736335252649604807.436065")
  }

  test("decimal64/decimal64=decimal64 positive decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci5_0/c_deci7_2 from deci_overflow where id = 1;", "0.99999028")
  }

  test("decimal64/decimal64=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci17_2/c_deci18_6 from deci_overflow where id = 1;",
      "1000.00000000094146301")
  }

  test("decimal64/decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci17_2/c_deci22_6 from deci_overflow where id = 1;",
      "0.09999999999999957")
  }

  test("decimal128/decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci22_6/c_deci21_6 from deci_overflow where id = 1;",
      "10.0000000000000243")
  }

  test("decimal64%decimal64=decimal64 positive decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci5_0%c_deci7_2 from deci_overflow where id = 1;", "12345.00")
  }

  test("decimal64%decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci22_6%c_deci17_2 from deci_overflow where id = 1;", "5.267891")
  }

  test("decimal128%decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci22_6%c_deci21_6 from deci_overflow where id = 1;", "3.000001")
  }


  // spark.sql.ansi.enabled=true
  test("decimal64+decimal64=decimal64 positive decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci5_0+c_deci7_2 from deci_overflow where id = 1;", "24690.12")
  }

  test("decimal64+decimal64=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci17_2+c_deci18_6 from deci_overflow where id = 1;", "123580245912579.570000")
  }

  test("decimal64+decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci17_2+c_deci22_6 from deci_overflow where id = 1;", "1358024680358023.797891")
  }

  test("decimal128+decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci21_6+c_deci22_6 from deci_overflow where id = 1;", "1358024680358024.024680")
  }

  test("decimal64-decimal64=decimal64 positive decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci7_2-c_deci5_0 from deci_overflow where id = 1;", "0.12")
  }

  test("decimal64-decimal64=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci18_6-c_deci17_2 from deci_overflow where id = 1;", "-123333332334332.890000")
  }

  test("decimal64-decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci22_6-c_deci17_2 from deci_overflow where id = 1;", "1111111102111111.337891")
  }

  test("decimal128-decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci22_6-c_deci21_6 from deci_overflow where id = 1;", "1111111102111111.111102")
  }

  test("decimal64*decimal64=decimal64 positive decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci5_0*c_deci7_2 from deci_overflow where id = 1;", "152400506.40")
  }

  test("decimal64*decimal64=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci17_2*c_deci18_6 from deci_overflow where id = 1;",
      "15241578780659191108332561.40820000")
  }

  test("decimal64*decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci17_2*c_deci22_6 from deci_overflow where id = 1;",
      "152415787806736055266232119611.091911")
  }

  test("decimal128*decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci21_6*c_deci22_6 from deci_overflow where id = 1;",
      "152415787806736335252649604807.436065")
  }

  test("decimal64/decimal64=decimal64 positive decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci5_0/c_deci7_2 from deci_overflow where id = 1;", "0.99999028")
  }

  test("decimal64/decimal64=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci17_2/c_deci18_6 from deci_overflow where id = 1;",
      "1000.00000000094146301")
  }

  test("decimal64/decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci17_2/c_deci22_6 from deci_overflow where id = 1;",
      "0.09999999999999957")
  }

  test("decimal128/decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci22_6/c_deci21_6 from deci_overflow where id = 1;",
      "10.0000000000000243")
  }

  test("decimal64%decimal64=decimal64 positive decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci5_0%c_deci7_2 from deci_overflow where id = 1;", "12345.00")
  }

  test("decimal64%decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci22_6%c_deci17_2 from deci_overflow where id = 1;", "5.267891")
  }

  test("decimal128%decimal128=decimal128 positive decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci22_6%c_deci21_6 from deci_overflow where id = 1;", "3.000001")
  }


  /* normal negative decimal operation test cases */
  // spark.sql.ansi.enabled=false
  test("decimal64+decimal64=decimal64 negative decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci5_0+c_deci7_2 from deci_overflow where id = 6;", "0.12")
  }

  test("decimal64+decimal64=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci17_2+c_deci18_6 from deci_overflow where id = 6;", "-123333332334332.890000")
  }

  test("decimal64+decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci17_2+c_deci22_6 from deci_overflow where id = 6;", "1111111102111111.337891")
  }

  test("decimal128+decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci21_6+c_deci22_6 from deci_overflow where id = 6;", "1111111102111111.111102")
  }

  test("decimal64-decimal64=decimal64 negative decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci7_2-c_deci5_0 from deci_overflow where id = 6;", "24690.12")
  }

  test("decimal64-decimal64=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci18_6-c_deci17_2 from deci_overflow where id = 6;", "123580245912579.570000")
  }

  test("decimal64-decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci22_6-c_deci17_2 from deci_overflow where id = 6;", "1358024680358023.797891")
  }

  test("decimal128-decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci22_6-c_deci21_6 from deci_overflow where id = 6;", "1358024680358024.024680")
  }

  test("decimal64*decimal64=decimal64 negative decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci5_0*c_deci7_2 from deci_overflow where id = 6;", "-152400506.40")
  }

  test("decimal64*decimal64=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci17_2*c_deci18_6 from deci_overflow where id = 6;",
      "-15241578780659191108332561.40820000")
  }

  test("decimal64*decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci17_2*c_deci22_6 from deci_overflow where id = 6;",
      "-152415787806736055266232119611.091911")
  }

  test("decimal128*decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci21_6*c_deci22_6 from deci_overflow where id = 6;",
      "-152415787806736335252649604807.436065")
  }

  test("decimal64/decimal64=decimal64 negative decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci5_0/c_deci7_2 from deci_overflow where id = 6;", "-0.99999028")
  }

  test("decimal64/decimal64=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci17_2/c_deci18_6 from deci_overflow where id = 6;",
      "-1000.00000000094146301")
  }

  test("decimal64/decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci17_2/c_deci22_6 from deci_overflow where id = 6;",
      "-0.09999999999999957")
  }

  test("decimal128/decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci22_6/c_deci21_6 from deci_overflow where id = 6;",
      "-10.0000000000000243")
  }

  test("decimal64%decimal64=decimal64 negative decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci5_0%c_deci7_2 from deci_overflow where id = 6;", "-12345.00")
  }

  test("decimal64%decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci22_6%c_deci17_2 from deci_overflow where id = 6;", "5.267891")
  }

  test("decimal128%decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci21_6%c_deci22_6 from deci_overflow where id = 6;", "-123456789123456.456789")
  }

  // spark.sql.ansi.enabled=false
  test("decimal64+decimal64=decimal64 negative decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci5_0+c_deci7_2 from deci_overflow where id = 6;", "0.12")
  }

  test("decimal64+decimal64=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci17_2+c_deci18_6 from deci_overflow where id = 6;", "-123333332334332.890000")
  }

  test("decimal64+decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci17_2+c_deci22_6 from deci_overflow where id = 6;", "1111111102111111.337891")
  }

  test("decimal128+decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci21_6+c_deci22_6 from deci_overflow where id = 6;", "1111111102111111.111102")
  }

  test("decimal64-decimal64=decimal64 negative decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci7_2-c_deci5_0 from deci_overflow where id = 6;", "24690.12")
  }

  test("decimal64-decimal64=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci18_6-c_deci17_2 from deci_overflow where id = 6;", "123580245912579.570000")
  }

  test("decimal64-decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci22_6-c_deci17_2 from deci_overflow where id = 6;", "1358024680358023.797891")
  }

  test("decimal128-decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci22_6-c_deci21_6 from deci_overflow where id = 6;", "1358024680358024.024680")
  }

  test("decimal64*decimal64=decimal64 negative decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci5_0*c_deci7_2 from deci_overflow where id = 6;", "-152400506.40")
  }

  test("decimal64*decimal64=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci17_2*c_deci18_6 from deci_overflow where id = 6;",
      "-15241578780659191108332561.40820000")
  }

  test("decimal64*decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci17_2*c_deci22_6 from deci_overflow where id = 6;",
      "-152415787806736055266232119611.091911")
  }

  test("decimal128*decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci21_6*c_deci22_6 from deci_overflow where id = 6;",
      "-152415787806736335252649604807.436065")
  }

  test("decimal64/decimal64=decimal64 negative decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci5_0/c_deci7_2 from deci_overflow where id = 6;", "-0.99999028")
  }

  test("decimal64/decimal64=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci17_2/c_deci18_6 from deci_overflow where id = 6;",
      "-1000.00000000094146301")
  }

  test("decimal64/decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci17_2/c_deci22_6 from deci_overflow where id = 6;",
      "-0.09999999999999957")
  }

  test("decimal128/decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci22_6/c_deci21_6 from deci_overflow where id = 6;",
      "-10.0000000000000243")
  }

  test("decimal64%decimal64=decimal64 negative decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci5_0%c_deci7_2 from deci_overflow where id = 6;", "-12345.00")
  }

  test("decimal64%decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci22_6%c_deci17_2 from deci_overflow where id = 6;", "5.267891")
  }

  test("decimal128%decimal128=decimal128 negative decimal operation normal when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci21_6%c_deci22_6 from deci_overflow where id = 6;", "-123456789123456.456789")
  }

  /* overflow decimal operation test cases */
  // spark.sql.ansi.enabled=false
  test("decimal add operation positive overflow when spark.sql.ansi.enabled=false") {
    checkResultNull("select (c_deci22_6*c_deci22_6)+c_deci18_6 from deci_overflow where id = 2;")
  }

  test("decimal add operation negative overflow when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci21_6+(0-c_deci22_6*c_deci22_6) from deci_overflow where id = 2;")
  }

  test("decimal subtract operation positive overflow when spark.sql.ansi.enabled=false") {
    checkResultNull("select (c_deci22_6*c_deci22_6)-c_deci21_6 from deci_overflow where id = 2;")
  }

  test("decimal subtract operation negative overflow when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci21_6-(c_deci22_6*c_deci22_6) from deci_overflow where id = 2;")
  }

  test("decimal multiple operation positive overflow when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci22_6*c_deci22_6*c_deci5_0 from deci_overflow where id = 2;")
  }

  test("decimal multiple operation negative overflow when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci21_6*c_deci22_6*c_deci22_6 from deci_overflow where id = 2;")
  }

  test("decimal divide operation positive overflow when spark.sql.ansi.enabled=false") {
    checkResultNull("select (c_deci22_6*c_deci22_6)/c_deci7_2 from deci_overflow where id = 3;")
  }

  test("decimal divide operation negative overflow when spark.sql.ansi.enabled=false") {
    checkResultNull("select (0-c_deci22_6*c_deci22_6)/c_deci7_2 from deci_overflow where id = 3;")
  }

  // spark.sql.ansi.enabled=true
  test("decimal add operation positive overflow when spark.sql.ansi.enabled=true") {
    checkAnsiResultOverflowException("select (c_deci22_6*c_deci22_6)+c_deci18_6 from deci_overflow where id = 2;")
  }

  test("decimal add operation negative overflow when spark.sql.ansi.enabled=true") {
    checkAnsiResultOverflowException("select c_deci21_6+(0-c_deci22_6*c_deci22_6) from deci_overflow where id = 2;")
  }

  test("decimal subtract operation positive overflow when spark.sql.ansi.enabled=true") {
    checkAnsiResultOverflowException("select (c_deci22_6*c_deci22_6)-c_deci21_6 from deci_overflow where id = 2;")
  }

  test("decimal subtract operation negative overflow when spark.sql.ansi.enabled=true") {
    checkAnsiResultOverflowException("select c_deci21_6-(c_deci22_6*c_deci22_6) from deci_overflow where id = 2;")
  }

  test("decimal multiple operation positive overflow when spark.sql.ansi.enabled=true") {
    checkAnsiResultOverflowException("select c_deci22_6*c_deci22_6*c_deci5_0 from deci_overflow where id = 2;")
  }

  test("decimal multiple operation negative overflow when spark.sql.ansi.enabled=true") {
    checkAnsiResultOverflowException("select c_deci21_6*c_deci22_6*c_deci22_6 from deci_overflow where id = 2;")
  }

  test("decimal divide operation positive overflow when spark.sql.ansi.enabled=true") {
    checkAnsiResultOverflowException("select (c_deci22_6*c_deci22_6)/c_deci7_2 from deci_overflow where id = 3;")
  }

  test("decimal divide operation negative overflow when spark.sql.ansi.enabled=true") {
    checkAnsiResultOverflowException("select (0-c_deci22_6*c_deci22_6)/c_deci7_2 from deci_overflow where id = 3;")
  }

  /* divide by zero decimal operation test cases */
  // spark.sql.ansi.enabled=false
  test("decimal64/decimal64(0) when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci5_0/c_deci7_2 from deci_overflow where id = 4")
  }

  test("decimal64/decimal128(0) when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci5_0/c_deci21_6 from deci_overflow where id = 4")
  }

  test("decimal128/decimal64(0) when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci22_6/c_deci7_2 from deci_overflow where id = 4")
  }

  test("decimal128/decimal128(0) when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci22_6/c_deci21_6 from deci_overflow where id = 4")
  }

  test("decimal64/literal(0) when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci17_2/0 from deci_overflow where id = 4")
  }

  test("decimal128/literal(0) when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci22_6/0 from deci_overflow where id = 4")
  }

  test("decimal64%decimal64(0) when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci5_0%c_deci7_2 from deci_overflow where id = 4")
  }

  test("decimal128%decimal64(0) when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci22_6%c_deci7_2 from deci_overflow where id = 4")
  }

  test("decimal64%decimal128(0) when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci18_6%c_deci21_6 from deci_overflow where id = 4")
  }

  test("decimal128%decimal128(0) when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci22_6%c_deci21_6 from deci_overflow where id = 4")
  }

  test("decimal64%literal(0) when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci18_6%0 from deci_overflow where id = 4")
  }

  test("decimal128%literal(0) when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci22_6%0 from deci_overflow where id = 4")
  }

  // spark.sql.ansi.enabled=true
  test("decimal64/decimal64(0) when spark.sql.ansi.enabled=true") {
    checkAnsiResultDivideBy0Exception("select c_deci5_0/c_deci7_2 from deci_overflow where id = 4")
  }

  test("decimal64/decimal128(0) when spark.sql.ansi.enabled=true") {
    checkAnsiResultDivideBy0Exception("select c_deci5_0/c_deci21_6 from deci_overflow where id = 4")
  }

  test("decimal128/decimal64(0) when spark.sql.ansi.enabled=true") {
    checkAnsiResultDivideBy0Exception("select c_deci22_6/c_deci7_2 from deci_overflow where id = 4")
  }

  test("decimal128/decimal128(0) when spark.sql.ansi.enabled=true") {
    checkAnsiResultDivideBy0Exception("select c_deci22_6/c_deci21_6 from deci_overflow where id = 4")
  }

  test("decimal64/literal(0) when spark.sql.ansi.enabled=true") {
    checkAnsiResultDivideBy0Exception("select c_deci17_2/0 from deci_overflow where id = 4")
  }

  test("decimal128/literal(0) when spark.sql.ansi.enabled=true") {
    checkAnsiResultDivideBy0Exception("select c_deci22_6/0 from deci_overflow where id = 4")
  }

  test("decimal64%decimal64(0) when spark.sql.ansi.enabled=true") {
    checkAnsiResultDivideBy0Exception("select c_deci5_0%c_deci7_2 from deci_overflow where id = 4")
  }

  test("decimal128%decimal64(0) when spark.sql.ansi.enabled=true") {
    checkAnsiResultDivideBy0Exception("select c_deci22_6%c_deci7_2 from deci_overflow where id = 4")
  }

  test("decimal64%decimal128(0) when spark.sql.ansi.enabled=true") {
    checkAnsiResultDivideBy0Exception("select c_deci18_6%c_deci21_6 from deci_overflow where id = 4")
  }

  test("decimal128%decimal128(0) when spark.sql.ansi.enabled=true") {
    checkAnsiResultDivideBy0Exception("select c_deci22_6%c_deci21_6 from deci_overflow where id = 4")
  }

  test("decimal64%literal(0) when spark.sql.ansi.enabled=true") {
    checkAnsiResultDivideBy0Exception("select c_deci18_6%0 from deci_overflow where id = 4")
  }

  test("decimal128%literal(0) when spark.sql.ansi.enabled=true") {
    checkAnsiResultDivideBy0Exception("select c_deci22_6%0 from deci_overflow where id = 4")
  }

  /* zero decimal operation test cases */
  // spark.sql.ansi.enabled=false
  test("decimal64+decimal64(0) when when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci5_0+c_deci7_2 from deci_overflow where id = 4;", "99999.00")
  }

  test("decimal64+decimal128(0) when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci5_0+c_deci21_6 from deci_overflow where id = 4;", "99999.000000")
  }

  test("decimal64(0)+decimal128 when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci7_2+c_deci22_6 from deci_overflow where id = 4;", "9999999999999999.999999")
  }

  test("decimal128(0)+decimal128 when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci21_6+c_deci22_6 from deci_overflow where id = 4;", "9999999999999999.999999")
  }

  test("decimal64+literal(0) when when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci5_0+0 from deci_overflow where id = 4;", "99999")
  }

  test("decimal128+literal(0) when when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci22_6+0 from deci_overflow where id = 4;", "9999999999999999.999999")
  }

  test("decimal64-decimal64(0) when when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci5_0-c_deci7_2 from deci_overflow where id = 4;", "99999.00")
  }

  test("decimal64-decimal128(0) when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci5_0-c_deci21_6 from deci_overflow where id = 4;", "99999.000000")
  }

  test("decimal64(0)-decimal128 when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci7_2-c_deci22_6 from deci_overflow where id = 4;", "-9999999999999999.999999")
  }

  test("decimal128(0)-decimal128 when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci21_6-c_deci22_6 from deci_overflow where id = 4;", "-9999999999999999.999999")
  }

  test("decimal64-literal(0) when when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci5_0-0 from deci_overflow where id = 4;", "99999")
  }

  test("literal(0)-decimal128 when when spark.sql.ansi.enabled=false") {
    checkResult("select 0-c_deci22_6 from deci_overflow where id = 4;", "-9999999999999999.999999")
  }

  test("decimal64*decimal64(0) when when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci5_0*c_deci7_2 from deci_overflow where id = 4;", "0.00")
  }

  test("decimal64*decimal128(0) when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci5_0*c_deci21_6 from deci_overflow where id = 4;", "0.000000")
  }

  test("decimal64(0)*decimal128 when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci7_2*c_deci22_6 from deci_overflow where id = 4;", "0E-8")
  }

  test("decimal128(0)*decimal128 when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci21_6*c_deci22_6 from deci_overflow where id = 4;", "0.000000")
  }

  test("decimal64*literal(0) when when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci5_0*0 from deci_overflow where id = 4;", "0")
  }

  test("literal(0)*decimal128 when when spark.sql.ansi.enabled=false") {
    checkResult("select 0*c_deci22_6 from deci_overflow where id = 4;", "0.000000")
  }

  test("decimal64(0)/decimal64 when when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci7_2/c_deci5_0 from deci_overflow where id = 4;", "0E-8")
  }

  test("decimal128(0)/decimal64 when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci21_6/c_deci5_0 from deci_overflow where id = 4;", "0E-12")
  }

  test("decimal64(0)/decimal128 when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci7_2/c_deci22_6 from deci_overflow where id = 4;", "0E-25")
  }

  test("decimal128(0)/decimal128 when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci21_6/c_deci22_6 from deci_overflow where id = 4;", "0E-17")
  }

  test("literal(0)/decimal64 when when spark.sql.ansi.enabled=false") {
    checkResult("select 0/c_deci5_0 from deci_overflow where id = 4;", "0.000000")
  }

  test("literal(0)/decimal128 when when spark.sql.ansi.enabled=false") {
    checkResult("select 0/c_deci22_6 from deci_overflow where id = 4;", "0E-23")
  }

  test("decimal64(0)%decimal64 when when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci7_2%c_deci5_0 from deci_overflow where id = 4;", "0.00")
  }

  test("decimal128(0)%decimal64 when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci21_6%c_deci5_0 from deci_overflow where id = 4;", "0.000000")
  }

  test("decimal64(0)%decimal128 when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci7_2%c_deci22_6 from deci_overflow where id = 4;", "0.000000")
  }

  test("decimal128(0)%decimal128 when spark.sql.ansi.enabled=false") {
    checkResult("select c_deci21_6%c_deci22_6 from deci_overflow where id = 4;", "0.000000")
  }

  test("literal(0)%decimal64 when when spark.sql.ansi.enabled=false") {
    checkResult("select 0%c_deci5_0 from deci_overflow where id = 4;", "0")
  }

  test("literal(0)%decimal128 when when spark.sql.ansi.enabled=false") {
    checkResult("select 0%c_deci22_6 from deci_overflow where id = 4;", "0.000000")
  }

  // spark.sql.ansi.enabled=true
  test("decimal64+decimal64(0) when when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci5_0+c_deci7_2 from deci_overflow where id = 4;", "99999.00")
  }

  test("decimal64+decimal128(0) when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci5_0+c_deci21_6 from deci_overflow where id = 4;", "99999.000000")
  }

  test("decimal64(0)+decimal128 when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci7_2+c_deci22_6 from deci_overflow where id = 4;", "9999999999999999.999999")
  }

  test("decimal128(0)+decimal128 when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci21_6+c_deci22_6 from deci_overflow where id = 4;", "9999999999999999.999999")
  }

  test("decimal64+literal(0) when when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci5_0+0 from deci_overflow where id = 4;", "99999")
  }

  test("decimal128+literal(0) when when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci22_6+0 from deci_overflow where id = 4;", "9999999999999999.999999")
  }

  test("decimal64-decimal64(0) when when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci5_0-c_deci7_2 from deci_overflow where id = 4;", "99999.00")
  }

  test("decimal64-decimal128(0) when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci5_0-c_deci21_6 from deci_overflow where id = 4;", "99999.000000")
  }

  test("decimal64(0)-decimal128 when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci7_2-c_deci22_6 from deci_overflow where id = 4;", "-9999999999999999.999999")
  }

  test("decimal128(0)-decimal128 when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci21_6-c_deci22_6 from deci_overflow where id = 4;", "-9999999999999999.999999")
  }

  test("decimal64-literal(0) when when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci5_0-0 from deci_overflow where id = 4;", "99999")
  }

  test("literal(0)-decimal128 when when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select 0-c_deci22_6 from deci_overflow where id = 4;", "-9999999999999999.999999")
  }

  test("decimal64*decimal64(0) when when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci5_0*c_deci7_2 from deci_overflow where id = 4;", "0.00")
  }

  test("decimal64*decimal128(0) when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci5_0*c_deci21_6 from deci_overflow where id = 4;", "0.000000")
  }

  test("decimal64(0)*decimal128 when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci7_2*c_deci22_6 from deci_overflow where id = 4;", "0E-8")
  }

  test("decimal128(0)*decimal128 when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci21_6*c_deci22_6 from deci_overflow where id = 4;", "0.000000")
  }

  test("decimal64*literal(0) when when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci5_0*0 from deci_overflow where id = 4;", "0")
  }

  test("literal(0)*decimal128 when when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select 0*c_deci22_6 from deci_overflow where id = 4;", "0.000000")
  }

  test("decimal64(0)/decimal64 when when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci7_2/c_deci5_0 from deci_overflow where id = 4;", "0E-8")
  }

  test("decimal128(0)/decimal64 when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci21_6/c_deci5_0 from deci_overflow where id = 4;", "0E-12")
  }

  test("decimal64(0)/decimal128 when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci7_2/c_deci22_6 from deci_overflow where id = 4;", "0E-25")
  }

  test("decimal128(0)/decimal128 when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci21_6/c_deci22_6 from deci_overflow where id = 4;", "0E-17")
  }

  test("literal(0)/decimal64 when when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select 0/c_deci5_0 from deci_overflow where id = 4;", "0.000000")
  }

  test("literal(0)/decimal128 when when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select 0/c_deci22_6 from deci_overflow where id = 4;", "0E-23")
  }

  test("decimal64(0)%decimal64 when when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci7_2%c_deci5_0 from deci_overflow where id = 4;", "0.00")
  }

  test("decimal128(0)%decimal64 when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci21_6%c_deci5_0 from deci_overflow where id = 4;", "0.000000")
  }

  test("decimal64(0)%decimal128 when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci7_2%c_deci22_6 from deci_overflow where id = 4;", "0.000000")
  }

  test("decimal128(0)%decimal128 when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select c_deci21_6%c_deci22_6 from deci_overflow where id = 4;", "0.000000")
  }

  test("literal(0)%decimal64 when when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select 0%c_deci5_0 from deci_overflow where id = 4;", "0")
  }

  test("literal(0)%decimal128 when when spark.sql.ansi.enabled=true") {
    checkAnsiResult("select 0%c_deci22_6 from deci_overflow where id = 4;", "0.000000")
  }

  /* NULL decimal operation test cases */
  // spark.sql.ansi.enabled=false
  test("decimal64+decimal64(NULL) when when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci5_0+c_deci7_2 from deci_overflow where id = 5;")
  }

  test("decimal64+decimal128(NULL) when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci5_0+c_deci21_6 from deci_overflow where id = 5;")
  }

  test("decimal64(NULL)+decimal128 when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci7_2+c_deci22_6 from deci_overflow where id = 5;")
  }

  test("decimal128(NULL)+decimal128 when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci21_6+c_deci22_6 from deci_overflow where id = 5;")
  }

  test("literal(NULL)+decimal64 when when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci5_0+NULL from deci_overflow where id = 5;")
  }

  test("decimal128+literal(NULL) when when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci22_6+NULL from deci_overflow where id = 5;")
  }

  test("decimal64-decimal64(NULL) when when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci5_0-c_deci7_2 from deci_overflow where id = 5;")
  }

  test("decimal64-decimal128(NULL) when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci5_0-c_deci21_6 from deci_overflow where id = 5;")
  }

  test("decimal64(NULL)-decimal128 when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci7_2-c_deci22_6 from deci_overflow where id = 5;")
  }

  test("decimal128(NULL)-decimal128 when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci21_6-c_deci22_6 from deci_overflow where id = 5;")
  }

  test("decimal64-literal(NULL) when when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci5_0-NULL from deci_overflow where id = 5;")
  }

  test("literal(NULL)-decimal128 when when spark.sql.ansi.enabled=false") {
    checkResultNull("select NULL-c_deci22_6 from deci_overflow where id = 5;")
  }

  test("decimal64*decimal64(NULL) when when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci5_0*c_deci7_2 from deci_overflow where id = 5;")
  }

  test("decimal64*decimal128(NULL) when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci5_0*c_deci21_6 from deci_overflow where id = 5;")
  }

  test("decimal64(NULL)*decimal128 when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci7_2*c_deci22_6 from deci_overflow where id = 5;")
  }

  test("decimal128*decimal128(NULL) when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci22_6*c_deci21_6 from deci_overflow where id = 5;")
  }

  test("decimal64*literal(NULL) when when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci5_0*NULL from deci_overflow where id = 5;")
  }

  test("literal(NULL)*decimal128 when when spark.sql.ansi.enabled=false") {
    checkResultNull("select NULL*c_deci22_6 from deci_overflow where id = 5;")
  }

  test("decimal64/decimal64(NULL) when when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci5_0/c_deci7_2 from deci_overflow where id = 5;")
  }

  test("decimal128(NULL)/decimal64 when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci21_6/c_deci5_0 from deci_overflow where id = 5;")
  }

  test("decimal128/decimal64(NULL) when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci22_6/c_deci7_2 from deci_overflow where id = 5;")
  }

  test("decimal128(NULL)/decimal128 when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci21_6/c_deci22_6 from deci_overflow where id = 5;")
  }

  test("decimal64/literal(NULL) when when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci5_0/NULL from deci_overflow where id = 5;")
  }

  test("literal(NULL)/decimal128 when when spark.sql.ansi.enabled=false") {
    checkResultNull("select NULL/c_deci22_6 from deci_overflow where id = 5;")
  }

  test("decimal64%decimal64(NULL) when when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci5_0%c_deci7_2 from deci_overflow where id = 5;")
  }

  test("decimal128(NULL)%decimal64 when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci21_6%c_deci5_0 from deci_overflow where id = 5;")
  }

  test("decimal64(NULL)%decimal128 when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci7_2%c_deci22_6 from deci_overflow where id = 5;")
  }

  test("decimal128%decimal128(NULL) when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci22_6%c_deci21_6 from deci_overflow where id = 5;")
  }

  test("literal(NULL)%decimal64 when when spark.sql.ansi.enabled=false") {
    checkResultNull("select NULL%c_deci5_0 from deci_overflow where id = 5;")
  }

  test("decimal128%literal(NULL) when when spark.sql.ansi.enabled=false") {
    checkResultNull("select c_deci22_6%NULL from deci_overflow where id = 5;")
  }

  // spark.sql.ansi.enabled=true
  test("decimal64+decimal64(NULL) when when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci5_0+c_deci7_2 from deci_overflow where id = 5;")
  }

  test("decimal64+decimal128(NULL) when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci5_0+c_deci21_6 from deci_overflow where id = 5;")
  }

  test("decimal64(NULL)+decimal128 when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci7_2+c_deci22_6 from deci_overflow where id = 5;")
  }

  test("decimal128(NULL)+decimal128 when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci21_6+c_deci22_6 from deci_overflow where id = 5;")
  }

  test("literal(NULL)+decimal64 when when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci5_0+NULL from deci_overflow where id = 5;")
  }

  test("decimal128+literal(NULL) when when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci22_6+NULL from deci_overflow where id = 5;")
  }

  test("decimal64-decimal64(NULL) when when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci5_0-c_deci7_2 from deci_overflow where id = 5;")
  }

  test("decimal64-decimal128(NULL) when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci5_0-c_deci21_6 from deci_overflow where id = 5;")
  }

  test("decimal64(NULL)-decimal128 when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci7_2-c_deci22_6 from deci_overflow where id = 5;")
  }

  test("decimal128(NULL)-decimal128 when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci21_6-c_deci22_6 from deci_overflow where id = 5;")
  }

  test("decimal64-literal(NULL) when when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci5_0-NULL from deci_overflow where id = 5;")
  }

  test("literal(NULL)-decimal128 when when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select NULL-c_deci22_6 from deci_overflow where id = 5;")
  }

  test("decimal64*decimal64(NULL) when when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci5_0*c_deci7_2 from deci_overflow where id = 5;")
  }

  test("decimal64*decimal128(NULL) when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci5_0*c_deci21_6 from deci_overflow where id = 5;")
  }

  test("decimal64(NULL)*decimal128 when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci7_2*c_deci22_6 from deci_overflow where id = 5;")
  }

  test("decimal128*decimal128(NULL) when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci22_6*c_deci21_6 from deci_overflow where id = 5;")
  }

  test("decimal64*literal(NULL) when when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci5_0*NULL from deci_overflow where id = 5;")
  }

  test("literal(NULL)*decimal128 when when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select NULL*c_deci22_6 from deci_overflow where id = 5;")
  }

  test("decimal64/decimal64(NULL) when when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci5_0/c_deci7_2 from deci_overflow where id = 5;")
  }

  test("decimal128(NULL)/decimal64 when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci21_6/c_deci5_0 from deci_overflow where id = 5;")
  }

  test("decimal128/decimal64(NULL) when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci22_6/c_deci7_2 from deci_overflow where id = 5;")
  }

  test("decimal128(NULL)/decimal128 when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci21_6/c_deci22_6 from deci_overflow where id = 5;")
  }

  test("decimal64/literal(NULL) when when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci5_0/NULL from deci_overflow where id = 5;")
  }

  test("literal(NULL)/decimal128 when when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select NULL/c_deci22_6 from deci_overflow where id = 5;")
  }

  test("decimal64%decimal64(NULL) when when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci5_0%c_deci7_2 from deci_overflow where id = 5;")
  }

  test("decimal128(NULL)%decimal64 when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci21_6%c_deci5_0 from deci_overflow where id = 5;")
  }

  test("decimal64(NULL)%decimal128 when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci7_2%c_deci22_6 from deci_overflow where id = 5;")
  }

  test("decimal128%decimal128(NULL) when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci22_6%c_deci21_6 from deci_overflow where id = 5;")
  }

  test("literal(NULL)%decimal64 when when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select NULL%c_deci5_0 from deci_overflow where id = 5;")
  }

  test("decimal128%literal(NULL) when when spark.sql.ansi.enabled=true") {
    checkAnsiResultNull("select c_deci22_6%NULL from deci_overflow where id = 5;")
  }

}