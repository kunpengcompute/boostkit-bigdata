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

import org.apache.spark.sql.execution.{ColumnarProjectExec, ColumnarSparkPlanTest, ProjectExec}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.{DataFrame, Row}

class ColumnarDecimalCastSuite extends ColumnarSparkPlanTest{
  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  private var byteDecimalDf: DataFrame = _
  private var shortDecimalDf: DataFrame = _
  private var intDecimalDf: DataFrame = _
  private var longDecimalDf: DataFrame = _
  private var floatDecimalDf: DataFrame = _
  private var doubleDecimalDf: DataFrame = _
  private var stringDecimalDf: DataFrame = _
  private var decimalDecimalDf: DataFrame = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    byteDecimalDf = Seq[(java.lang.Byte, java.lang.Byte, Decimal, Decimal, Decimal)](
      (127.toByte, 12.toByte, Decimal(100000, 7, 0), Decimal(128.99, 7, 2), Decimal(11223344.123, 21, 6)),
      ((-12).toByte, null, Decimal(25, 7, 0), Decimal(25.55, 7, 2), Decimal(-99999999999.1234, 21, 6)),
      (9.toByte, (-11).toByte, Decimal(-25, 7, 0), null, null),
      ((-9).toByte, null, Decimal(145, 7, 0), Decimal(256.66, 7, 2), null)
    ).toDF("c_byte_normal", "c_byte_null", "c_deci7_0", "c_deci7_2_null", "c_deci21_6_null")

    shortDecimalDf = Seq[(java.lang.Short, java.lang.Short, Decimal, Decimal)](
      (10.toShort, 15.toShort, Decimal(130.6, 17, 2), Decimal(128.99, 21, 6)),
      ((-10).toShort, null, null, Decimal(32723.55, 21, 6)),
      (1000.toShort, null, Decimal(-30.8, 17, 2), null),
      ((-1000).toShort, 2000.toShort, null, Decimal(-99999.19, 21, 6)),
    ).toDF("c_short_normal", "c_short_null", "c_deci17_2_null", "c_deci21_6_null")

    intDecimalDf = Seq[(java.lang.Integer, java.lang.Integer, Decimal, Decimal)](
      (1272763, 1111, null, Decimal(1234.555431, 21, 6)),
      (22723, 2222, Decimal(32728543.12, 17, 2), Decimal(99999999.999, 21, 6)),
      (9, null, Decimal(-195010407800.34, 17, 2), Decimal(-99999999.999, 21, 6)),
      (345, -4444, Decimal(12000.56, 17, 2), null)
    ).toDF("c_int_normal", "c_int_null", "c_deci17_2_null", "c_deci21_6_null")

    longDecimalDf = Seq[(java.lang.Long, java.lang.Long, Decimal, Decimal)](
      (922337203L, 1231313L, null, Decimal(1922337203685.99, 38, 2)),
      (22723L, null, Decimal(2233720368.12, 17, 2), Decimal(54775800.55, 38, 2)),
      (9L, -123131, Decimal(-2192233720.34, 17, 2), null)
    ).toDF("c_long_normal", "c_long_null", "c_deci17_2_null", "c_deci38_2_null")

    floatDecimalDf = Seq[(java.lang.Float, java.lang.Float, Decimal, Decimal)](
      (1234.4129F, 123.12F, null, Decimal(10000.99, 38, 2)),
      (1234.4125F, 123.34F, Decimal(1234.11, 17, 2), Decimal(10000.99, 38, 2)),
      (3.4E10F, null, Decimal(999999999999.22, 17, 2), Decimal(999999999999.99, 38, 2)),
      (-3.4E-10F, -1123.1113F, Decimal(-999999999999.33, 17, 2), Decimal(-999999999999.99, 38, 2))
    ).toDF("c_float_normal", "c_float_null", "c_deci17_2_null", "c_deci38_2_null")

    doubleDecimalDf = Seq[(java.lang.Double, java.lang.Double, Decimal, Decimal)](
      (1234.4129, 1123, Decimal(10000.99, 17, 2), Decimal(1234.14, 38, 2)),
      (1234.4125, null, Decimal(10000.99, 17, 2), Decimal(1234.14, 38, 2)),
      (1234.4124, 1234, Decimal(10000.99, 17, 2), null)
    ).toDF("c_double_normal", "c_double_null", "c_deci17_2_null", "c_deci38_2_null")

    stringDecimalDf = Seq[(String, String, Decimal, Decimal)](
      ("  99999   ", "111       ", null, Decimal(128.99, 38, 2)),
      ("-1234.15  ", "222.2     ", Decimal(99999.11, 17, 2), Decimal(99999.99, 38, 2)),
      ("abc       ", "-333.33   ", Decimal(-11111.22, 17, 2), Decimal(-99999.19, 38, 2)),
      ("999999    ", null, Decimal(99999.33, 17, 2), null),
    ).toDF("c_string_normal", "c_string_null", "c_deci17_2_null", "c_deci38_2_null")

    decimalDecimalDf = Seq[(Decimal, Decimal)](
      (Decimal(128.99, 17, 2), Decimal(1234.555431, 21, 6)),
      (null, Decimal(99999999.999, 21, 6)),
      (Decimal(25.19, 17, 2), Decimal(-99999999.999, 21, 6)),
      (Decimal(-195010407800.19, 17, 2), Decimal(-999999.99999, 21, 6))
    ).toDF("c_deci17_2_null", "c_deci21_6")

    byteDecimalDf.createOrReplaceTempView("deci_byte")
    shortDecimalDf.createOrReplaceTempView("deci_short")
    intDecimalDf.createOrReplaceTempView("deci_int")
    longDecimalDf.createOrReplaceTempView("deci_long")
    floatDecimalDf.createOrReplaceTempView("deci_float")
    doubleDecimalDf.createOrReplaceTempView("deci_double")
    stringDecimalDf.createOrReplaceTempView("deci_string")
    decimalDecimalDf.createOrReplaceTempView("deci_decimal")
  }

  // byte
  test("Test ColumnarProjectExec not happen and result is same as native " +
    "when cast byte to decimal") {
    val res = spark.sql("select c_byte_normal, cast(c_byte_normal as decimal(10, 2))," +
      "cast(c_byte_normal as decimal(19,1)) from deci_byte")
    assertOmniProjectNotHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(-12, -12.00, -12.0),
        Row(127, 127.00, 127.0),
        Row(-9, -9.00, -9.0),
        Row(9, 9.00, 9.0)
      )
    )
  }

  test("Test ColumnarProjectExec not happen and result is same as native " +
    "when cast byte to decimal overflow with spark.sql.ansi.enabled=false") {
    val res = spark.sql("select c_byte_normal, cast(c_byte_normal as decimal(2, 1))," +
      "cast(c_byte_normal as decimal(19,18)) from deci_byte")
    assertOmniProjectNotHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(-12, null, null),
        Row(127, null, null),
        Row(-9, -9.0, -9.000000000000000000),
        Row(9, 9.0, 9.000000000000000000)
      )
    )
  }

  test("Test ColumnarProjectExec not happen and result is same as native " +
    "when cast byte to decimal with null") {
    val res = spark.sql("select c_byte_null, cast(c_byte_null as decimal(10, 2))," +
      "cast(c_byte_null as decimal(19,3)) from deci_byte")
    assertOmniProjectNotHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(null, null, null),
        Row(12, 12.00, 12.000),
        Row(null, null, null),
        Row(-11, -11.00, -11.000)
      )
    )
  }

  test("Test ColumnarProjectExec not happen and result is same as native " +
    "when cast decimal to byte") {
    val res = spark.sql("select cast(c_deci7_0 as byte)," +
      "cast(c_deci7_2_null as byte), cast(c_deci21_6_null as byte) from deci_byte")
    assertOmniProjectNotHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(25.toByte, 25.toByte, 1.toByte),
        Row((-96).toByte, (-128).toByte, 48.toByte),
        Row((-111).toByte, 0.toByte, null),
        Row((-25).toByte, null, null)
      )
    )
  }

  // short
  test("Test ColumnarProjectExec not happen and result is same as native " +
    "when cast short to decimal") {
    val res = spark.sql("select c_short_normal, cast(c_short_normal as decimal(13, 3))," +
      "cast(c_short_normal as decimal(20,2)) from deci_short")
    assertOmniProjectNotHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(-1000, -1000.000, -1000.00),
        Row(10, 10.000, 10.00),
        Row(-10, -10.000, -10.00),
        Row(1000, 1000.000, 1000.00)
      )
    )
  }

  test("Test ColumnarProjectExec not happen and result is same as native " +
    "when cast short to decimal overflow with spark.sql.ansi.enabled=false") {
    val res = spark.sql("select c_short_normal, cast(c_short_normal as decimal(2, 1))," +
      "cast(c_short_normal as decimal(20,18)) from deci_short")
    assertOmniProjectNotHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(-1000, null, null),
        Row(10, null, 10.000000000000000000),
        Row(-10, null, -10.000000000000000000),
        Row(1000, null, null)
      )
    )
  }

  test("Test ColumnarProjectExec not happen and result is same as native " +
    "when cast short to decimal with null") {
    val res = spark.sql("select c_short_null, cast(c_short_null as decimal(14, 2))," +
      "cast(c_short_null as decimal(20,3)) from deci_short")
    assertOmniProjectNotHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(2000, 2000.00, 2000.000),
        Row(15, 15.00, 15.000),
        Row(null, null, null),
        Row(null, null, null)
      )
    )
  }

  test("Test ColumnarProjectExec not happen and result is same as native " +
    "when cast decimal to short") {
    val res = spark.sql("select cast(c_deci17_2_null as short)," +
      "cast(c_deci21_6_null as short) from deci_short")
    assertOmniProjectNotHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(null, 31073.toShort),
        Row(130.toShort, 128.toShort),
        Row(null, 32723.toShort),
        Row((-30).toShort, null)
      )
    )
  }

  // int
  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast int to decimal") {
    val res = spark.sql("select c_int_normal, cast(c_int_normal as decimal(16, 3))," +
      "cast(c_int_normal as decimal(22,4)) from deci_int")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(22723, 22723.000, 22723.0000),
        Row(1272763, 1272763.000, 1272763.0000),
        Row(9, 9.000, 9.0000),
        Row(345, 345.000, 345.0000)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast int to decimal overflow with spark.sql.ansi.enabled=false") {
    val res = spark.sql("select c_int_normal, cast(c_int_normal as decimal(2, 1))," +
      "cast(c_int_normal as decimal(22,19)) from deci_int")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(22723, null, null),
        Row(1272763, null, null),
        Row(9, 9.0, 9.0000000000000000000),
        Row(345, null, 345.0000000000000000000)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast int to decimal with null") {
    val res = spark.sql("select c_int_null, cast(c_int_null as decimal(16, 4))," +
      "cast(c_int_null as decimal(23,5)) from deci_int")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(2222, 2222.0000, 2222.00000),
        Row(1111, 1111.0000, 1111.00000),
        Row(null, null, null),
        Row(-4444, -4444.0000, -4444.00000)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast decimal to int") {
    val res = spark.sql("select cast(c_deci17_2_null as int)," +
      "cast(c_deci21_6_null as int) from deci_int")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(32728543, 99999999),
        Row(null, 1234),
        Row(-1736879480, -99999999),
        Row(12000, null)
      )
    )
  }

  // long
  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast long to decimal") {
    val res = spark.sql("select c_long_normal, cast(c_long_normal as decimal(16, 2))," +
      "cast(c_long_normal as decimal(26,7)) from deci_long")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(922337203L, 922337203.00, 922337203.0000000),
        Row(22723L, 22723.00, 22723.0000000),
        Row(9L, 9.000, 9.0000)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast long to decimal overflow with spark.sql.ansi.enabled=false") {
    val res = spark.sql("select c_long_normal, cast(c_long_normal as decimal(3, 1))," +
      "cast(c_long_normal as decimal(27,24)) from deci_long")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(922337203L, null, null),
        Row(22723L, null, null),
        Row(9L, 9.0, 9.000000000000000000000000)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast long to decimal with null") {
    val res = spark.sql("select c_long_null, cast(c_long_null as decimal(17, 6))," +
      "cast(c_long_null as decimal(29,7)) from deci_long")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(null, null, null),
        Row(1231313L, 1231313.000000, 1231313.0000000),
        Row(-123131L, -123131.000000, -123131.0000000)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast decimal to long") {
    val res = spark.sql("select cast(c_deci17_2_null as long)," +
      "cast(c_deci38_2_null as long) from deci_long")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(null, 1922337203685L),
        Row(2233720368L, 54775800L),
        Row(-2192233720L, null)
      )
    )
  }

  // float
  test("Test ColumnarProjectExec not happen and result is same as native " +
    "when cast float to decimal") {
    val res = spark.sql("select c_float_normal, cast(c_float_normal as decimal(14, 3))," +
      "cast(c_float_normal as decimal(30,7)) from deci_float")
    assertOmniProjectNotHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(-3.4E-10F, 0.000, 0.0000000),
        Row(3.3999999E10F, 33999998976.00, 33999998976.0000000),
        Row(1234.4125F, 1234.412, 1234.4124756),
        Row(1234.4128F, 1234.413, 1234.4128418)
      )
    )
  }

  test("Test ColumnarProjectExec not happen and result is same as native " +
    "when cast float to decimal overflow with spark.sql.ansi.enabled=false") {
    val res = spark.sql("select c_float_normal, cast(c_float_normal as decimal(14, 11))," +
      "cast(c_float_normal as decimal(30,27)) from deci_float")
    assertOmniProjectNotHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(-3.4E-10F, -0.00000000034, -0.000000000340000000376150500),
        Row(3.3999999E10F, null, null),
        Row(1234.4125F, null, null),
        Row(1234.4128F, null, null)
      )
    )
  }

  test("Test ColumnarProjectExec not happen and result is same as native " +
    "when cast float to decimal with null") {
    val res = spark.sql("select c_float_null, cast(c_float_null as decimal(17, 6))," +
      "cast(c_float_null as decimal(29,7)) from deci_float")
    assertOmniProjectNotHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(-1123.1113F, -1123.111328, -1123.1113281),
        Row(null, null, null),
        Row(123.34F, 123.339996, 123.3399963),
        Row(123.12F, 123.120003, 123.1200027)
      )
    )
  }

  test("Test ColumnarProjectExec not happen and result is same as native " +
    "when cast decimal to float") {
    val res = spark.sql("select cast(c_deci17_2_null as float)," +
      "cast(c_deci38_2_null as float) from deci_float")
    assertOmniProjectNotHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(-1.0E12F, -1.0E12F),
        Row(1.0E12F, 1.0E12F),
        Row(1234.11F, 10000.99F),
        Row(null, 10000.99F)
      )
    )
  }

  // double
  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast double to decimal") {
    val res = spark.sql("select c_double_normal, cast(c_double_normal as decimal(8, 4))," +
      "cast(c_double_normal as decimal(32,4)) from deci_double")
    assertOmniProjectNotHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(1234.4129, 1234.4129, 1234.4129),
        Row(1234.4125, 1234.4125, 1234.4125),
        Row(1234.4124, 1234.4124, 1234.4124)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast double to decimal overflow with spark.sql.ansi.enabled=false") {
    val res = spark.sql("select c_double_normal, cast(c_double_normal as decimal(8, 6))," +
      "cast(c_double_normal as decimal(32,30)) from deci_double")
    assertOmniProjectNotHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(1234.4129, null, null),
        Row(1234.4125, null, null),
        Row(1234.4124, null, null)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast double to decimal with null") {
    val res = spark.sql("select c_double_null, cast(c_double_null as decimal(8, 4))," +
      "cast(c_double_null as decimal(34,4)) from deci_double")
    assertOmniProjectNotHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(1123.0, 1123.0000, 1123.0000),
        Row(null, null, null),
        Row(1234.0, 1234.0000, 1234.0000)
      )
    )
  }

  test("Test ColumnarProjectExecc happen and result is same as native " +
    "when cast decimal to double") {
    val res = spark.sql("select cast(c_deci17_2_null as double)," +
      "cast(c_deci38_2_null as double) from deci_double")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(10000.99, 1234.14),
        Row(10000.99, 1234.14),
        Row(10000.99, null)
      )
    )
  }

  // decimal
  test("Test ColumnarProjectExec happen when cast decimal to decimal") {
    val res = spark.sql("select c_deci21_6, cast(c_deci21_6 as decimal(17, 6))," +
      "cast(c_deci21_6 as decimal(28,9)) from deci_decimal")
    assertOmniProjectHappened(res)
  }

  test("Test ColumnarProjectExec happen when cast decimal " +
    "to decimal overflow with spark.sql.ansi.enabled=false") {
    val res = spark.sql("select c_deci21_6, cast(c_deci21_6 as decimal(17, 14))," +
      "cast(c_deci21_6 as decimal(31,29)) from deci_decimal")
    assertOmniProjectHappened(res)
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast decimal to decimal with null") {
    val res = spark.sql("select c_deci17_2_null, cast(c_deci17_2_null as decimal(18, 6))," +
      "cast(c_deci17_2_null as decimal(31,10)) from deci_decimal")
    assertOmniProjectHappened(res)
  }

  // string
  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast string to decimal") {
    val res = spark.sql("select c_string_normal, cast(c_string_normal as decimal(16, 5))," +
      "cast(c_string_normal as decimal(27,5)) from deci_string")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row("abc       ", null, null),
        Row("-1234.15  ", -1234.15000, -1234.15000),
        Row("  99999   ", 99999.00000, 99999.00000),
        Row("999999    ", 999999.00000, 999999.00000)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast string to decimal overflow with spark.sql.ansi.enabled=false") {
    val res = spark.sql("select c_string_normal, cast(c_string_normal as decimal(16, 14))," +
      "cast(c_string_normal as decimal(27,23)) from deci_string")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row("abc       ", null, null),
        Row("-1234.15  ", null, -1234.15000000000000000000000),
        Row("  99999   ", null, null),
        Row("999999    ", null, null)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast string to decimal with null") {
    val res = spark.sql("select c_string_null, cast(c_string_null as decimal(16, 5))," +
      "cast(c_string_null as decimal(27,5)) from deci_string")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row("-333.33   ", -333.33000, -333.33000),
        Row("222.2     ", 222.20000, 222.20000),
        Row("111       ", 111.00000, 111.00000),
        Row(null, null, null)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast decimal to string") {
    val res = spark.sql("select cast(cast(c_deci17_2_null as string) as decimal(38, 2))," +
      "cast(cast(c_deci38_2_null as string) as decimal(38, 2)) from deci_string")
    val executedPlan = res.queryExecution.executedPlan
    println(executedPlan)
    assert(executedPlan.find(_.isInstanceOf[ColumnarProjectExec]).isDefined, s"ColumnarProjectExec not happened, executedPlan as follows： \n$executedPlan")
    assert(executedPlan.find(_.isInstanceOf[ProjectExec]).isEmpty, s"ProjectExec happened, executedPlan as follows： \n$executedPlan")
    checkAnswer(
      res,
      Seq(
        Row(-11111.22, -99999.19),
        Row(99999.11, 99999.99),
        Row(null, 128.99),
        Row(99999.33, null)
      )
    )
  }

  // literal int
  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast literal int to decimal") {
    val res = spark.sql("select cast(1111 as decimal(7,0)), cast(1111 as decimal(21, 6))")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(1111, 1111.000000)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast literal int to decimal overflow with spark.sql.ansi.enabled=false") {
    val res = spark.sql("select cast(1111 as decimal(7,5)), cast(1111 as decimal(21, 19))")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(null, null)
      )
    )
  }

  // literal long
  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast literal long to decimal") {
    val res = spark.sql("select cast(111111111111111 as decimal(15,0)), cast(111111111111111 as decimal(21, 6))")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(111111111111111L, 111111111111111.000000)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast literal long to decimal overflow with spark.sql.ansi.enabled=false") {
    val res = spark.sql("select cast(111111111111111 as decimal(15,11)), cast(111111111111111 as decimal(21, 15))")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(null, null)
      )
    )
  }

  // literal double
  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast literal double to decimal") {
    val res = spark.sql("select cast(666666.666 as decimal(15,3)), cast(666666.666 as decimal(21, 6))")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(666666.666, 666666.666)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast literal decimal to decimal overflow with spark.sql.ansi.enabled=false") {
    val res = spark.sql("select cast(666666.666 as decimal(4,3)), cast(666666.666 as decimal(21, 16))," +
      "cast(666666.666 as decimal(21, 16)), cast(666666.666 as decimal(21, 2))")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(null, null, null, 666666.67)
      )
    )
  }

  // literal string
  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast literal string to decimal") {
    val res = spark.sql("select cast('  666666.666 ' as decimal(15,3)), cast('  666666.666 ' as decimal(21, 6))")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(666666.666, 666666.666000)
      )
    )
  }

  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast literal string to decimal overflow with spark.sql.ansi.enabled=false") {
    val res = spark.sql("select cast('  666666.666 ' as decimal(15,3)), cast('  666666.666 ' as decimal(21, 6)), " +
      "cast('  666666.666 ' as decimal(21,18)), cast('  666666.666 ' as decimal(21, 2))")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(666666.666, 666666.666000, null, 666666.67)
      )
    )
  }

  // literal null
  test("Test ColumnarProjectExec happen and result is same as native " +
    "when cast literal null to decimal") {
    val res = spark.sql("select cast(null as decimal(2,0)), cast(null as decimal(21, 0))")
    assertOmniProjectHappened(res)
    checkAnswer(
      res,
      Seq(
        Row(null, null)
      )
    )
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