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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.execution.ColumnarSparkPlanTest
import org.apache.spark.sql.types.{DataType, Decimal}

import java.math.MathContext

class CastSuite extends ColumnarSparkPlanTest {

  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  private var cast_table: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    cast_table = Seq[(Int, Boolean, Byte, Short, Int, Long, Float, Double, String, Decimal, Decimal)](
      (0, true, 10, 10, 10, 10, 10.0F, 10.0D, "10", new Decimal().set(BigDecimal("10.12", MathContext.UNLIMITED), 4, 2),
        new Decimal().set(BigDecimal("123456789123456.456789", MathContext.UNLIMITED), 21, 6)),
      (1, false, -10, -10, -10, -10, -10.0F, -10.0D, "-10", new Decimal().set(BigDecimal("-10.12", MathContext.UNLIMITED), 4, 2),
        new Decimal().set(BigDecimal("-123456789123456.456789", MathContext.UNLIMITED), 21, 6)),
      (2, false, 0, 0, 0, 0, 0, 0, "0", new Decimal().set(BigDecimal("0", MathContext.UNLIMITED), 4, 2),
        new Decimal().set(BigDecimal("0", MathContext.UNLIMITED), 21, 6)),
      (3, true, 127, 32767, 2147483647, 9223372036854775807L, 1.0E30F, 1.0E30D, "0",
        new Decimal().set(BigDecimal("99.99", MathContext.UNLIMITED), 4, 2),
        new Decimal().set(BigDecimal("999999999999999.999999", MathContext.UNLIMITED), 21, 6)),
    ).toDF("id", "c_boolean", "c_byte", "c_short", "c_int", "c_long", "c_float", "c_double", "c_string",
      "c_deci64", "c_deci128")

    // Decimal in DataFrame is decimal(38,16), so need to cast to the target decimal type
    cast_table = cast_table.withColumn("c_deci64", Column("c_deci64").cast("decimal(4,2)"))
      .withColumn("c_deci128", Column("c_deci128").cast("decimal(21,6)"))

    cast_table.createOrReplaceTempView("cast_table")
    cast_table.printSchema()
  }

  test("cast null as boolean") {
    val result = spark.sql("select cast(null as boolean);")
    val exception = intercept[Exception](
      result.collect().toSeq.head.getBoolean(0)
    )
  }

  test("cast null as byte") {
    val result = spark.sql("select cast(null as byte);")
    val exception = intercept[Exception](
      result.collect().toSeq.head.getByte(0)
    )
    assert(exception.isInstanceOf[NullPointerException], s"sql: ${sql}")
  }

  test("cast null as short") {
    val result = spark.sql("select cast(null as short);")
    val exception = intercept[Exception](
      result.collect().toSeq.head.getShort(0)
    )
    assert(exception.isInstanceOf[NullPointerException], s"sql: ${sql}")
  }

  test("cast null as int") {
    val result = spark.sql("select cast(null as int);")
    val exception = intercept[Exception](
      result.collect().toSeq.head.getInt(0)
    )
    assert(exception.isInstanceOf[NullPointerException], s"sql: ${sql}")
  }

  test("cast null as long") {
    val result = spark.sql("select cast(null as long);")
    val exception = intercept[Exception](
      result.collect().toSeq.head.getLong(0)
    )
    assert(exception.isInstanceOf[NullPointerException], s"sql: ${sql}")
  }

  test("cast null as float") {
    val result = spark.sql("select cast(null as float);")
    val exception = intercept[Exception](
      result.collect().toSeq.head.getFloat(0)
    )
    assert(exception.isInstanceOf[NullPointerException], s"sql: ${sql}")
  }

  test("cast null as double") {
    val result = spark.sql("select cast(null as double);")
    val exception = intercept[Exception](
      result.collect().toSeq.head.getDouble(0)
    )
    assert(exception.isInstanceOf[NullPointerException], s"sql: ${sql}")
  }

  test("cast null as date") {
    val result = spark.sql("select cast(null as date);")
    val output = result.collect().toSeq.head.getDate(0)
    assertResult(null, s"sql: ${sql}")(output)
  }

  test("cast null as timestamp") {
    val result = spark.sql("select cast(null as timestamp);")
    val output = result.collect().toSeq.head.getTimestamp(0)
    assertResult(null, s"sql: ${sql}")(output)
  }

  test("cast null as string") {
    val result = spark.sql("select cast(null as string);")
    val output = result.collect().toSeq.head.getString(0)
    assertResult(null, s"sql: ${sql}")(output)
  }

  test("cast null as decimal64") {
    val result = spark.sql("select cast(null as decimal(3,1));")
    val output = result.collect().toSeq.head.getDecimal(0)
    assertResult(null, s"sql: ${sql}")(output)
  }

  test("cast null as decimal128") {
    val result = spark.sql("select cast(null as decimal(23,2));")
    val output = result.collect().toSeq.head.getDecimal(0)
    assertResult(null, s"sql: ${sql}")(output)
  }

  test("cast string to boolean") {
    val result1 = spark.sql("select cast('true' as boolean);")
    val output1 = result1.collect().toSeq.head.getBoolean(0)
    assertResult(true, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast('0' as boolean);")
    val output2 = result2.collect().toSeq.head.getBoolean(0)
    assertResult(false, s"sql: ${sql}")(output2)

    val result3 = spark.sql("select cast('1' as boolean);")
    val output3 = result3.collect().toSeq.head.getBoolean(0)
    assertResult(true, s"sql: ${sql}")(output3)

    val result4 = spark.sql("select cast('10' as boolean);")
    val exception4 = intercept[Exception](
      result4.collect().toSeq.head.getBoolean(0)
    )
    assert(exception4.isInstanceOf[NullPointerException], s"sql: ${sql}")

    val result5 = spark.sql("select cast('test' as boolean);")
    val exception5 = intercept[Exception](
      result5.collect().toSeq.head.getBoolean(0)
    )
    assert(exception5.isInstanceOf[NullPointerException], s"sql: ${sql}")
  }

  test("cast boolean to string") {
    val result1 = spark.sql("select cast(c_boolean as string) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getString(0)
    assertResult("true", s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_boolean as string) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getString(0)
    assertResult("false", s"sql: ${sql}")(output2)
  }

  test("cast string to byte") {
    val result1 = spark.sql("select cast('10' as byte);")
    val output1 = result1.collect().toSeq.head.getByte(0)
    assertResult(10, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast('99999999999999999999' as byte);")
    val exception2 = intercept[Exception](
      result2.collect().toSeq.head.getByte(0)
    )
    assert(exception2.isInstanceOf[NullPointerException], s"sql: ${sql}")

    val result3 = spark.sql("select cast('false' as byte);")
    val exception3 = intercept[Exception](
      result3.collect().toSeq.head.getByte(0)
    )
    assert(exception3.isInstanceOf[NullPointerException], s"sql: ${sql}")
  }

  test("cast byte to string") {
    val result1 = spark.sql("select cast(c_byte as string) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getString(0)
    assertResult("10", s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_byte as string) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getString(0)
    assertResult("-10", s"sql: ${sql}")(output2)
  }

  test("cast string to short") {
    val result1 = spark.sql("select cast('10' as short);")
    val output1 = result1.collect().toSeq.head.getShort(0)
    assertResult(10, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast('99999999999999999999' as short);")
    val exception2 = intercept[Exception](
      result2.collect().toSeq.head.getShort(0)
    )
    assert(exception2.isInstanceOf[NullPointerException], s"sql: ${sql}")

    val result3 = spark.sql("select cast('false' as short);")
    val exception3 = intercept[Exception](
      result3.collect().toSeq.head.getShort(0)
    )
    assert(exception3.isInstanceOf[NullPointerException], s"sql: ${sql}")
  }

  test("cast short to string") {
    val result1 = spark.sql("select cast(c_short as string) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getString(0)
    assertResult("10", s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_short as string) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getString(0)
    assertResult("-10", s"sql: ${sql}")(output2)
  }

  test("cast string to int") {
    val result1 = spark.sql("select cast('10' as int);")
    val output1 = result1.collect().toSeq.head.getInt(0)
    assertResult(10, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast('99999999999999999999' as int);")
    val exception2 = intercept[Exception](
      result2.collect().toSeq.head.getInt(0)
    )
    assert(exception2.isInstanceOf[NullPointerException], s"sql: ${sql}")

    val result3 = spark.sql("select cast('false' as int);")
    val exception3 = intercept[Exception](
      result3.collect().toSeq.head.getInt(0)
    )
    assert(exception3.isInstanceOf[NullPointerException], s"sql: ${sql}")
  }

  test("cast int to string") {
    val result1 = spark.sql("select cast(c_int as string) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getString(0)
    assertResult("10", s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_int as string) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getString(0)
    assertResult("-10", s"sql: ${sql}")(output2)
  }

  test("cast string to long") {
    val result1 = spark.sql("select cast('10' as long);")
    val output1 = result1.collect().toSeq.head.getLong(0)
    assertResult(10, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast('999999999999999999999999999999' as long);")
    val exception2 = intercept[Exception](
      result2.collect().toSeq.head.getLong(0)
    )
    assert(exception2.isInstanceOf[NullPointerException], s"sql: ${sql}")

    val result3 = spark.sql("select cast('false' as long);")
    val exception3 = intercept[Exception](
      result3.collect().toSeq.head.getLong(0)
    )
    assert(exception3.isInstanceOf[NullPointerException], s"sql: ${sql}")
  }

  test("cast long to string") {
    val result1 = spark.sql("select cast(c_long as string) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getString(0)
    assertResult("10", s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_long as string) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getString(0)
    assertResult("-10", s"sql: ${sql}")(output2)
  }

  test("cast string to float") {
    val result1 = spark.sql("select cast('10' as float);")
    val output1 = result1.collect().toSeq.head.getFloat(0)
    assertResult(10.0F, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast('999999999999999999999999999999' as float);")
    val output2 = result2.collect().toSeq.head.getFloat(0)
    assertResult(1.0E30F, s"sql: ${sql}")(output2)

    val result3 = spark.sql("select cast('false' as float);")
    val exception3 = intercept[Exception](
      result3.collect().toSeq.head.getFloat(0)
    )
    assert(exception3.isInstanceOf[NullPointerException], s"sql: ${sql}")
  }

  test("cast float to string") {
    val result1 = spark.sql("select cast(c_float as string) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getString(0)
    assertResult("10.0", s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_float as string) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getString(0)
    assertResult("-10.0", s"sql: ${sql}")(output2)
  }

  test("cast string to double") {
    val result1 = spark.sql("select cast('10' as double);")
    val output1 = result1.collect().toSeq.head.getDouble(0)
    assertResult(10.0D, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast('999999999999999999999999999999' as double);")
    val output2 = result2.collect().toSeq.head.getDouble(0)
    assertResult(1.0E30, s"sql: ${sql}")(output2)

    val result3 = spark.sql("select cast('false' as double);")
    val exception3 = intercept[Exception](
      result3.collect().toSeq.head.getDouble(0)
    )
    assert(exception3.isInstanceOf[NullPointerException], s"sql: ${sql}")
  }

  test("cast double to string") {
    val result1 = spark.sql("select cast(c_double as string) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getString(0)
    assertResult("10.0", s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_double as string) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getString(0)
    assertResult("-10.0", s"sql: ${sql}")(output2)
  }

  test("cast string to decimal64") {
    val result1 = spark.sql("select cast('10' as decimal(4,2));")
    val output1 = result1.collect().toSeq.head.getDecimal(0)
    assertResult("10.00", s"sql: ${sql}")(output1.toString)

    val result2 = spark.sql("select cast('999999999999999999999999999999' as decimal(4,2));")
    val output2 = result2.collect().toSeq.head.getDecimal(0)
    assertResult(null, s"sql: ${sql}")(output2)

    val result3 = spark.sql("select cast('false' as decimal(4,2));")
    val output3 =result3.collect().toSeq.head.getDecimal(0)
    assertResult(null, s"sql: ${sql}")(output3)
  }

  test("cast decimal64 to string") {
    val result1 = spark.sql("select cast(c_deci64 as string) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getString(0)
    assertResult("10.12", s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_deci64 as string) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getString(0)
    assertResult("-10.12", s"sql: ${sql}")(output2)
  }

  test("cast string to decimal128") {
    val result1 = spark.sql("select cast('10' as decimal(21,6));")
    val output1 = result1.collect().toSeq.head.getDecimal(0)
    assertResult("10.000000", s"sql: ${sql}")(output1.toString)

    val result2 = spark.sql("select cast('999999999999999999999999999999' as decimal(21,6));")
    val output2 = result2.collect().toSeq.head.getDecimal(0)
    assertResult(null, s"sql: ${sql}")(output2)

    val result3 = spark.sql("select cast('false' as decimal(21,6));")
    val output3 = result3.collect().toSeq.head.getDecimal(0)
    assertResult(null, s"sql: ${sql}")(output3)
  }

  test("cast decimal128 to string") {
    val result1 = spark.sql("select cast(c_deci128 as string) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getString(0)
    assertResult("123456789123456.456789", s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_deci128 as string) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getString(0)
    assertResult("-123456789123456.456789", s"sql: ${sql}")(output2)
  }

  // cast from boolean
  test("cast boolean to byte") {
    val result1 = spark.sql("select cast(c_boolean as byte) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getByte(0)
    assertResult(1, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_boolean as byte) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getByte(0)
    assertResult(0, s"sql: ${sql}")(output2)
  }

  test("cast boolean to short") {
    val result1 = spark.sql("select cast(c_boolean as short) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getShort(0)
    assertResult(1, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_boolean as short) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getShort(0)
    assertResult(0, s"sql: ${sql}")(output2)
  }

  test("cast boolean to int") {
    val result1 = spark.sql("select cast(c_boolean as int) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getInt(0)
    assertResult(1, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_boolean as int) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getInt(0)
    assertResult(0, s"sql: ${sql}")(output2)
  }

  test("cast boolean to long") {
    val result1 = spark.sql("select cast(c_boolean as long) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getLong(0)
    assertResult(1, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_boolean as long) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getLong(0)
    assertResult(0, s"sql: ${sql}")(output2)
  }

  test("cast boolean to float") {
    val result1 = spark.sql("select cast(c_boolean as float) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getFloat(0)
    assertResult(1.0F, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_boolean as float) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getFloat(0)
    assertResult(0.0F, s"sql: ${sql}")(output2)
  }

  test("cast boolean to double") {
    val result1 = spark.sql("select cast(c_boolean as double) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getDouble(0)
    assertResult(1.0D, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_boolean as double) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getDouble(0)
    assertResult(0.0D, s"sql: ${sql}")(output2)
  }

  test("cast boolean to decimal64") {
    val result1 = spark.sql("select cast(c_boolean as decimal(4,2)) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getDecimal(0)
    assertResult("1.00", s"sql: ${sql}")(output1.toString)

    val result2 = spark.sql("select cast(c_boolean as decimal(4,2)) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getDecimal(0)
    assertResult("0.00", s"sql: ${sql}")(output2.toString)
  }

  test("cast boolean to decimal128") {
    val result1 = spark.sql("select cast(c_boolean as decimal(21,6)) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getDecimal(0)
    assertResult("1.000000", s"sql: ${sql}")(output1.toString)

    val result2 = spark.sql("select cast(c_boolean as decimal(21,6)) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getDecimal(0)
    assertResult("0.000000", s"sql: ${sql}")(output2.toString)
  }

  // cast from byte
  test("cast byte to boolean") {
    val result1 = spark.sql("select cast(c_byte as boolean) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getBoolean(0)
    assertResult(true, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_byte as boolean) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getBoolean(0)
    assertResult(true, s"sql: ${sql}")(output2)

    val result3= spark.sql("select cast(c_byte as boolean) from cast_table where id=2;")
    val output3 = result3.collect().toSeq.head.getBoolean(0)
    assertResult(false, s"sql: ${sql}")(output3)
  }

  test("cast byte to short") {
    val result1 = spark.sql("select cast(c_byte as short) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getShort(0)
    assertResult(10, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_byte as short) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getShort(0)
    assertResult(-10, s"sql: ${sql}")(output2)
  }

  test("cast byte to int") {
    val result1 = spark.sql("select cast(c_byte as int) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getInt(0)
    assertResult(10, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_byte as int) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getInt(0)
    assertResult(-10, s"sql: ${sql}")(output2)
  }

  test("cast byte to long") {
    val result1 = spark.sql("select cast(c_byte as long) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getLong(0)
    assertResult(10, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_byte as long) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getLong(0)
    assertResult(-10, s"sql: ${sql}")(output2)
  }

  test("cast byte to float") {
    val result1 = spark.sql("select cast(c_byte as float) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getFloat(0)
    assertResult(10.0F, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_byte as float) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getFloat(0)
    assertResult(-10.0F, s"sql: ${sql}")(output2)
  }

  test("cast byte to double") {
    val result1 = spark.sql("select cast(c_byte as double) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getDouble(0)
    assertResult(10.0D, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_byte as double) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getDouble(0)
    assertResult(-10.0D, s"sql: ${sql}")(output2)
  }

  test("cast byte to decimal64") {
    val result1 = spark.sql("select cast(c_byte as decimal(4,2)) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getDecimal(0)
    assertResult("10.00", s"sql: ${sql}")(output1.toString)

    val result2 = spark.sql("select cast(c_byte as decimal(4,2)) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getDecimal(0)
    assertResult("-10.00", s"sql: ${sql}")(output2.toString)
  }

  test("cast byte to decimal128") {
    val result1 = spark.sql("select cast(c_byte as decimal(21,6)) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getDecimal(0)
    assertResult("10.000000", s"sql: ${sql}")(output1.toString)

    val result2 = spark.sql("select cast(c_byte as decimal(21,6)) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getDecimal(0)
    assertResult("-10.000000", s"sql: ${sql}")(output2.toString)
  }

  // cast from short
  test("cast short to boolean") {
    val result1 = spark.sql("select cast(c_short as boolean) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getBoolean(0)
    assertResult(true, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_short as boolean) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getBoolean(0)
    assertResult(true, s"sql: ${sql}")(output2)

    val result3 = spark.sql("select cast(c_short as boolean) from cast_table where id=2;")
    val output3 = result3.collect().toSeq.head.getBoolean(0)
    assertResult(false, s"sql: ${sql}")(output3)
  }

  test("cast short to byte") {
    val result1 = spark.sql("select cast(c_short as byte) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getByte(0)
    assertResult(10, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_short as byte) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getByte(0)
    assertResult(-10, s"sql: ${sql}")(output2)

    val result3 = spark.sql("select cast(c_short as byte) from cast_table where id=3;")
    val output3 = result3.collect().toSeq.head.getByte(0)
    assertResult(-1, s"sql: ${sql}")(output3)
  }

  test("cast short to int") {
    val result1 = spark.sql("select cast(c_short as int) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getInt(0)
    assertResult(10, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_short as int) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getInt(0)
    assertResult(-10, s"sql: ${sql}")(output2)

    val result3 = spark.sql("select cast(c_short as int) from cast_table where id=3;")
    val output3 = result3.collect().toSeq.head.getInt(0)
    assertResult(32767, s"sql: ${sql}")(output3)
  }

  test("cast short to long") {
    val result1 = spark.sql("select cast(c_short as long) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getLong(0)
    assertResult(10, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_short as long) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getLong(0)
    assertResult(-10, s"sql: ${sql}")(output2)

    val result3 = spark.sql("select cast(c_short as long) from cast_table where id=3;")
    val output3 = result3.collect().toSeq.head.getLong(0)
    assertResult(32767, s"sql: ${sql}")(output3)
  }

  test("cast short to float") {
    val result1 = spark.sql("select cast(c_short as float) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getFloat(0)
    assertResult(10.0F, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_short as float) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getFloat(0)
    assertResult(-10.0F, s"sql: ${sql}")(output2)

    val result3 = spark.sql("select cast(c_short as float) from cast_table where id=3;")
    val output3 = result3.collect().toSeq.head.getFloat(0)
    assertResult(32767.0F, s"sql: ${sql}")(output3)
  }

  test("cast short to double") {
    val result1 = spark.sql("select cast(c_short as double) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getDouble(0)
    assertResult(10.0D, s"sql: ${sql}")(output1)

    val result2 = spark.sql("select cast(c_short as double) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getDouble(0)
    assertResult(-10.0D, s"sql: ${sql}")(output2)

    val result3 = spark.sql("select cast(c_short as double) from cast_table where id=3;")
    val output3 = result3.collect().toSeq.head.getDouble(0)
    assertResult(32767.0D, s"sql: ${sql}")(output3)
  }

  test("cast short to decimal64") {
    val result1 = spark.sql("select cast(c_short as decimal(4,2)) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getDecimal(0)
    assertResult("10.00", s"sql: ${sql}")(output1.toString)

    val result2 = spark.sql("select cast(c_short as decimal(4,2)) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getDecimal(0)
    assertResult("-10.00", s"sql: ${sql}")(output2.toString)

    val result3 = spark.sql("select cast(c_short as decimal(4,2)) from cast_table where id=3;")
    val output3 = result3.collect().toSeq.head.getDecimal(0)
    assertResult(null, s"sql: ${sql}")(output3)

    val result4 = spark.sql("select cast(c_short as decimal(9,2)) from cast_table where id=3;")
    val output4 = result4.collect().toSeq.head.getDecimal(0)
    assertResult("32767.00", s"sql: ${sql}")(output4.toString)
  }

  test("cast short to decimal128") {
    val result1 = spark.sql("select cast(c_short as decimal(21,6)) from cast_table where id=0;")
    val output1 = result1.collect().toSeq.head.getDecimal(0)
    assertResult("10.000000", s"sql: ${sql}")(output1.toString)

    val result2 = spark.sql("select cast(c_short as decimal(21,6)) from cast_table where id=1;")
    val output2 = result2.collect().toSeq.head.getDecimal(0)
    assertResult("-10.000000", s"sql: ${sql}")(output2.toString)

    val result3 = spark.sql("select cast(c_short as decimal(21,6)) from cast_table where id=3;")
    val output3 = result3.collect().toSeq.head.getDecimal(0)
    assertResult("32767.000000", s"sql: ${sql}")(output3.toString)
  }
}