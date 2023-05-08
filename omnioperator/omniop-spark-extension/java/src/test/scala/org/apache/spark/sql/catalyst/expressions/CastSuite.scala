package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.execution.ColumnarSparkPlanTest
import org.apache.spark.sql.types.DataType

class CastSuite extends ColumnarSparkPlanTest {
  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  test("cast null as boolean") {
    val result = spark.sql("select cast(null as boolean);")
    val output = result.collect().toSeq.head.getBoolean(0)
    assertResult(null, s"sql: ${sql}")(output)
  }

  test("cast null as byte") {
    val result = spark.sql("select cast(null as byte);")
    val output = result.collect().toSeq.head.getByte(0)
    assertResult(null, s"sql: ${sql}")(output)
  }

  test("cast null as short") {
    val result = spark.sql("select cast(null as short);")
    val output = result.collect().toSeq.head.getShort(0)
    assertResult(null, s"sql: ${sql}")(output)
  }

  test("cast null as int") {
    val result = spark.sql("select cast(null as int);")
    val output = result.collect().toSeq.head.getInt(0)
    assertResult(null, s"sql: ${sql}")(output)
  }

  test("cast null as long") {
    val result = spark.sql("select cast(null as long);")
    val output = result.collect().toSeq.head.getLong(0)
    assertResult(null, s"sql: ${sql}")(output)
  }

  test("cast null as float") {
    val result = spark.sql("select cast(null as float);")
    val output = result.collect().toSeq.head.getFloat(0)
    assertResult(null, s"sql: ${sql}")(output)
  }

  test("cast null as double") {
    val result = spark.sql("select cast(null as double);")
    val output = result.collect().toSeq.head.getDouble(0)
    assertResult(null, s"sql: ${sql}")(output)
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
}
