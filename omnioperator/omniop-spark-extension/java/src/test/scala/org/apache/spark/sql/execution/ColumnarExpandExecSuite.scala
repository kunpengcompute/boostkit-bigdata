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

package org.apache.spark.sql.execution

import org.apache.spark.sql.{DataFrame, Row}

class ColumnarExpandExecSuite extends ColumnarSparkPlanTest {

  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  private var dealer: DataFrame = _
  private var floatDealer: DataFrame = _
  private var nullDealer: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    dealer = Seq[(Int, String, String, Int)](
      (100, "Fremont", "Honda Civic", 10),
      (100, "Fremont", "Honda Accord", 15),
      (100, "Fremont", "Honda CRV", 7),
      (200, "Dublin", "Honda Civic", 20),
      (200, "Dublin", "Honda Accord", 10),
      (200, "Dublin", "Honda CRV", 3),
      (300, "San Jose", "Honda Civic", 5),
      (300, "San Jose", "Honda Accord", 8),
    ).toDF("id", "city", "car_model", "quantity")
    dealer.createOrReplaceTempView("dealer")

    floatDealer = Seq[(Int, String, String, Float)](
      (100, "Fremont", "Honda Civic", 10.00F),
      (100, "Fremont", "Honda Accord", 15.00F),
      (100, "Fremont", "Honda CRV", 7.00F),
      (200, "Dublin", "Honda Civic", 20.00F),
      (200, "Dublin", "Honda Accord", 10.00F),
      (200, "Dublin", "Honda CRV", 3.00F),
      (300, "San Jose", "Honda Civic", 5.00F),
      (300, "San Jose", "Honda Accord", 8.00F),
    ).toDF("id", "city", "car_model", "quantity")
    floatDealer.createOrReplaceTempView("float_dealer")

    nullDealer = Seq[(Int, String, String, Int)](
      (100, null, "Honda Civic", 10),
      (100, "Fremont", "Honda Accord", 15),
      (100, "Fremont", null, 7),
      (200, "Dublin", "Honda Civic", 20),
      (200, null, "Honda Accord", 10),
      (200, "Dublin", "Honda CRV", 3),
      (300, "San Jose", null, 5),
      (300, "San Jose", "Honda Accord", 8),
      (300, null, null, 8),
    ).toDF("id", "city", "car_model", "quantity")
    nullDealer.createOrReplaceTempView("null_dealer")

  }

  test("use ColumnarExpandExec in Grouping Sets clause when default") {
    val result = spark.sql("SELECT city, car_model, sum(quantity) AS sum FROM dealer " +
      "GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ()) ORDER BY city, car_model;")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarExpandExec]).isDefined)
    assert(plan.find(_.isInstanceOf[ExpandExec]).isEmpty)
  }

  test("use ExpandExec in Grouping Sets clause when SparkExtension rollback") {
    val result = spark.sql("SELECT city, car_model, sum(quantity) AS sum FROM float_dealer " +
      "GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ()) ORDER BY city, car_model;")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarExpandExec]).isEmpty)
    assert(plan.find(_.isInstanceOf[ExpandExec]).isDefined)
  }

  test("use ExpandExec in Grouping Sets clause when spark.omni.sql.columnar.expand=false") {
    spark.conf.set("spark.omni.sql.columnar.expand", false)
    val result = spark.sql("SELECT city, car_model, sum(quantity) AS sum FROM float_dealer " +
      "GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ()) ORDER BY city, car_model;")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarExpandExec]).isEmpty)
    assert(plan.find(_.isInstanceOf[ExpandExec]).isDefined)
    spark.conf.set("spark.omni.sql.columnar.expand", true)
  }

  test("use ColumnarExpandExec in Rollup clause when default") {
    val result = spark.sql("SELECT city, car_model, sum(quantity) AS sum FROM dealer " +
      "GROUP BY ROLLUP(city, car_model) ORDER BY city, car_model;")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarExpandExec]).isDefined)
    assert(plan.find(_.isInstanceOf[ExpandExec]).isEmpty)
  }

  test("use ExpandExec in Rollup clause when SparkExtension rollback") {
    val result = spark.sql("SELECT city, car_model, sum(quantity) AS sum FROM float_dealer " +
      "GROUP BY ROLLUP(city, car_model) ORDER BY city, car_model;")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarExpandExec]).isEmpty)
    assert(plan.find(_.isInstanceOf[ExpandExec]).isDefined)
  }

  test("use ExpandExec in Rollup clause when spark.omni.sql.columnar.expand=false") {
    spark.conf.set("spark.omni.sql.columnar.expand", false)
    val result = spark.sql("SELECT city, car_model, sum(quantity) AS sum FROM float_dealer " +
      "GROUP BY ROLLUP(city, car_model) ORDER BY city, car_model;")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarExpandExec]).isEmpty)
    assert(plan.find(_.isInstanceOf[ExpandExec]).isDefined)
    spark.conf.set("spark.omni.sql.columnar.expand", true)
  }

  test("use ColumnarExpandExec in Cube clause when default") {
    val result = spark.sql("SELECT city, car_model, sum(quantity) AS sum FROM dealer " +
      "GROUP BY CUBE(city, car_model) ORDER BY city, car_model;")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarExpandExec]).isDefined)
    assert(plan.find(_.isInstanceOf[ExpandExec]).isEmpty)
  }

  test("use ExpandExec in Cube clause when SparkExtension rollback") {
    val result = spark.sql("SELECT city, car_model, sum(quantity) AS sum FROM float_dealer " +
      "GROUP BY CUBE(city, car_model) ORDER BY city, car_model;")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarExpandExec]).isEmpty)
    assert(plan.find(_.isInstanceOf[ExpandExec]).isDefined)
  }

  test("use ExpandExec in Cube clause when spark.omni.sql.columnar.expand=false") {
    spark.conf.set("spark.omni.sql.columnar.expand", false)
    val result = spark.sql("SELECT city, car_model, sum(quantity) AS sum FROM float_dealer " +
      "GROUP BY CUBE(city, car_model) ORDER BY city, car_model;")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarExpandExec]).isEmpty)
    assert(plan.find(_.isInstanceOf[ExpandExec]).isDefined)
    spark.conf.set("spark.omni.sql.columnar.expand", true)
  }

  test("ColumnarExpandExec exec correctly in Grouping Sets clause") {
    val result = spark.sql("SELECT city, car_model, sum(quantity) AS sum FROM dealer " +
      "GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ()) ORDER BY city, car_model;")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarExpandExec]).isDefined)

    val expect = Seq(
      Row(null, null, 78),
      Row(null, "Honda Accord", 33),
      Row(null, "Honda CRV", 10),
      Row(null, "Honda Civic", 35),
      Row("Dublin", null, 33),
      Row("Dublin", "Honda Accord", 10),
      Row("Dublin", "Honda CRV", 3),
      Row("Dublin", "Honda Civic", 20),
      Row("Fremont", null, 32),
      Row("Fremont", "Honda Accord", 15),
      Row("Fremont", "Honda CRV", 7),
      Row("Fremont", "Honda Civic", 10),
      Row("San Jose", null, 13),
      Row("San Jose", "Honda Accord", 8),
      Row("San Jose", "Honda Civic", 5),
    )
    checkAnswer(result, expect)
  }

  test("ColumnarExpandExec exec correctly in Rollup clause") {
    val result = spark.sql("SELECT city, car_model, sum(quantity) AS sum FROM dealer " +
      "GROUP BY ROLLUP (city, car_model) ORDER BY city, car_model;")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarExpandExec]).isDefined)

    val expect = Seq(
      Row(null, null, 78),
      Row("Dublin", null, 33),
      Row("Dublin", "Honda Accord", 10),
      Row("Dublin", "Honda CRV", 3),
      Row("Dublin", "Honda Civic", 20),
      Row("Fremont", null, 32),
      Row("Fremont", "Honda Accord", 15),
      Row("Fremont", "Honda CRV", 7),
      Row("Fremont", "Honda Civic", 10),
      Row("San Jose", null, 13),
      Row("San Jose", "Honda Accord", 8),
      Row("San Jose", "Honda Civic", 5),
    )
    checkAnswer(result, expect)
  }

  test("ColumnarExpandExec exec correctly in Cube clause") {
    val result = spark.sql("SELECT city, car_model, sum(quantity) AS sum FROM dealer " +
      "GROUP BY CUBE (city, car_model) ORDER BY city, car_model;")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarExpandExec]).isDefined)

    val expect = Seq(
      Row(null, null, 78),
      Row(null, "Honda Accord", 33),
      Row(null, "Honda CRV", 10),
      Row(null, "Honda Civic", 35),
      Row("Dublin", null, 33),
      Row("Dublin", "Honda Accord", 10),
      Row("Dublin", "Honda CRV", 3),
      Row("Dublin", "Honda Civic", 20),
      Row("Fremont", null, 32),
      Row("Fremont", "Honda Accord", 15),
      Row("Fremont", "Honda CRV", 7),
      Row("Fremont", "Honda Civic", 10),
      Row("San Jose", null, 13),
      Row("San Jose", "Honda Accord", 8),
      Row("San Jose", "Honda Civic", 5),
    )
    checkAnswer(result, expect)
  }

  test("ColumnarExpandExec exec correctly in Grouping Sets clause with GROUPING__ID column") {
    val result = spark.sql("SELECT city, car_model, sum(quantity) AS sum, GROUPING__ID FROM dealer " +
      "GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ()) ORDER BY city, car_model;")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarExpandExec]).isDefined)

    val expect = Seq(
      Row(null, null, 78, 3),
      Row(null, "Honda Accord", 33, 2),
      Row(null, "Honda CRV", 10, 2),
      Row(null, "Honda Civic", 35, 2),
      Row("Dublin", null, 33, 1),
      Row("Dublin", "Honda Accord", 10, 0),
      Row("Dublin", "Honda CRV", 3, 0),
      Row("Dublin", "Honda Civic", 20, 0),
      Row("Fremont", null, 32, 1),
      Row("Fremont", "Honda Accord", 15, 0),
      Row("Fremont", "Honda CRV", 7, 0),
      Row("Fremont", "Honda Civic", 10, 0),
      Row("San Jose", null, 13, 1),
      Row("San Jose", "Honda Accord", 8, 0),
      Row("San Jose", "Honda Civic", 5, 0),
    )
    checkAnswer(result, expect)
  }

  test("ColumnarExpandExec exec correctly in Rollup clause with GROUPING__ID column") {
    val result = spark.sql("SELECT city, car_model, sum(quantity) AS sum, GROUPING__ID FROM dealer " +
      "GROUP BY ROLLUP (city, car_model) ORDER BY city, car_model;")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarExpandExec]).isDefined)

    val expect = Seq(
      Row(null, null, 78, 3),
      Row("Dublin", null, 33, 1),
      Row("Dublin", "Honda Accord", 10, 0),
      Row("Dublin", "Honda CRV", 3, 0),
      Row("Dublin", "Honda Civic", 20, 0),
      Row("Fremont", null, 32, 1),
      Row("Fremont", "Honda Accord", 15, 0),
      Row("Fremont", "Honda CRV", 7, 0),
      Row("Fremont", "Honda Civic", 10, 0),
      Row("San Jose", null, 13, 1),
      Row("San Jose", "Honda Accord", 8, 0),
      Row("San Jose", "Honda Civic", 5, 0),
    )
    checkAnswer(result, expect)
  }

  test("ColumnarExpandExec exec correctly in Cube clause with GROUPING__ID column") {
    val result = spark.sql("SELECT city, car_model, sum(quantity) AS sum, GROUPING__ID FROM dealer " +
      "GROUP BY CUBE (city, car_model) ORDER BY city, car_model;")
    val plan = result.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[ColumnarExpandExec]).isDefined)

    val expect = Seq(
      Row(null, null, 78, 3),
      Row(null, "Honda Accord", 33, 2),
      Row(null, "Honda CRV", 10, 2),
      Row(null, "Honda Civic", 35, 2),
      Row("Dublin", null, 33, 1),
      Row("Dublin", "Honda Accord", 10, 0),
      Row("Dublin", "Honda CRV", 3, 0),
      Row("Dublin", "Honda Civic", 20, 0),
      Row("Fremont", null, 32, 1),
      Row("Fremont", "Honda Accord", 15, 0),
      Row("Fremont", "Honda CRV", 7, 0),
      Row("Fremont", "Honda Civic", 10, 0),
      Row("San Jose", null, 13, 1),
      Row("San Jose", "Honda Accord", 8, 0),
      Row("San Jose", "Honda Civic", 5, 0),
    )
    checkAnswer(result, expect)
  }


  test("ColumnarExpandExec and ExpandExec return the same result when use Grouping Sets clause") {
    val sql = "SELECT city, car_model, sum(quantity) AS sum FROM dealer " +
      "GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ()) ORDER BY city, car_model;"
    checkExpandExecAndColumnarExpandExecAgree(sql)
  }

  test("ColumnarExpandExec and ExpandExec return the same result when use Rollup clause") {
    val sql = "SELECT city, car_model, sum(quantity) AS sum FROM dealer " +
      "GROUP BY ROLLUP(city, car_model) ORDER BY city, car_model;"
    checkExpandExecAndColumnarExpandExecAgree(sql)
  }

  test("ColumnarExpandExec and ExpandExec return the same result when use Cube clause") {
    val sql = "SELECT city, car_model, sum(quantity) AS sum FROM dealer " +
      "GROUP BY CUBE (city, car_model) ORDER BY city, car_model;"
    checkExpandExecAndColumnarExpandExecAgree(sql)
  }

  test("ColumnarExpandExec and ExpandExec return the same result when use Grouping Sets clause with null value") {
    val sql = "SELECT city, car_model, sum(quantity) AS sum FROM null_dealer " +
      "GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ()) ORDER BY city, car_model;"
    checkExpandExecAndColumnarExpandExecAgree(sql)
  }

  test("ColumnarExpandExec and ExpandExec return the same result when use Rollup clause with null value") {
    val sql = "SELECT city, car_model, sum(quantity) AS sum FROM null_dealer " +
      "GROUP BY ROLLUP(city, car_model) ORDER BY city, car_model;"
    checkExpandExecAndColumnarExpandExecAgree(sql)
  }

  test("ColumnarExpandExec and ExpandExec return the same result when use Cube clause with null value") {
    val sql = "SELECT city, car_model, sum(quantity) AS sum FROM null_dealer " +
      "GROUP BY CUBE (city, car_model) ORDER BY city, car_model;"
    checkExpandExecAndColumnarExpandExecAgree(sql)
  }

  test("ColumnarExpandExec and ExpandExec return the same result when use Grouping Sets clause with GROUPING__ID column") {
    val sql = "SELECT city, car_model, sum(quantity) AS sum, GROUPING__ID FROM dealer " +
      "GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ()) ORDER BY city, car_model;"
    checkExpandExecAndColumnarExpandExecAgree(sql)
  }

  test("ColumnarExpandExec and ExpandExec return the same result when use Rollup clause with GROUPING__ID column") {
    val sql = "SELECT city, car_model, sum(quantity) AS sum, GROUPING__ID FROM dealer " +
      "GROUP BY ROLLUP(city, car_model) ORDER BY city, car_model;"
    checkExpandExecAndColumnarExpandExecAgree(sql)
  }
  test("ColumnarExpandExec and ExpandExec return the same result when use Cube clause with GROUPING__ID column") {
    val sql = "SELECT city, car_model, sum(quantity) AS sum, GROUPING__ID FROM dealer " +
      "GROUP BY CUBE (city, car_model) ORDER BY city, car_model;"
    checkExpandExecAndColumnarExpandExecAgree(sql)
  }

  // check ExpandExec and ColumnarExpandExec return the same result
  def checkExpandExecAndColumnarExpandExecAgree(sql: String): Unit = {
    spark.conf.set("spark.omni.sql.columnar.expand", true)
    val omniResult = spark.sql(sql)
    val omniPlan = omniResult.queryExecution.executedPlan
    assert(omniPlan.find(_.isInstanceOf[ColumnarExpandExec]).isDefined)
    assert(omniPlan.find(_.isInstanceOf[ExpandExec]).isEmpty)

    spark.conf.set("spark.omni.sql.columnar.expand", false)
    val sparkResult = spark.sql(sql)
    val sparkPlan = sparkResult.queryExecution.executedPlan
    assert(sparkPlan.find(_.isInstanceOf[ColumnarExpandExec]).isEmpty)
    assert(sparkPlan.find(_.isInstanceOf[ExpandExec]).isDefined)

    // DataFrame do not support comparing with equals method, use DataFrame.except instead
    assert(omniResult.except(sparkResult).isEmpty)

    spark.conf.set("spark.omni.sql.columnar.expand", true)
  }

}

