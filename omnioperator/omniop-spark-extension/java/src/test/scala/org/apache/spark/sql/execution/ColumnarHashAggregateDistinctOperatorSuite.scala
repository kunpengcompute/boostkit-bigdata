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

package org.apache.spark.sql.execution

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.types._

class ColumnarHashAggregateDistinctOperatorSuite extends ColumnarSparkPlanTest {

  private var dealer: DataFrame = _
  private var dealer_decimal: DataFrame = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()

    dealer = spark.createDataFrame(
      sparkContext.parallelize(Seq(
        Row(100, "Fremont", "Honda Civic", 10),
        Row(100, "Fremont", "Honda Accord", null),
        Row(100, "Fremont", "Honda CRV", 7),
        Row(200, "Dublin", "Honda Civic", 20),
        Row(200, "Dublin", "Honda Civic", null),
        Row(200, "Dublin", "Honda Accord", 3),
        Row(300, "San Jose", "Honda Civic", 5),
        Row(300, "San Jose", "Honda Accord", null)
      )), new StructType()
        .add("id", IntegerType)
        .add("city", StringType)
        .add("car_model", StringType)
        .add("quantity", IntegerType))
    dealer.createOrReplaceTempView("dealer")

    dealer_decimal = spark.createDataFrame(
      sparkContext.parallelize(Seq(
        Row(100, "Fremont", "Honda Civic", BigDecimal("123456.78"), null, BigDecimal("1234567891234567.89")),
        Row(100, "Fremont", "Honda Accord", BigDecimal("456.78"), BigDecimal("456789.12"), null),
        Row(100, "Fremont", "Honda CRV", BigDecimal("6.78"), BigDecimal("6789.12"), BigDecimal("67891234567.89")),
        Row(200, "Dublin", "Honda Civic", BigDecimal("123456.78"), null, BigDecimal("1234567891234567.89")),
        Row(200, "Dublin", "Honda Accord", BigDecimal("6.78"), BigDecimal("9.12"), BigDecimal("567.89")),
        Row(200, "Dublin", "Honda CRV", BigDecimal("123456.78"), BigDecimal("123456789.12"), null),
        Row(300, "San Jose", "Honda Civic", BigDecimal("3456.78"), null, BigDecimal("34567891234567.89")),
        Row(300, "San Jose", "Honda Accord", BigDecimal("56.78"), BigDecimal("56789.12"), null)
      )), new StructType()
        .add("id", IntegerType)
        .add("city", StringType)
        .add("car_model", StringType)
        .add("quantity_dec8_2", DecimalType(8, 2))
        .add("quantity_dec11_2", DecimalType(11, 2))
        .add("quantity_dec18_2", DecimalType(18, 2)))
    dealer_decimal.createOrReplaceTempView("dealer_decimal")
  }

  test("Test HashAgg with 1 distinct:") {
    val sql1 = "SELECT car_model, count(DISTINCT quantity) AS count FROM dealer" +
      " GROUP BY car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql1)

    val sql2 = "SELECT car_model, avg(DISTINCT quantity) AS count FROM dealer" +
      " GROUP BY car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql2)

    val sql3 = "SELECT car_model, sum(DISTINCT quantity) AS count FROM dealer" +
      " GROUP BY car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql3)

    val sql4 = "SELECT car_model, count(DISTINCT quantity) AS count FROM dealer" +
      " GROUP BY car_model order by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql4)

    val sql5 = "SELECT car_model, avg(DISTINCT quantity) AS count FROM dealer" +
      " GROUP BY car_model order by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql5)

    val sql6 = "SELECT car_model, sum(DISTINCT quantity) AS count FROM dealer" +
      " GROUP BY car_model order by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql6)
  }

  test("Test HashAgg with 1 distinct + 1 without distinct:") {
    val sql1 = "SELECT car_model, max(id), count(DISTINCT quantity) AS count FROM dealer" +
      " GROUP BY car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql1)

    val sql2 = "SELECT car_model, count(id), avg(DISTINCT quantity) AS count FROM dealer" +
      " GROUP BY car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql2)

    val sql3 = "SELECT car_model, min(id), sum(DISTINCT quantity) AS count FROM dealer" +
      " GROUP BY car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql3)

    val sql4 = "SELECT car_model, max(id), count(DISTINCT quantity) AS count FROM dealer" +
      " GROUP BY car_model order by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql4)

    val sql5 = "SELECT car_model, count(id), avg(DISTINCT quantity) AS count FROM dealer" +
      " GROUP BY car_model order by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql5)

    val sql6 = "SELECT car_model, min(id), sum(DISTINCT quantity) AS count FROM dealer" +
      " GROUP BY car_model order by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql6)
  }

  test("Test HashAgg with multi distinct + multi without distinct:") {
    val sql1 = "select car_model, min(id), max(quantity), count(distinct city) from dealer" +
      " group by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql1)

    val sql2 = "select car_model, avg(DISTINCT quantity), count(DISTINCT city) from dealer" +
      " group by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql2)

    val sql3 = "select car_model, sum(DISTINCT quantity), count(DISTINCT city) from dealer" +
      " group by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql3)

    val sql4 = "select car_model, avg(DISTINCT quantity), sum(DISTINCT city) from dealer" +
      " group by car_model;"
    // sum(DISTINCT city) have knownfloatingpointnormalized(normalizenanandzero(cast(city as double)))
    // not support, HashAggExec will partial replace
    assertHashAggregateExecOmniAndSparkResultEqual(sql4, false)

    val sql5 = "select car_model, count(DISTINCT city), avg(DISTINCT quantity), sum(DISTINCT city) from dealer" +
      " group by car_model;"
    // sum(DISTINCT city) have knownfloatingpointnormalized(normalizenanandzero(cast(city as double)))
    // not support, HashAggExec will partial replace
    assertHashAggregateExecOmniAndSparkResultEqual(sql5, false)

    val sql6 = "select car_model, min(id), sum(DISTINCT quantity), count(DISTINCT city) from dealer" +
      " group by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql6)

    val sql7 = "select car_model, sum(DISTINCT quantity), count(DISTINCT city), avg(DISTINCT city), min(id), max(id) from dealer" +
      " group by car_model;"
    // avg(DISTINCT city) have knownfloatingpointnormalized(normalizenanandzero(cast(city as double)))
    // not support, HashAggExec will partial replace
    assertHashAggregateExecOmniAndSparkResultEqual(sql7, false)

    val sql8 = "select car_model, min(id), sum(DISTINCT quantity), count(DISTINCT city), avg(DISTINCT city) from dealer" +
      " group by car_model;"
    // avg(DISTINCT city) have knownfloatingpointnormalized(normalizenanandzero(cast(city as double)))
    // not support, HashAggExec will partial replace
    assertHashAggregateExecOmniAndSparkResultEqual(sql8, false)
  }

  test("Test HashAgg with decimal distinct:") {
    val sql1 = "select car_model, avg(DISTINCT quantity_dec8_2), count(DISTINCT city) from dealer_decimal" +
      " group by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql1, hashAggExecFullReplace = false)

    val sql2 = "select car_model, min(id), sum(DISTINCT quantity_dec8_2), count(DISTINCT city) from dealer_decimal" +
      " group by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql2)

    val sql3 = "select car_model, count(DISTINCT quantity_dec8_2), count(DISTINCT city), avg(DISTINCT city), min(id), max(id) from dealer_decimal" +
      " group by car_model;"
    // avg(DISTINCT city) have knownfloatingpointnormalized(normalizenanandzero(cast(city as double)))
    // not support, HashAggExec will partial replace
    assertHashAggregateExecOmniAndSparkResultEqual(sql3, false)

    val sql4 = "select car_model, avg(DISTINCT quantity_dec11_2), count(DISTINCT city) from dealer_decimal" +
      " group by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql4, hashAggExecFullReplace = false)

    val sql5 = "select car_model, min(id), sum(DISTINCT quantity_dec11_2), count(DISTINCT city) from dealer_decimal" +
      " group by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql5)

    val sql6 = "select car_model, count(DISTINCT quantity_dec11_2), count (DISTINCT city), avg(DISTINCT city), min(id), max(id) from dealer_decimal" +
      " group by car_model;"
    // avg(DISTINCT city) have knownfloatingpointnormalized(normalizenanandzero(cast(city as double)))
    // not support, HashAggExec will partial replace
    assertHashAggregateExecOmniAndSparkResultEqual(sql6, false)

    val sql7 = "select car_model, count(DISTINCT quantity_dec8_2), avg(DISTINCT quantity_dec8_2), sum(DISTINCT quantity_dec8_2) from dealer_decimal" +
      " group by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql7, hashAggExecFullReplace = false)

    val sql8 = "select car_model, count(DISTINCT quantity_dec11_2), avg(DISTINCT quantity_dec11_2), sum(DISTINCT quantity_dec11_2) from dealer_decimal" +
      " group by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql8, hashAggExecFullReplace = false)
  }

  test("Test HashAgg with multi distinct + multi without distinct + order by:") {
    val sql1 = "select car_model, min(id), max(quantity), count(distinct city) from dealer" +
      " group by car_model order by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql1)

    val sql2 = "select car_model, avg(DISTINCT quantity), count(DISTINCT city) from dealer" +
      " group by car_model order by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql2)

    val sql3 = "select car_model, sum(DISTINCT quantity), count(DISTINCT city) from dealer" +
      " group by car_model order by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql3)

    val sql4 = "select car_model, avg(DISTINCT quantity), sum(DISTINCT city) from dealer" +
      " group by car_model order by car_model;"
    // sum(DISTINCT city) have knownfloatingpointnormalized(normalizenanandzero(cast(city as double)))
    // not support, HashAggExec will partial replace
    assertHashAggregateExecOmniAndSparkResultEqual(sql4, false)

    val sql5 = "select car_model, count(DISTINCT city), avg(DISTINCT quantity), sum(DISTINCT city) from dealer" +
      " group by car_model order by car_model;"
    // sum(DISTINCT city) have knownfloatingpointnormalized(normalizenanandzero(cast(city as double)))
    // not support, HashAggExec will partial replace
    assertHashAggregateExecOmniAndSparkResultEqual(sql5, false)

    val sql6 = "select car_model, min(id), sum(DISTINCT quantity), count(DISTINCT city) from dealer" +
      " group by car_model order by car_model;"
    // count(DISTINCT city) have knownfloatingpointnormalized(normalizenanandzero(cast(city as double)))
    // not support, HashAggExec will partial replace
    assertHashAggregateExecOmniAndSparkResultEqual(sql6, false)

    val sql7 = "select car_model, sum(DISTINCT quantity), count(DISTINCT city), avg(DISTINCT city), min(id), max(id) from dealer" +
      " group by car_model order by car_model;"
    // avg(DISTINCT city) have knownfloatingpointnormalized(normalizenanandzero(cast(city as double)))
    // not support, HashAggExec will partial replace
    assertHashAggregateExecOmniAndSparkResultEqual(sql7, false)

    val sql8 = "select car_model, min(id), sum(DISTINCT quantity), count(DISTINCT city), avg(DISTINCT city) from dealer" +
      " group by car_model order by car_model;"
    // avg(DISTINCT city) have knownfloatingpointnormalized(normalizenanandzero(cast(city as double)))
    // not support, HashAggExec will partial replace
    assertHashAggregateExecOmniAndSparkResultEqual(sql8, false)
  }

  test("Test HashAgg with 1 distinct + order by:") {
    val sql1 = "SELECT car_model, count(DISTINCT city) AS count FROM dealer" +
      " GROUP BY car_model order by car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql1)
  }

  test("Test HashAgg with 2 distinct: group by columnar as distinct columnar") {
    val sql1 = "SELECT city, car_model, count(DISTINCT city), max(DISTINCT quantity) FROM dealer" +
      " GROUP BY city, car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql1)

    val sql2 = "SELECT city, id, count(DISTINCT id), max(DISTINCT quantity) FROM dealer" +
      " GROUP BY city, id;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql2)

    val sql3 = "SELECT city, quantity_dec8_2, count(DISTINCT quantity_dec8_2), max(DISTINCT id) FROM dealer_decimal" +
      " GROUP BY city, quantity_dec8_2;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql3)

    val sql4 = "SELECT city, quantity_dec11_2, count(DISTINCT quantity_dec11_2), max(DISTINCT id) FROM dealer_decimal" +
      " GROUP BY city, quantity_dec11_2;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql4)

    val sql5 = "SELECT city, quantity_dec18_2, count(DISTINCT quantity_dec18_2), max(DISTINCT id) FROM dealer_decimal" +
      " GROUP BY city, quantity_dec18_2;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql5)
  }

  test("Test HashAgg with aggkey expresion + 2 distinct: ") {
    val sql1 = "SELECT car_model, city, count(DISTINCT concat(city,car_model)), max(DISTINCT quantity)" +
      " FROM dealer GROUP BY city, car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql1)

    val sql2 = "SELECT city, concat(city,car_model), count(DISTINCT concat(city,car_model)), max(DISTINCT quantity)" +
      " FROM dealer GROUP BY city, car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql2)

    val sql3 = "SELECT city, id, count (DISTINCT id + 3), max(DISTINCT quantity + 1)" +
      " FROM dealer GROUP BY city, id;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql3)

    val sql4 = "SELECT city, quantity_dec8_2, count(DISTINCT quantity_dec8_2 + 1.00), max(DISTINCT id)" +
      " FROM dealer_decimal GROUP BY city, quantity_dec8_2;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql4)

    val sql5 = "SELECT city, quantity_dec11_2, count(DISTINCT quantity_dec11_2 + 1.00), max(DISTINCT id)" +
      " FROM dealer_decimal GROUP BY city, quantity_dec11_2;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql5)

    val sql6 = "SELECT city, quantity_dec18_2, count(DISTINCT quantity_dec18_2 + 1.00), max(DISTINCT id)" +
      " FROM dealer_decimal GROUP BY city, quantity_dec18_2;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql6)
  }

  test("Test HashAgg with 2 distinct + order by: group by columnar as distinct columnar") {
    val sql1 = "SELECT city, car_model, count(DISTINCT city), max(DISTINCT quantity) FROM dealer" +
      " GROUP BY city, car_model order by city, car_model;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql1)

    val sql2 = "SELECT city, id, count(DISTINCT id), max(DISTINCT quantity) FROM dealer" +
      " GROUP BY city, id order by city, id;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql2)

    val sql3 = "SELECT city, quantity_dec8_2, count(DISTINCT quantity_dec8_2), max(DISTINCT id)" +
      " FROM dealer_decimal GROUP BY city, quantity_dec8_2 order BY city, quantity_dec8_2;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql3)

    val sql4 = "SELECT city, quantity_dec11_2, count(DISTINCT quantity_dec11_2), max(DISTINCT id)" +
      " FROM dealer_decimal GROUP BY city, quantity_dec11_2 order BY city, quantity_dec11_2;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql4)

    val sql5 = "SELECT city, quantity_dec18_2, count(DISTINCT quantity_dec18_2), max(DISTINCT id)" +
      " FROM dealer_decimal GROUP BY city, quantity_dec18_2 order BY city, quantity_dec18_2;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql5)
  }

  test("Test HashAgg with aggkey expresion + 2 distinct + order by:") {
    val sql1 = "SELECT city, quantity_dec18_2, count(DISTINCT quantity_dec18_2 + 1.00), max(DISTINCT id)" +
      " FROM dealer_decimal GROUP BY city, quantity_dec18_2 order BY city, quantity_dec18_2;"
    assertHashAggregateExecOmniAndSparkResultEqual(sql1)
  }

  private def assertHashAggregateExecOmniAndSparkResultEqual(sql: String, hashAggExecFullReplace: Boolean = true): Unit = {
    // run ColumnarHashAggregateExec config
    spark.conf.set("spark.omni.sql.columnar.hashagg", true)
    val omniResult = spark.sql(sql)
    val omniPlan = omniResult.queryExecution.executedPlan
    assert(omniPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isDefined,
      s"SQL:${sql}\n@OmniEnv no ColumnarHashAggregateExec,omniPlan:${omniPlan}")
    if (hashAggExecFullReplace) {
      assert(omniPlan.find(_.isInstanceOf[HashAggregateExec]).isEmpty,
        s"SQL:${sql}\n@OmniEnv have HashAggregateExec,omniPlan:${omniPlan}")
    }

    // run HashAggregateExec config
    spark.conf.set("spark.omni.sql.columnar.hashagg", false)
    val sparkResult = spark.sql(sql)
    val sparkPlan = sparkResult.queryExecution.executedPlan
    assert(sparkPlan.find(_.isInstanceOf[ColumnarHashAggregateExec]).isEmpty,
      s"SQL:${sql}\n@SparkEnv have ColumnarHashAggregateExec,sparkPlan:${sparkPlan}")
    assert(sparkPlan.find(_.isInstanceOf[HashAggregateExec]).isDefined,
      s"SQL:${sql}\n@SparkEnv no HashAggregateExec,sparkPlan:${sparkPlan}")
    // DataFrame do not support comparing with equals method, use DataFrame.except instead
    // DataFrame.except can do equal for rows misorder(with and without order by are same)
    assert(omniResult.except(sparkResult).isEmpty,
      s"SQL:${sql}\nomniResult:${omniResult.show()}\nsparkResult:${sparkResult.show()}\n")
    spark.conf.set("spark.omni.sql.columnar.hashagg", true)
  }
}
