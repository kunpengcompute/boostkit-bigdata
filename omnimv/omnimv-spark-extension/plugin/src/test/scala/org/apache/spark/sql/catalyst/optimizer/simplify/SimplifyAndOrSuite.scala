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

package org.apache.spark.sql.catalyst.optimizer.simplify

import com.huawei.boostkit.spark.util.ExprSimplifier

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.rules.RewriteSuite
import org.apache.spark.sql.catalyst.optimizer.rules.RewriteSuite._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}


class SimplifyAndOrSuite extends RewriteSuite {

  val pulledUpPredicates: Set[Expression] = Set()

  test("prepare env") {
    // clean
    spark.sql(
      """
        |DROP TABLE IF EXISTS T1;
        |""".stripMargin
    )

    // create table
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS T1 (ID INT, UID INT, DESC STRING);
        |""".stripMargin
    )
  }

  test("simplify_AndOr_01") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID = 5
        |AND (ID = 5 OR ID = 10);
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
        .asInstanceOf[Project].child.asInstanceOf[Filter].condition
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition)
    assert("(spark_catalog.default.t1.`ID` = 5)".equals(res.sql))
  }

  test("simplify_AndOr_02") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID = 6
        |AND (NOT ID = 5 OR ID = 10)
        |AND NOT ID = 5;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
        .asInstanceOf[Project].child.asInstanceOf[Filter].condition
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition)
    assert(("((spark_catalog.default.t1.`ID` = 6) " +
        "AND (NOT (spark_catalog.default.t1.`ID` = 5)))").equals(res.sql))
  }

  test("simplify_AndOr_03") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE (5 = ID OR ID = 6 OR ID = 10)
        |AND (6 = ID OR ID = 5 OR ID = 10 OR 15 = ID);
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
        .asInstanceOf[Project].child.asInstanceOf[Filter].condition
    // set unknownAsFalse = true
    val res = ExprSimplifier.simplify(targetCondition)
    assert(res.sql.split("OR").length == 3)
  }

  test("simplify_AndOr_04") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE UID = 11
        |AND ID = 1998
        |AND (ID = 2002 OR ID = 1998 OR ID = 2000 OR ID = 1999 OR ID = 2001)
        |AND (UID = 11 OR UID = 2 OR UID = 9);
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
        .asInstanceOf[Project].child.asInstanceOf[Filter].condition
    // set unknownAsFalse = true
    val res = ExprSimplifier.simplify(targetCondition)
    assert(res.sql.split("AND").length == 2)
  }

  test("clean env") {
    // clean
    spark.sql(
      """
        |DROP TABLE IF EXISTS T1;
        |""".stripMargin
    )
  }
}
