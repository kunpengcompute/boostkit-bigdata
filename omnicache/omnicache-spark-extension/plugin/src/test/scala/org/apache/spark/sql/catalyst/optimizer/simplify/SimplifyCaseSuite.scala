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
import org.apache.spark.sql.catalyst.plans.logical.Project

class SimplifyCaseSuite extends RewriteSuite {

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

  /**
   * CASE
   * WHEN p1 THEN x1
   * WHEN p2(AlwaysTrue) THEN x2
   * WHEN p3 THEN x3
   * WHEN p4 THEN x4
   * ELSE x5 END
   * can be rewritten to:
   * CASE
   * WHEN p1 THEN x1
   * ELSE x2 END */
  test("simplify_Case_01") {
    val df = spark.sql(
      """
        |SELECT *,
        |CASE WHEN ID = 3 THEN "I am 3."
        |WHEN TRUE THEN "I am 4."
        |WHEN ID = 5 THEN "I am 5."
        |ELSE "other" END FROM T1;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition
        .asInstanceOf[Project].projectList(3).children.head)
    assert(("CASE " +
        "WHEN (spark_catalog.default.t1.`ID` = 3) THEN 'I am 3.' " +
        "ELSE 'I am 4.' END").equals(res.sql))
  }

  /**
   * CASE
   * WHEN p1 THEN x1
   * WHEN p2(AlwaysFalse) THEN x2
   * WHEN p3(AlwaysFalse) THEN x3
   * WHEN p4 THEN x4
   * ELSE x5 END
   * can be rewritten to:
   * CASE
   * WHEN p1 THEN x1
   * WHEN p4 THEN x4
   * ELSE x5 END */
  test("simplify_Case_02") {
    val df = spark.sql(
      """
        |SELECT *,
        |CASE WHEN ID = 3 THEN "I am 3."
        |WHEN FALSE THEN "I am 4."
        |WHEN FALSE THEN "I am 5."
        |WHEN ID = 6 THEN "I am 6."
        |ELSE "other" END FROM T1;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition
        .asInstanceOf[Project].projectList(3).children.head)
    assert(("CASE " +
        "WHEN (spark_catalog.default.t1.`ID` = 3) THEN 'I am 3.' " +
        "WHEN (spark_catalog.default.t1.`ID` = 6) THEN 'I am 6.' " +
        "ELSE 'other' END").equals(res.sql))
  }

  /**
   * CASE
   * WHEN p1(AlwaysTrue) THEN x1(isNotNull)
   * WHEN p2 THEN x2
   * WHEN p3 THEN x3
   * WHEN p4 THEN x4
   * ELSE x5 END
   * can be rewritten to:
   * x1 */
  test("simplify_Case_03") {
    val df = spark.sql(
      """
        |SELECT *,
        |CASE WHEN TRUE THEN "I am 3."
        |WHEN ID = 4 THEN "I am 4."
        |WHEN ID = 5 THEN "I am 5."
        |WHEN ID = 6 THEN "I am 6."
        |ELSE "other" END FROM T1;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition
        .asInstanceOf[Project].projectList(3).children.head)
    assert("'I am 3.'".equals(res.sql))
  }

  /**
   * CASE
   * WHEN p1(AlwaysTrue) THEN x1(isNull)
   * WHEN p2 THEN x2
   * WHEN p3 THEN x3
   * WHEN p4 THEN x4
   * ELSE x5 END
   * can be rewritten to:
   * Literal(False) */
  test("simplify_Case_04") {
    val df = spark.sql(
      """
        |SELECT *,
        |CASE WHEN TRUE THEN NULL
        |WHEN ID = 4 THEN "I am 4."
        |WHEN ID = 5 THEN "I am 5."
        |WHEN ID = 6 THEN "I am 6."
        |ELSE "other" END FROM T1;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition
        .asInstanceOf[Project].projectList(3).children.head)
    assert("false".equals(res.sql))
  }

  /**
   * CASE
   * WHEN p1 THEN TRUE
   * WHEN p2 THEN TRUE
   * ELSE FALSE
   * END
   * can be rewritten to: (p1 or p2) */
  test("simplify_Case_05") {
    val df = spark.sql(
      """
        |SELECT *,
        |CASE WHEN ID = 3 THEN TRUE
        |WHEN ID = 4 THEN TRUE
        |WHEN ID = 5 THEN TRUE
        |ELSE FALSE END FROM T1;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition
        .asInstanceOf[Project].projectList(3).children.head)
    assert(("(((spark_catalog.default.t1.`ID` = 3) " +
        "OR (spark_catalog.default.t1.`ID` = 4)) " +
        "OR (spark_catalog.default.t1.`ID` = 5))").equals(res.sql))
  }

  /**
   * CASE
   * WHEN p1 THEN TRUE
   * WHEN p2 THEN FALSE
   * WHEN p3 THEN TRUE
   * ELSE FALSE
   * END
   * can be rewritten to: (p1 or (p3 and not p2)) */
  test("simplify_Case_06") {
    val df = spark.sql(
      """
        |SELECT *,
        |CASE WHEN 3 = 3 THEN TRUE
        |WHEN 4 = 4 THEN FALSE
        |WHEN 5 = 5 THEN TRUE
        |ELSE FALSE END FROM T1;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition
        .asInstanceOf[Project].projectList(3).children.head)
    assert("((3 = 3) OR ((5 = 5) AND (NOT (4 = 4))))".equals(res.sql))

    val df2 = spark.sql(
      """
        |SELECT *,
        |CASE WHEN 3 = 3 THEN TRUE
        |WHEN 4 = 4 THEN FALSE
        |WHEN 4 = 5 THEN FALSE
        |WHEN 5 = 5 THEN FALSE
        |WHEN 4 = 6 THEN TRUE
        |WHEN 6 = 6 THEN FALSE
        |ELSE FALSE END FROM T1;
        |""".stripMargin
    )
    val targetCondition2 = df2.queryExecution.analyzed
    val res2 = simplify.simplify(targetCondition2
        .asInstanceOf[Project].projectList(3).children.head)
    assert("(((3 = 3) OR ((4 = 6) AND (NOT (5 = 5)))) OR (NOT (6 = 6)))".equals(res2.sql))
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
