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
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}


class SimplifyAndSuite extends RewriteSuite {

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
   * unknownAsFalse = false
   */
  test("simplify_And2_isAlwaysFalse") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID > 5 AND FALSE;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = false
    val simplify = ExprSimplifier(unknownAsFalse = false, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("false"))
  }

  /**
   * unknownAsFalse = false
   */
  test("simplify_And2_isAlwaysTrue") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE TRUE AND TRUE;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = false
    val simplify = ExprSimplifier(unknownAsFalse = false, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("true"))
  }

  /**
   * unknownAsFalse = false
   * test for "if terms containsAllSql notTerms"
   */
  test("simplify_And2_notTerms_01") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE NOT (ID < 1) AND NOT ((ID < 5 AND ID > 10) OR NOT ID < 15);
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = false
    val simplify = ExprSimplifier(unknownAsFalse = false, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("(((spark_catalog.default.t1.`ID` < 15) " +
        "AND (spark_catalog.default.t1.`ID` >= 1)) " +
        "AND ((spark_catalog.default.t1.`ID` >= 5) " +
        "OR (spark_catalog.default.t1.`ID` <= 10)))"))
  }

  /**
   * unknownAsFalse = false
   * test for "if terms containsAllSql notTerms"
   */
  test("simplify_And2_notTerms_02") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID > 10 AND NOT ((ID < 15 AND ID > 10) OR NOT ID < 15);
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = false
    val simplify = ExprSimplifier(unknownAsFalse = false, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("false"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_isAlwaysFalse") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID > 5 AND FALSE;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("false"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_isAlwaysTrue") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE TRUE AND TRUE;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("true"))
  }

  /**
   * unknownAsFalse = true
   * test for "if terms containsAllSql notTerms"
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_NotTerms") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID > 10 AND NOT ((ID < 15 AND ID > 10) OR NOT ID < 15);
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("false"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_notNullOperands") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID > 10 AND isNotNull(ID);
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("(spark_catalog.default.t1.`ID` > 10)"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_nullOperands") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE 10 > Cast(ID as Int) AND UID > 10 AND isNull(UID);
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("false"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_in") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID IN (10, 15) AND isNull(ID);
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("false"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_negatedTerms") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID >= UID AND UID > ID;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("false"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_equalityConstantTerms") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID = 5 AND ID = 6;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("false"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_equality") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID = 5 AND UID = 6 AND ID = UID;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("false"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_equality_02") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID = 5 AND UID = 5 AND ID = UID;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("((spark_catalog.default.t1.`ID` = 5) " +
        "AND (spark_catalog.default.t1.`UID` = 5))"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_equalTo") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID > 5 AND ID = 3;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("false"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_equalTo_02") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID > 5 AND ID = 6;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("(spark_catalog.default.t1.`ID` = 6)"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_lessThan") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID > 5 AND ID < 4;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("false"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_lessThan_02") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID > 5 AND ID < 6;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("((spark_catalog.default.t1.`ID` > 5) " +
        "AND (spark_catalog.default.t1.`ID` < 6))"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_lessThan_03") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID < 5 AND ID < 6;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("(spark_catalog.default.t1.`ID` < 5)"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_lessThan_04") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID < 5 AND ID < 4;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("(spark_catalog.default.t1.`ID` < 4)"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_lessThanOrEqual_01") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID < 5 AND ID <= 4;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("(spark_catalog.default.t1.`ID` <= 4)"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_lessThanOrEqual_02") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID < 4 AND ID <= 4;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("(spark_catalog.default.t1.`ID` < 4)"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_lessThanOrEqual_03") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID > 3 AND ID <= 3;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("false"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_lessThanOrEqual_04") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID > 3 AND ID <= 2;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("false"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_lessThanOrEqual_05") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID > 3 AND ID <= 5;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("((spark_catalog.default.t1.`ID` > 3) " +
        "AND (spark_catalog.default.t1.`ID` <= 5))"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_lessThanOrEqual_06") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID < 8 AND ID <= 5;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("(spark_catalog.default.t1.`ID` <= 5)"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_greaterThan_01") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID < 8 AND ID > 5;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("((spark_catalog.default.t1.`ID` < 8) " +
        "AND (spark_catalog.default.t1.`ID` > 5))"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_greaterThan_02") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID < 3 AND ID > 5;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("false"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_greaterThan_03") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID > 8 AND ID > 5;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("(spark_catalog.default.t1.`ID` > 8)"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_greaterThan_04") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID > 3 AND ID > 5;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("(spark_catalog.default.t1.`ID` > 5)"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_greaterThanOrEqual_01") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID > 5 AND ID >= 5;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("(spark_catalog.default.t1.`ID` > 5)"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_greaterThanOrEqual_02") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID > 8 AND ID >= 5;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("(spark_catalog.default.t1.`ID` > 8)"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_greaterThanOrEqual_03") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID <= 3 AND ID >= 5;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("false"))
  }

  /**
   * unknownAsFalse = true
   */
  test("simplify_simplifyAnd2ForUnknownAsFalse_processRange_greaterThanOrEqual_04") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID < 5 AND ID >= 5;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition.asInstanceOf[Project]
        .child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("false"))
  }

  test("simplify_simplifyAnd2ForUnknownAsFalse_rangeToPoint") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID >= 5 AND ID <= 5;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition
        .asInstanceOf[Project].child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("(spark_catalog.default.t1.`ID` = 5)"))
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
