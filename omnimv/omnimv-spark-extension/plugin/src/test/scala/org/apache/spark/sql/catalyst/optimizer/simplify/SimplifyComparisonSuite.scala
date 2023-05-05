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

import org.apache.spark.sql.catalyst.expressions.{BinaryComparison, Expression, GreaterThan, LessThan, LessThanOrEqual, Literal}
import org.apache.spark.sql.catalyst.optimizer.rules.RewriteSuite
import org.apache.spark.sql.catalyst.optimizer.rules.RewriteSuite._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}


class SimplifyComparisonSuite extends RewriteSuite {

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

  test("simplify_Comparison_01") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID >= ID;
        |""".stripMargin
    )
    val df2 = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID > ID;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    val targetCondition2 = df2.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition
        .asInstanceOf[Project].child.asInstanceOf[Filter].condition)
    val res2 = simplify.simplify(targetCondition2
        .asInstanceOf[Project].child.asInstanceOf[Filter].condition)
    assert("(spark_catalog.default.t1.`ID` IS NOT NULL)".equals(res.sql))
    assert("false".equals(res2.sql))
  }

  test("simplify_Comparison_02") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE 1 > 2 AND 1 >= 2 AND 1 = 2 AND 1 < 2 AND 1 <= 2;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition
        .asInstanceOf[Project].child.asInstanceOf[Filter].condition)
    assert("false".equals(res.sql))
  }

  /**
   * Include
   * def range(Expression kind, Comparable c)
   * def residue(Expression ref, Range<Comparable> r0, List<Expression> predicates)
   * def createComparison(expr: Expression)
   * ,etc
   * range dependent function.
   */
  test("simplifyUsingPredicates_residue_encloses") {
    // residue return Range.all()
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID < 10;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    var pulledUpPredicates01: Set[Expression] = Set()
    // Assume that pulledUpPredicates is "ID < 5"
    pulledUpPredicates01.+=(LessThan(targetCondition
        .asInstanceOf[Project].child.asInstanceOf[Filter]
        .condition.asInstanceOf[BinaryComparison].left, Literal(5)))
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates01)
    val res = simplify.simplify(targetCondition
        .asInstanceOf[Project].child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("true"))
  }

  /**
   * residue(id > 10, [id < 5]) => False
   */
  test("simplifyUsingPredicates_residue_null") {
    // <residue> Ranges do not intersect. Return null meaning the empty range.
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID > 10;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    var pulledUpPredicates01: Set[Expression] = Set()
    // Assume that pulledUpPredicates is "ID < 5"
    pulledUpPredicates01.+=(LessThan(targetCondition
        .asInstanceOf[Project].child.asInstanceOf[Filter]
        .condition.asInstanceOf[BinaryComparison].left, Literal(5)))
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates01)
    val res = simplify.simplify(targetCondition
        .asInstanceOf[Project].child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("false"))
  }

  /**
   * residue(id > 10, [id > 5]) => id > 10
   */
  test("simplifyUsingPredicates_residue_isConnected") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID > 10;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    var pulledUpPredicates01: Set[Expression] = Set()
    // Assume that pulledUpPredicates is "ID > 5"
    pulledUpPredicates01.+=(GreaterThan(targetCondition
        .asInstanceOf[Project].child.asInstanceOf[Filter]
        .condition.asInstanceOf[BinaryComparison].left, Literal(5)))
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates01)
    val res = simplify.simplify(targetCondition
        .asInstanceOf[Project].child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("(spark_catalog.default.t1.`ID` > 10)"))
  }

  /**
   * residue(id >= 10, [id < 10]) => False
   */
  test("simplifyUsingPredicates_residue_point_open") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID >= 10;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    var pulledUpPredicates01: Set[Expression] = Set()
    // Assume that pulledUpPredicates is "ID < 10"
    pulledUpPredicates01.+=(LessThan(targetCondition
        .asInstanceOf[Project].child.asInstanceOf[Filter]
        .condition.asInstanceOf[BinaryComparison].left, Literal(10)))
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates01)
    val res = simplify.simplify(targetCondition
        .asInstanceOf[Project].child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("false"))
  }

  /**
   * residue(id >= 10, [id <= 10]) => id = 10
   */
  test("simplifyUsingPredicates_residue_point_close") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID >= 10;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    var pulledUpPredicates01: Set[Expression] = Set()
    // Assume that pulledUpPredicates is "ID <= 10"
    pulledUpPredicates01.+=(LessThanOrEqual(targetCondition
        .asInstanceOf[Project].child.asInstanceOf[Filter]
        .condition.asInstanceOf[BinaryComparison].left, Literal(10)))
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates01)
    val res = simplify.simplify(targetCondition
        .asInstanceOf[Project].child.asInstanceOf[Filter].condition)
    assert(res.sql.equals("(spark_catalog.default.t1.`ID` = 10)"))
  }

  /**
   * residue(id < 10, [id > 0]) => not simplify
   */
  test("simplifyUsingPredicates_notWorthSimplifying") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE ID < 10;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    var pulledUpPredicates01: Set[Expression] = Set()
    // Assume that pulledUpPredicates is "ID > 0"
    pulledUpPredicates01.+=(GreaterThan(targetCondition
        .asInstanceOf[Project].child.asInstanceOf[Filter]
        .condition.asInstanceOf[BinaryComparison].left, Literal(0)))
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates01)
    val res = simplify.simplify(targetCondition
        .asInstanceOf[Project].child.asInstanceOf[Filter].condition)
    // range has been reduced but it's not worth simplifying
    assert(res.sql.equals("(spark_catalog.default.t1.`ID` < 10)"))
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
