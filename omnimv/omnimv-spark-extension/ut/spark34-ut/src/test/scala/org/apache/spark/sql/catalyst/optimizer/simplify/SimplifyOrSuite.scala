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


class SimplifyOrSuite extends RewriteSuite {

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

  test("simplify_Or_01") {
    val df = spark.sql(
      """
        |SELECT * FROM T1
        |WHERE FALSE OR ID > 5 OR TRUE;
        |""".stripMargin
    )
    val targetCondition = df.queryExecution.analyzed
    // set unknownAsFalse = true
    val simplify = ExprSimplifier(unknownAsFalse = true, pulledUpPredicates)
    val res = simplify.simplify(targetCondition
        .asInstanceOf[Project].child.asInstanceOf[Filter].condition)
    assert("true".equals(res.sql))
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
