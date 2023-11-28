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

package org.apache.spark.sql.catalyst.optimizer

import com.huawei.boostkit.spark.ColumnarPluginConfig

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Attribute, CreateNamedStruct, GetStructField, Literal, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class MergeSubqueryFiltersSuite extends PlanTest {

  override def beforeEach(): Unit = {
    CTERelationDef.curId.set(0)
  }

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("MergeSubqueryFilters", Once, MergeSubqueryFilters) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.string)

  private def definitionNode(plan: LogicalPlan, cteIndex: Int) = {
    CTERelationDef(plan, cteIndex, underSubquery = true)
  }

  private def extractorExpression(cteIndex: Int, output: Seq[Attribute], fieldIndex: Int) = {
    GetStructField(ScalarSubquery(CTERelationRef(cteIndex, _resolved = true, output)), fieldIndex)
      .as("scalarsubquery()")
  }

  test("Merging subqueries with different filters") {
    val subquery1 = ScalarSubquery(testRelation.where('b > 0).groupBy()(max('a).as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.where('b < 0).groupBy()(sum('a).as("sum_a")))
    val subquery3 = ScalarSubquery(testRelation.where('b === 0).groupBy()(avg('a).as("avg_a")))
    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2,
        subquery3)

    val correctAnswer = if (ColumnarPluginConfig.getConf.filterMergeEnable) {
      val mergedSubquery = testRelation
        .where('b > 0 || 'b < 0 || 'b === 0)
        .groupBy()(
          max('a, Some('b > 0)).as("max_a"),
          sum('a, Some('b < 0)).as("sum_a"),
          avg('a, Some('b === 0)).as("avg_a"))
        .select(CreateNamedStruct(Seq(
          Literal("max_a"), 'max_a,
          Literal("sum_a"), 'sum_a,
          Literal("avg_a"), 'avg_a
        )).as("mergedValue"))
      val analyzedMergedSubquery = mergedSubquery.analyze
      WithCTE(
        testRelation
          .select(
            extractorExpression(0, analyzedMergedSubquery.output, 0),
            extractorExpression(0, analyzedMergedSubquery.output, 1),
            extractorExpression(0, analyzedMergedSubquery.output, 2)),
        Seq(definitionNode(analyzedMergedSubquery, 0)))
    } else {
      originalQuery
    }

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Merging subqueries with same condition in filter and in having") {
    val subquery1 = ScalarSubquery(testRelation.where('b > 0).groupBy()(max('a).as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.groupBy()(max('a, Some('b > 0)).as("max_a_2")))
    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2)

    val correctAnswer = if (ColumnarPluginConfig.getConf.filterMergeEnable) {
      val mergedSubquery = testRelation
        .groupBy()(
          max('a, Some('b > 0)).as("max_a"))
        .select(CreateNamedStruct(Seq(
          Literal("max_a"), 'max_a)).as("mergedValue"))
      val analyzedMergedSubquery = mergedSubquery.analyze

      WithCTE(testRelation.select(
            extractorExpression(0, analyzedMergedSubquery.output, 0),
            extractorExpression(0, analyzedMergedSubquery.output, 0)),
        Seq(definitionNode(analyzedMergedSubquery, 0)))
    } else {
      originalQuery
    }

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Merging subqueries with different filters, multiple filters propagated") {
    val subquery1 =
      ScalarSubquery(testRelation.where('b > 0).where('c === "a").groupBy()(max('a).as("max_a")))
    val subquery2 =
      ScalarSubquery(testRelation.where('b > 0).where('c === "b").groupBy()(avg('a).as("avg_a")))
    val subquery3 = ScalarSubquery(
      testRelation.where('b < 0).where('c === "c").groupBy()(count('a).as("cnt_a")))
    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2,
        subquery3)

    val correctAnswer = if (ColumnarPluginConfig.getConf.filterMergeEnable) {
      val mergedSubquery = testRelation
        .where('b > 0 || 'b < 0)
        .where('b > 0 && ('c === "a" || 'c === "b") || 'b < 0 && 'c === "c")
        .groupBy()(
          max('a, Some('b > 0 && 'c === "a")).as("max_a"),
          avg('a, Some('b > 0 && 'c === "b")).as("avg_a"),
          count('a, Some('b < 0 && 'c === "c")).as("cnt_a"))
        .select(CreateNamedStruct(Seq(
          Literal("max_a"), 'max_a,
          Literal("avg_a"), 'avg_a,
          Literal("cnt_a"), 'cnt_a
        )).as("mergedValue"))
      val analyzedMergedSubquery = mergedSubquery.analyze

      WithCTE(testRelation.select(
            extractorExpression(0, analyzedMergedSubquery.output, 0),
            extractorExpression(0, analyzedMergedSubquery.output, 1),
            extractorExpression(0, analyzedMergedSubquery.output, 2)),
        Seq(definitionNode(analyzedMergedSubquery, 0)))
    } else {
      originalQuery
    }

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }
}
