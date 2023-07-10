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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat
import org.apache.spark.sql.catalyst.statsEstimation.{StatsEstimationTestBase, StatsTestPlan}

class HeuristicJoinReorderSuite
  extends HeuristicJoinReorderPlanTestBase with StatsEstimationTestBase {

  private val columnInfo: AttributeMap[ColumnStat] = AttributeMap(Seq(
    attr("t1.k-1-2") -> rangeColumnStat(2, 0),
    attr("t1.v-1-10") -> rangeColumnStat(10, 0),
    attr("t2.k-1-5") -> rangeColumnStat(5, 0),
    attr("t3.v-1-100") -> rangeColumnStat(100, 0),
    attr("t4.k-1-2") -> rangeColumnStat(2, 0),
    attr("t4.v-1-10") -> rangeColumnStat(10, 0),
    attr("t5.k-1-5") -> rangeColumnStat(5, 0),
    attr("t5.v-1-5") -> rangeColumnStat(5, 0)
  ))

  private val nameToAttr: Map[String, Attribute] = columnInfo.map(kv => kv._1.name -> kv._1)
  private val nameToColInfo: Map[String, (Attribute, ColumnStat)] =
    columnInfo.map(kv => kv._1.name -> kv)

  private val t1 = StatsTestPlan(
    outputList = Seq("t1.k-1-2", "t1.v-1-10").map(nameToAttr),
    rowCount = 1000,
    size = Some(1000 * (8 + 4 + 4)),
    attributeStats = AttributeMap(Seq("t1.k-1-2", "t1.v-1-10").map(nameToColInfo)))

  private val t2 = StatsTestPlan(
    outputList = Seq("t2.k-1-5").map(nameToAttr),
    rowCount = 20,
    size = Some(20 * (8 + 4)),
    attributeStats = AttributeMap(Seq("t2.k-1-5").map(nameToColInfo)))

  private val t3 = StatsTestPlan(
    outputList = Seq("t3.v-1-100").map(nameToAttr),
    rowCount = 100,
    size = Some(100 * (8 + 4)),
    attributeStats = AttributeMap(Seq("t3.v-1-100").map(nameToColInfo)))

  test("reorder 3 tables") {
    val originalPlan =
      t1.join(t2).join(t3)
        .where((nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")) &&
          (nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))

    val analyzed = originalPlan.analyze
    val optimized = HeuristicJoinReorder.apply(analyzed).select(outputsOf(t1, t2, t3): _*)
    val expected =
      t1.join(t2, Inner, Some(nameToAttr("t1.k-1-2") === nameToAttr("t2.k-1-5")))
        .join(t3, Inner, Some(nameToAttr("t1.v-1-10") === nameToAttr("t3.v-1-100")))
        .select(outputsOf(t1, t2, t3): _*)

    assert(equivalentOutput(analyzed, expected))
    assert(equivalentOutput(analyzed, optimized))

    compareJoinOrder(optimized, expected)
  }
}
