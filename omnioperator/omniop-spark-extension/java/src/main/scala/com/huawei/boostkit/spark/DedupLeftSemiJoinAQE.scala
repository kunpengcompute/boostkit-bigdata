/*
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.spark

import com.huawei.boostkit.spark.util.PhysicalPlanSelector

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.{ExtendedAggUtils, HashAggregateExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.SparkSession

case class DedupLeftSemiJoinAQE(session: SparkSession) extends Rule[SparkPlan] {


  private def matchDedupLeftSemiJoinCondition(shj: ShuffledHashJoinExec): Boolean = {
    val conf = ColumnarPluginConfig.getSessionConf
    val enableDedupLeftSemiJoin: Boolean = conf.enableDedupLeftSemiJoin
    val dedupLeftSemiJoinThreshold: Int = conf.dedupLeftSemiJoinThreshold

    shj.joinType match {
      case LeftSemi if enableDedupLeftSemiJoin && shj.condition.isEmpty && shj.right.isInstanceOf[ShuffleExchangeExec] && (shj.right.output.size >= dedupLeftSemiJoinThreshold) => true
      case _ => false
    }
  }


  override def apply(plan: SparkPlan): SparkPlan = PhysicalPlanSelector.maybe(session, plan) {

    plan.transformDown {
      // Falls back for BroadcastNestedLoopJoinExec
      case shj: ShuffledHashJoinExec if matchDedupLeftSemiJoinCondition(shj) =>
        val shuffleExchange = shj.right.asInstanceOf[ShuffleExchangeExec]
        val toAddAggPlan = shuffleExchange.child
        val partialAgg = PhysicalAggregation.unapply(Aggregate(toAddAggPlan.output, toAddAggPlan.output, toAddAggPlan.logicalLink.get)) match {
          case Some((groupingExpressions, aggExpressions, resultExpressions, _))
            if aggExpressions.forall(expr => expr.isInstanceOf[AggregateExpression]) =>
            ExtendedAggUtils.planPartialAggregateWithoutDistinct(
              ExtendedAggUtils.normalizeGroupingExpressions(groupingExpressions),
              aggExpressions.map(_.asInstanceOf[AggregateExpression]),
              resultExpressions,
              toAddAggPlan).asInstanceOf[HashAggregateExec]
        }
      shj.copy(right = shuffleExchange.copy(child = partialAgg))
    }
  }
}
