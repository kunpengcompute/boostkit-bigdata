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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.catalyst.expressions.{BindReferences, RuntimeFilterExpression, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{RUNTIME_FILTER_EXPRESSION, SUBQUERY_WRAPPER}
import org.apache.spark.sql.execution.{OmniColumnarToRowExec, ColumnarBroadcastExchangeExec, QueryExecution, ScalarSubquery, SparkPlan, ColumnarProjectExec,
  SubqueryAdaptiveBroadcastExec, SubqueryBroadcastExec, SubqueryExec, SubqueryWrapper, ColumnarFilterExec, ColumnarFileSourceScanExec, ColumnarConditionProjectExec}
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode, HashJoin}
import org.apache.spark.sql.execution.BroadcastExchangeExecProxy

/**
 * A rule to insert runtime filter in order to reuse the results of broadcast.
 */
case class PlanAdaptiveRuntimeFilterFilters(
                                             rootPlan: AdaptiveSparkPlanExec) extends Rule[SparkPlan] with AdaptiveSparkPlanHelper {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.runtimeFilterBloomFilterEnabled) {
      return plan
    }

    plan.transformAllExpressionsWithPruning(
      _.containsAllPatterns(RUNTIME_FILTER_EXPRESSION, SUBQUERY_WRAPPER)) {
      case RuntimeFilterExpression(SubqueryWrapper(
      SubqueryAdaptiveBroadcastExec(name, index, true, _, buildKeys,
      adaptivePlan: AdaptiveSparkPlanExec), exprId)) =>
        val filterCreationSidePlan = getFilterCreationSidePlan(adaptivePlan.executedPlan)
        val executedFilterCreationSidePlan =
          QueryExecution.prepareExecutedPlan(adaptivePlan.session, filterCreationSidePlan)

        val newExecutedFilterCreationSidePlan = executedFilterCreationSidePlan transformUp {
          case omniColumnarToRowExec: OmniColumnarToRowExec =>
            omniColumnarToRowExec.child
        }

        val broadcastExchangeNoFallback = checkBroadcastExchangeNoFallback(newExecutedFilterCreationSidePlan)

        val onlyAttributeReference = checkAttributeReference(buildKeys, index)

        val packedKeys = BindReferences.bindReferences(
          HashJoin.rewriteKeyExpr(buildKeys), newExecutedFilterCreationSidePlan.output)
        val mode = HashedRelationBroadcastMode(packedKeys)
        // plan a broadcast exchange of the build side of the join
        val exchange = new ColumnarBroadcastExchangeExec(mode, newExecutedFilterCreationSidePlan)
        val compareExchange = BroadcastExchangeExec(mode, filterCreationSidePlan)

        val canReuseExchange = conf.exchangeReuseEnabled && buildKeys.nonEmpty && broadcastExchangeNoFallback && onlyAttributeReference &&
          find(rootPlan) {
            case BroadcastHashJoinExec(_, _, _, BuildLeft, _, left, _, _) =>
              val executedLeft = QueryExecution.prepareExecutedPlan(adaptivePlan.session, left)
              if (left.isInstanceOf[BroadcastQueryStageExec]) {
                executedLeft.asInstanceOf[BroadcastQueryStageExec].plan.sameResult(exchange)
              } else {
                executedLeft.sameResult(exchange)
              }
            case BroadcastHashJoinExec(_, _, _, BuildRight, _, _, right, _) =>
              val executedRight = QueryExecution.prepareExecutedPlan(adaptivePlan.session, right)
              if (right.isInstanceOf[BroadcastQueryStageExec]) {
                executedRight.asInstanceOf[BroadcastQueryStageExec].plan.sameResult(exchange)
              } else {
                executedRight.sameResult(exchange)
              }
            case _ => false
          }.isDefined

        val bloomFilterSubquery = if (canReuseExchange) {
          exchange.setLogicalLink(filterCreationSidePlan.logicalLink.get)

          val broadcastValues = SubqueryBroadcastExec(name, index, buildKeys, exchange)
          val broadcastProxy =
            BroadcastExchangeExecProxy(broadcastValues, newExecutedFilterCreationSidePlan.output)

          val newExecutedPlan = adaptivePlan.executedPlan transformUp {
            case hashAggregateExec: ObjectHashAggregateExec
              if hashAggregateExec.child.eq(filterCreationSidePlan) =>
              hashAggregateExec.copy(child = broadcastProxy)
          }
          val newAdaptivePlan = adaptivePlan.copy(inputPlan = newExecutedPlan)

          ScalarSubquery(
            SubqueryExec.createForScalarSubquery(
              s"scalar-subquery#${exprId.id}",
              newAdaptivePlan), exprId)
        } else {
          ScalarSubquery(
            SubqueryExec.createForScalarSubquery(
              s"scalar-subquery#${exprId.id}",
              adaptivePlan), exprId)
        }

        RuntimeFilterExpression(bloomFilterSubquery)
    }
  }

  private def getFilterCreationSidePlan(plan: SparkPlan): SparkPlan = {
    plan match {
      case objectHashAggregate: ObjectHashAggregateExec =>
        getFilterCreationSidePlan(objectHashAggregate.child)
      case shuffleExchange: ShuffleExchangeExec =>
        getFilterCreationSidePlan(shuffleExchange.child)
      case queryStageExec: ShuffleQueryStageExec =>
        getFilterCreationSidePlan(queryStageExec.plan)
      case other => other
    }
  }

  private def checkBroadcastExchangeNoFallback(plan: SparkPlan): Boolean = {
    plan match {
      case columnarFilter: ColumnarFilterExec =>
        checkBroadcastExchangeNoFallback(columnarFilter.child)
      case columnarCondition: ColumnarConditionProjectExec =>
        checkBroadcastExchangeNoFallback(columnarCondition.child)
      case columnarProject: ColumnarProjectExec =>
        checkBroadcastExchangeNoFallback(columnarProject.child)
      case columnarFileScan: ColumnarFileSourceScanExec => true
      case other => false
    }
  }

   private def checkAttributeReference(buildKeys: Seq[Expression], index: Int): Boolean = {
    if (index >= 0 && index < buildKeys.length) {
      buildKeys(index).isInstanceOf[AttributeReference]
    } else {
      false
    }
  }
}