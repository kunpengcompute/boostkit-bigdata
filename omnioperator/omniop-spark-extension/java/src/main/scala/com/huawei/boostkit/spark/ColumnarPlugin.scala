/*
 * Copyright (C) 2020-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

import com.huawei.boostkit.spark.ColumnarPluginConfig.ENABLE_OMNI_COLUMNAR_TO_ROW
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor
import nova.hetu.omniruntime.memory.MemoryManager

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.{Ascending, DynamicPruningSubquery, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.optimizer.{DelayCartesianProduct, RewriteSelfJoinInInPredicate}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, ColumnarCustomShuffleReaderExec, CustomShuffleReaderExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.{DummyLogicalPlan, ExtendedAggUtils, HashAggregateExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.ColumnarBatchSupportUtil.checkColumnarBatchSupport
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.execution.util.SparkMemoryUtils.addLeakSafeTaskCompletionListener

case class ColumnarPreOverrides() extends Rule[SparkPlan] with PredicateHelper{
  val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf
  val enableColumnarFileScan: Boolean = columnarConf.enableColumnarFileScan
  val enableColumnarProject: Boolean = columnarConf.enableColumnarProject
  val enableColumnarFilter: Boolean = columnarConf.enableColumnarFilter
  val enableColumnarExpand: Boolean = columnarConf.enableColumnarExpand
  val enableColumnarHashAgg: Boolean = columnarConf.enableColumnarHashAgg
  val enableTakeOrderedAndProject: Boolean = columnarConf.enableTakeOrderedAndProject &&
    columnarConf.enableColumnarShuffle
  val enableColumnarBroadcastExchange: Boolean = columnarConf.enableColumnarBroadcastExchange &&
    columnarConf.enableColumnarBroadcastJoin
  val enableColumnarBroadcastJoin: Boolean = columnarConf.enableColumnarBroadcastJoin &&
    columnarConf.enableColumnarBroadcastExchange
  val enableSortMergeJoinFusion: Boolean = columnarConf.enableSortMergeJoinFusion
  val enableColumnarSortMergeJoin: Boolean = columnarConf.enableColumnarSortMergeJoin
  val enableColumnarSort: Boolean = columnarConf.enableColumnarSort
  val enableColumnarWindow: Boolean = columnarConf.enableColumnarWindow
  val enableColumnarShuffle: Boolean = columnarConf.enableColumnarShuffle
  val enableShuffledHashJoin: Boolean = columnarConf.enableShuffledHashJoin
  val enableColumnarUnion: Boolean = columnarConf.enableColumnarUnion
  val enableFusion: Boolean = columnarConf.enableFusion
  var isSupportAdaptive: Boolean = true
  val enableColumnarProjectFusion: Boolean = columnarConf.enableColumnarProjectFusion
  val enableColumnarTopNSort: Boolean = columnarConf.enableColumnarTopNSort
  val topNSortThreshold: Int = columnarConf.topNSortThreshold
  val enableDedupLeftSemiJoin: Boolean = columnarConf.enableDedupLeftSemiJoin
  val dedupLeftSemiJoinThreshold: Int = columnarConf.dedupLeftSemiJoinThreshold
  val enableColumnarCoalesce: Boolean = columnarConf.enableColumnarCoalesce
  val enableRollupOptimization: Boolean = columnarConf.enableRollupOptimization
  val enableLocalColumnarLimit: Boolean = columnarConf.enableLocalColumnarLimit
  val enableGlobalColumnarLimit: Boolean = columnarConf.enableGlobalColumnarLimit

  def apply(plan: SparkPlan): SparkPlan = {
    replaceWithColumnarPlan(plan)
  }

  def setAdaptiveSupport(enable: Boolean): Unit = { isSupportAdaptive = enable }

  def checkBhjRightChild(x: Any): Boolean = {
    x match {
      case _: ColumnarFilterExec | _: ColumnarConditionProjectExec => true
      case _ => false
    }
  }

  def isTopNExpression(e: Expression): Boolean = e match {
    case Alias(child, _) => isTopNExpression(child)
    case WindowExpression(windowFunction, _)
        if windowFunction.isInstanceOf[Rank] =>
      true
    case _ => false
  }

  def isStrictTopN(e: Expression): Boolean = e match {
    case Alias(child, _) => isStrictTopN(child)
    case WindowExpression(windowFunction, _) => windowFunction.isInstanceOf[RowNumber]
  }

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: RowGuard =>
      val actualPlan: SparkPlan = plan.child match {
        case p: BroadcastHashJoinExec =>
          p.withNewChildren(p.children.map {
            case RowGuard(queryStage: BroadcastQueryStageExec) =>
              fallBackBroadcastQueryStage(queryStage)
            case queryStage: BroadcastQueryStageExec =>
              fallBackBroadcastQueryStage(queryStage)
            case plan: BroadcastExchangeExec =>
              // if BroadcastHashJoin is row-based, BroadcastExchange should also be row-based
              RowGuard(plan)
            case other => other
          })
        case p: BroadcastNestedLoopJoinExec =>
          p.withNewChildren(p.children.map {
            case RowGuard(queryStage: BroadcastQueryStageExec) =>
              fallBackBroadcastQueryStage(queryStage)
            case queryStage: BroadcastQueryStageExec =>
              fallBackBroadcastQueryStage(queryStage)
            case plan: BroadcastExchangeExec =>
              // if BroadcastNestedLoopJoin is row-based, BroadcastExchange should also be row-based
              RowGuard(plan)
            case other => other
          })
        case other =>
          other
      }
      logDebug(s"Columnar Processing for ${actualPlan.getClass} is under RowGuard.")
      actualPlan.withNewChildren(actualPlan.children.map(replaceWithColumnarPlan))
    case plan: FileSourceScanExec
      if enableColumnarFileScan && checkColumnarBatchSupport(conf, plan) =>
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarFileSourceScanExec(
        plan.relation,
        plan.output,
        plan.requiredSchema,
        plan.partitionFilters,
        plan.optionalBucketSet,
        plan.optionalNumCoalescedBuckets,
        plan.dataFilters,
        plan.tableIdentifier,
        plan.disableBucketedScan
      )
    case range: RangeExec =>
      new ColumnarRangeExec(range.range)
    case plan: ProjectExec if enableColumnarProject =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
      child match {
        case ColumnarFilterExec(condition, child) =>
          ColumnarConditionProjectExec(plan.projectList, condition, child)
        case join : ColumnarBroadcastHashJoinExec =>
          if (plan.projectList.forall(project => OmniExpressionAdaptor.isSimpleProjectForAll(project)) && enableColumnarProjectFusion) {
            ColumnarBroadcastHashJoinExec(
              join.leftKeys,
              join.rightKeys,
              join.joinType,
              join.buildSide,
              join.condition,
              join.left,
              join.right,
              join.isNullAwareAntiJoin,
              plan.projectList)
          } else {
            ColumnarProjectExec(plan.projectList, child)
          }
        case join : ColumnarShuffledHashJoinExec =>
          if (plan.projectList.forall(project => OmniExpressionAdaptor.isSimpleProjectForAll(project)) && enableColumnarProjectFusion) {
            ColumnarShuffledHashJoinExec(
              join.leftKeys,
              join.rightKeys,
              join.joinType,
              join.buildSide,
              join.condition,
              join.left,
              join.right,
              plan.projectList)
          } else {
            ColumnarProjectExec(plan.projectList, child)
          }
        case join : ColumnarSortMergeJoinExec =>
          if (plan.projectList.forall(project => OmniExpressionAdaptor.isSimpleProjectForAll(project)) && enableColumnarProjectFusion) {
            if(enableSortMergeJoinFusion && join.left.isInstanceOf[SortExec] && join.right.isInstanceOf[SortExec]) {
              val left = replaceWithColumnarPlan(join.left.asInstanceOf[SortExec])
              val right = replaceWithColumnarPlan(join.right.asInstanceOf[SortExec])
              ColumnarSortMergeJoinFusionExec(
                join.leftKeys,
                join.rightKeys,
                join.joinType,
                join.condition,
                left,
                right,
                join.isSkewJoin,
                plan.projectList)
            } else {
              ColumnarSortMergeJoinExec(
                join.leftKeys,
                join.rightKeys,
                join.joinType,
                join.condition,
                join.left,
                join.right,
                join.isSkewJoin,
                plan.projectList)
            }
          } else {
            ColumnarProjectExec(plan.projectList, child)
          }
        case _ =>
          ColumnarProjectExec(plan.projectList, child)
      }
    case plan: FilterExec if enableColumnarFilter =>
      if(enableColumnarTopNSort) {
        val filterExec = plan.transform {
          case f@FilterExec(condition,
          w@WindowExec(Seq(windowExpression), _, orderSpec, sort: SortExec))
            if orderSpec.nonEmpty && isTopNExpression(windowExpression) =>
            var topn = Int.MaxValue
            val nonTopNConditions = splitConjunctivePredicates(condition).filter {
              case LessThan(e: NamedExpression, IntegerLiteral(n))
                if e.exprId == windowExpression.exprId =>
                topn = Math.min(topn, n - 1)
                false
              case LessThanOrEqual(e: NamedExpression, IntegerLiteral(n))
                if e.exprId == windowExpression.exprId =>
                topn = Math.min(topn, n)
                false
              case GreaterThan(IntegerLiteral(n), e: NamedExpression)
                if e.exprId == windowExpression.exprId =>
                topn = Math.min(topn, n - 1)
                false
              case GreaterThanOrEqual(IntegerLiteral(n), e: NamedExpression)
                if e.exprId == windowExpression.exprId =>
                topn = Math.min(topn, n)
                false
              case EqualTo(e: NamedExpression, IntegerLiteral(n))
                if n == 1 && e.exprId == windowExpression.exprId =>
                topn = 1
                false
              case EqualTo(IntegerLiteral(n), e: NamedExpression)
                if n == 1 && e.exprId == windowExpression.exprId =>
                topn = 1
                false
              case _ => true
            }

            if (topn > 0 && topn <= topNSortThreshold) {
              val strictTopN = isStrictTopN(windowExpression)
              val topNSortExec = ColumnarTopNSortExec(
                topn, strictTopN, w.partitionSpec, w.orderSpec, sort.global, replaceWithColumnarPlan(sort.child))
              logInfo(s"Columnar Processing for ${topNSortExec.getClass} is currently supported.")
              val newCondition = if (nonTopNConditions.isEmpty) {
                Literal.TrueLiteral
              } else {
                nonTopNConditions.reduce(And)
              }
              val window = ColumnarWindowExec(w.windowExpression, w.partitionSpec, w.orderSpec, topNSortExec)
              return ColumnarFilterExec(newCondition, window)
            } else {
              logInfo{s"topn: ${topn} is bigger than topNSortThreshold: ${topNSortThreshold}."}
              val child = replaceWithColumnarPlan(f.child)
              return ColumnarFilterExec(f.condition, child)
            }
        }
      }
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarFilterExec(plan.condition, child)

    case plan: ExpandExec if enableColumnarExpand =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarExpandExec(plan.projections, plan.output, child)
    case plan: HashAggregateExec if enableColumnarHashAgg =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
      if (enableFusion && !isSupportAdaptive) {
        if (plan.aggregateExpressions.forall(_.mode == Partial)) {
          child match {
            case proj1 @ ColumnarProjectExec(_,
            join1 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _,
            proj2 @ ColumnarProjectExec(_,
            join2 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _,
            proj3 @ ColumnarProjectExec(_,
            join3 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _,
            proj4 @ ColumnarProjectExec(_,
            join4 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _,
            filter @ ColumnarFilterExec(_,
            scan @ ColumnarFileSourceScanExec(_, _, _, _, _, _, _, _, _)
            ), _, _, _)), _, _, _)), _, _, _)), _, _, _))
              if checkBhjRightChild(
                child.asInstanceOf[ColumnarProjectExec].child.children(1)
                  .asInstanceOf[ColumnarBroadcastExchangeExec].child) =>
              ColumnarMultipleOperatorExec(
                plan,
                proj1,
                join1,
                proj2,
                join2,
                proj3,
                join3,
                proj4,
                join4,
                filter,
                scan.relation,
                plan.output,
                scan.requiredSchema,
                scan.partitionFilters,
                scan.optionalBucketSet,
                scan.optionalNumCoalescedBuckets,
                scan.dataFilters,
                scan.tableIdentifier,
                scan.disableBucketedScan)
            case proj1 @ ColumnarProjectExec(_,
            join1 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _,
            proj2 @ ColumnarProjectExec(_,
            join2 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _,
            proj3 @ ColumnarProjectExec(_,
            join3 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _, _,
            filter @ ColumnarFilterExec(_,
            scan @ ColumnarFileSourceScanExec(_, _, _, _, _, _, _, _, _)), _, _)) , _, _, _)), _, _, _))
              if checkBhjRightChild(
                child.asInstanceOf[ColumnarProjectExec].child.children(1)
                  .asInstanceOf[ColumnarBroadcastExchangeExec].child) =>
              ColumnarMultipleOperatorExec1(
                plan,
                proj1,
                join1,
                proj2,
                join2,
                proj3,
                join3,
                filter,
                scan.relation,
                plan.output,
                scan.requiredSchema,
                scan.partitionFilters,
                scan.optionalBucketSet,
                scan.optionalNumCoalescedBuckets,
                scan.dataFilters,
                scan.tableIdentifier,
                scan.disableBucketedScan)
            case proj1 @ ColumnarProjectExec(_,
            join1 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _,
            proj2 @ ColumnarProjectExec(_,
            join2 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _,
            proj3 @ ColumnarProjectExec(_,
            join3 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _,
            filter @ ColumnarFilterExec(_,
            scan @ ColumnarFileSourceScanExec(_, _, _, _, _, _, _, _, _)), _, _, _)) , _, _, _)), _, _, _))
              if checkBhjRightChild(
                child.asInstanceOf[ColumnarProjectExec].child.children(1)
                  .asInstanceOf[ColumnarBroadcastExchangeExec].child) =>
              ColumnarMultipleOperatorExec1(
                plan,
                proj1,
                join1,
                proj2,
                join2,
                proj3,
                join3,
                filter,
                scan.relation,
                plan.output,
                scan.requiredSchema,
                scan.partitionFilters,
                scan.optionalBucketSet,
                scan.optionalNumCoalescedBuckets,
                scan.dataFilters,
                scan.tableIdentifier,
                scan.disableBucketedScan)
            case _ =>
              new ColumnarHashAggregateExec(
                plan.requiredChildDistributionExpressions,
                plan.groupingExpressions,
                plan.aggregateExpressions,
                plan.aggregateAttributes,
                plan.initialInputBufferOffset,
                plan.resultExpressions,
                child)
          }
        } else {
          new ColumnarHashAggregateExec(
            plan.requiredChildDistributionExpressions,
            plan.groupingExpressions,
            plan.aggregateExpressions,
            plan.aggregateAttributes,
            plan.initialInputBufferOffset,
            plan.resultExpressions,
            child)
        }
      } else {
        if (child.isInstanceOf[ColumnarExpandExec]) {
          var columnarExpandExec = child.asInstanceOf[ColumnarExpandExec]
          val matchRollupOptimization: Boolean = columnarExpandExec.matchRollupOptimization()
          if (matchRollupOptimization && enableRollupOptimization) {
            // The sparkPlan: ColumnarExpandExec -> ColumnarHashAggExec => ColumnarExpandExec -> ColumnarHashAggExec -> ColumnarOptRollupExec.
            // ColumnarHashAggExec handles the first combination by Partial mode, i.e. projections[0].
            // ColumnarOptRollupExec handles the residual combinations by PartialMerge mode, i.e. projections[1]~projections[n].
            val projections = columnarExpandExec.projections
            val headProjections = projections.slice(0, 1)
            var residualProjections = projections.slice(1, projections.length)
            // replace parameters
            columnarExpandExec = columnarExpandExec.replace(headProjections)

            // partial
            val partialHashAggExec = new ColumnarHashAggregateExec(
              plan.requiredChildDistributionExpressions,
              plan.groupingExpressions,
              plan.aggregateExpressions,
              plan.aggregateAttributes,
              plan.initialInputBufferOffset,
              plan.resultExpressions,
              columnarExpandExec)


            // If the aggregator has an expression, more than one column in the projection is used
            // for expression calculation. Meanwhile, If the single distinct syntax exists, the
            // sequence of group columns is disordered. Therefore, we need to calculate the sequence
            // of expandSeq first to ensure the project operator correctly processes the columns.
            val expectSeq = plan.resultExpressions
            val expandSeq = columnarExpandExec.output
            // the processing sequences of expandSeq
            residualProjections = residualProjections.map(projection => {
              val indexSeq: Seq[Expression] = expectSeq.map(expectExpr => {
                val index = expandSeq.indexWhere(expandExpr => expectExpr.exprId.equals(expandExpr.exprId))
                if (index != -1) {
                  projection.apply(index) match {
                    case literal: Literal => literal
                    case _ => expectExpr
                  }
                } else {
                  expectExpr
                }
              })
              indexSeq
            })

            // partial merge
            val groupingExpressions = plan.resultExpressions.slice(0, plan.groupingExpressions.length)
            val aggregateExpressions = plan.aggregateExpressions.map(expr => {
              expr.copy(expr.aggregateFunction, PartialMerge, expr.isDistinct, expr.filter, expr.resultId)
            })

            // need ExpandExec parameters and HashAggExec parameters
            new ColumnarOptRollupExec(
              residualProjections,
              plan.output,
              groupingExpressions,
              aggregateExpressions,
              plan.aggregateAttributes,
              partialHashAggExec)
          } else {
            new ColumnarHashAggregateExec(
              plan.requiredChildDistributionExpressions,
              plan.groupingExpressions,
              plan.aggregateExpressions,
              plan.aggregateAttributes,
              plan.initialInputBufferOffset,
              plan.resultExpressions,
              child)
          }
        } else {
          new ColumnarHashAggregateExec(
            plan.requiredChildDistributionExpressions,
            plan.groupingExpressions,
            plan.aggregateExpressions,
            plan.aggregateAttributes,
            plan.initialInputBufferOffset,
            plan.resultExpressions,
            child)
        }
      }

    case plan: TakeOrderedAndProjectExec if enableTakeOrderedAndProject =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarTakeOrderedAndProjectExec(
        plan.limit,
        plan.sortOrder,
        plan.projectList,
        child)
    case plan: BroadcastExchangeExec if enableColumnarBroadcastExchange =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
      new ColumnarBroadcastExchangeExec(plan.mode, child)
    case plan: BroadcastHashJoinExec if enableColumnarBroadcastJoin =>
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
      val left = replaceWithColumnarPlan(plan.left)
      val right = replaceWithColumnarPlan(plan.right)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarBroadcastHashJoinExec(
        plan.leftKeys,
        plan.rightKeys,
        plan.joinType,
        plan.buildSide,
        plan.condition,
        left,
        right)
    case plan: ShuffledHashJoinExec if enableShuffledHashJoin =>
      val left = replaceWithColumnarPlan(plan.left)
      val right = replaceWithColumnarPlan(plan.right)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarShuffledHashJoinExec(
        plan.leftKeys,
        plan.rightKeys,
        plan.joinType,
        plan.buildSide,
        plan.condition,
        left,
        right)
    // DeduplicateRightSideOfLeftSemiJoin Rule works only for Spark 3.1.
    case plan: SortMergeJoinExec if enableColumnarSortMergeJoin && enableDedupLeftSemiJoin => {
      plan.joinType match {
        case LeftSemi => {
          if (plan.condition.isEmpty && plan.left.isInstanceOf[SortExec] && plan.right.isInstanceOf[SortExec]
            && plan.right.asInstanceOf[SortExec].child.isInstanceOf[ShuffleExchangeExec]) {
            val nextChild = plan.right.asInstanceOf[SortExec].child.asInstanceOf[ShuffleExchangeExec].child
            if (nextChild.output.size >= dedupLeftSemiJoinThreshold) {
              nextChild match {
                case ProjectExec(_, BroadcastHashJoinExec(_, _, _, _, _, _, _, _)) => {
                  val left = replaceWithColumnarPlan(plan.left)
                  val val1 = replaceWithColumnarPlan(nextChild.asInstanceOf[ProjectExec])
                  val partialAgg = PhysicalAggregation.unapply(Aggregate(nextChild.output, nextChild.output,
                    new DummyLogicalPlan)) match {
                    case Some((groupingExpressions, aggExpressions, resultExpressions, _))
                      if aggExpressions.forall(expr => expr.isInstanceOf[AggregateExpression]) =>
                      ExtendedAggUtils.planPartialAggregateWithoutDistinct(
                        ExtendedAggUtils.normalizeGroupingExpressions(groupingExpressions),
                        aggExpressions.map(_.asInstanceOf[AggregateExpression]),
                        resultExpressions,
                        val1)
                  }

                  if (partialAgg.isInstanceOf[HashAggregateExec]) {
                    val newHashAgg = new ColumnarHashAggregateExec(
                      partialAgg.asInstanceOf[HashAggregateExec].requiredChildDistributionExpressions,
                      partialAgg.asInstanceOf[HashAggregateExec].groupingExpressions,
                      partialAgg.asInstanceOf[HashAggregateExec].aggregateExpressions,
                      partialAgg.asInstanceOf[HashAggregateExec].aggregateAttributes,
                      partialAgg.asInstanceOf[HashAggregateExec].initialInputBufferOffset,
                      partialAgg.asInstanceOf[HashAggregateExec].resultExpressions,
                      val1)

                    val newShuffle = new ColumnarShuffleExchangeExec(
                      plan.right.asInstanceOf[SortExec].child.asInstanceOf[ShuffleExchangeExec].outputPartitioning,
                      newHashAgg,
                      plan.right.asInstanceOf[SortExec].child.asInstanceOf[ShuffleExchangeExec].shuffleOrigin
                    )
                    val newSort = new ColumnarSortExec(
                      plan.right.asInstanceOf[SortExec].sortOrder,
                      plan.right.asInstanceOf[SortExec].global,
                      newShuffle,
                      plan.right.asInstanceOf[SortExec].testSpillFrequency)
                    ColumnarSortMergeJoinExec(
                      plan.leftKeys,
                      plan.rightKeys,
                      plan.joinType,
                      plan.condition,
                      left,
                      newSort,
                      plan.isSkewJoin)
                  } else {
                    logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
                    if (enableSortMergeJoinFusion && plan.left.isInstanceOf[SortExec]
                      && plan.right.isInstanceOf[SortExec]) {
                      val left = replaceWithColumnarPlan(plan.left.asInstanceOf[SortExec].child)
                      val right = replaceWithColumnarPlan(plan.right.asInstanceOf[SortExec].child)
                      new ColumnarSortMergeJoinFusionExec(
                        plan.leftKeys,
                        plan.rightKeys,
                        plan.joinType,
                        plan.condition,
                        left,
                        right,
                        plan.isSkewJoin)
                    } else {
                      val left = replaceWithColumnarPlan(plan.left)
                      val right = replaceWithColumnarPlan(plan.right)
                      new ColumnarSortMergeJoinExec(
                        plan.leftKeys,
                        plan.rightKeys,
                        plan.joinType,
                        plan.condition,
                        left,
                        right,
                        plan.isSkewJoin)
                    }
                  }
                }
                case _ => {
                  logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
                  if (enableSortMergeJoinFusion && plan.left.isInstanceOf[SortExec]
                    && plan.right.isInstanceOf[SortExec]) {
                    val left = replaceWithColumnarPlan(plan.left.asInstanceOf[SortExec].child)
                    val right = replaceWithColumnarPlan(plan.right.asInstanceOf[SortExec].child)
                    new ColumnarSortMergeJoinFusionExec(
                      plan.leftKeys,
                      plan.rightKeys,
                      plan.joinType,
                      plan.condition,
                      left,
                      right,
                      plan.isSkewJoin)
                  } else {
                    val left = replaceWithColumnarPlan(plan.left)
                    val right = replaceWithColumnarPlan(plan.right)
                    new ColumnarSortMergeJoinExec(
                      plan.leftKeys,
                      plan.rightKeys,
                      plan.joinType,
                      plan.condition,
                      left,
                      right,
                      plan.isSkewJoin)
                  }
                }
              }
            } else {
              logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
              if (enableSortMergeJoinFusion && plan.left.isInstanceOf[SortExec] && plan.right.isInstanceOf[SortExec]) {
                val left = replaceWithColumnarPlan(plan.left.asInstanceOf[SortExec].child)
                val right = replaceWithColumnarPlan(plan.right.asInstanceOf[SortExec].child)
                new ColumnarSortMergeJoinFusionExec(
                  plan.leftKeys,
                  plan.rightKeys,
                  plan.joinType,
                  plan.condition,
                  left,
                  right,
                  plan.isSkewJoin)
              } else {
                val left = replaceWithColumnarPlan(plan.left)
                val right = replaceWithColumnarPlan(plan.right)
                new ColumnarSortMergeJoinExec(
                  plan.leftKeys,
                  plan.rightKeys,
                  plan.joinType,
                  plan.condition,
                  left,
                  right,
                  plan.isSkewJoin)
              }
            }
          } else {
            logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
            if (enableSortMergeJoinFusion && plan.left.isInstanceOf[SortExec] && plan.right.isInstanceOf[SortExec]) {
              val left = replaceWithColumnarPlan(plan.left.asInstanceOf[SortExec].child)
              val right = replaceWithColumnarPlan(plan.right.asInstanceOf[SortExec].child)
              new ColumnarSortMergeJoinFusionExec(
                plan.leftKeys,
                plan.rightKeys,
                plan.joinType,
                plan.condition,
                left,
                right,
                plan.isSkewJoin)
            } else {
              val left = replaceWithColumnarPlan(plan.left)
              val right = replaceWithColumnarPlan(plan.right)
              new ColumnarSortMergeJoinExec(
                plan.leftKeys,
                plan.rightKeys,
                plan.joinType,
                plan.condition,
                left,
                right,
                plan.isSkewJoin)
            }
          }
        }
        case _ => {
          logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
          if (enableSortMergeJoinFusion && plan.left.isInstanceOf[SortExec] && plan.right.isInstanceOf[SortExec]) {
            val left = replaceWithColumnarPlan(plan.left.asInstanceOf[SortExec].child)
            val right = replaceWithColumnarPlan(plan.right.asInstanceOf[SortExec].child)
            new ColumnarSortMergeJoinFusionExec(
              plan.leftKeys,
              plan.rightKeys,
              plan.joinType,
              plan.condition,
              left,
              right,
              plan.isSkewJoin)
          } else {
            val left = replaceWithColumnarPlan(plan.left)
            val right = replaceWithColumnarPlan(plan.right)
            new ColumnarSortMergeJoinExec(
              plan.leftKeys,
              plan.rightKeys,
              plan.joinType,
              plan.condition,
              left,
              right,
              plan.isSkewJoin)
          }
        }
      }
    }
    case plan: SortMergeJoinExec if enableColumnarSortMergeJoin =>
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
      if (enableSortMergeJoinFusion && plan.left.isInstanceOf[SortExec] && plan.right.isInstanceOf[SortExec]) {
        val left = replaceWithColumnarPlan(plan.left.asInstanceOf[SortExec].child)
        val right = replaceWithColumnarPlan(plan.right.asInstanceOf[SortExec].child)
        new ColumnarSortMergeJoinFusionExec(
          plan.leftKeys,
          plan.rightKeys,
          plan.joinType,
          plan.condition,
          left,
          right,
          plan.isSkewJoin)
      } else {
        val left = replaceWithColumnarPlan(plan.left)
        val right = replaceWithColumnarPlan(plan.right)
        new ColumnarSortMergeJoinExec(
          plan.leftKeys,
          plan.rightKeys,
          plan.joinType,
          plan.condition,
          left,
          right,
          plan.isSkewJoin)
      }
    case plan: SortExec if enableColumnarSort =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarSortExec(plan.sortOrder, plan.global, child, plan.testSpillFrequency)
    case plan: WindowExec if enableColumnarWindow =>
      val child = replaceWithColumnarPlan(plan.child)
      if (child.output.isEmpty) {
        return plan
      }
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
      child match {
        case ColumnarSortExec(sortOrder, _, sortChild, _) =>
          if (Seq(plan.partitionSpec.map(SortOrder(_, Ascending)) ++ plan.orderSpec) == Seq(sortOrder)) {
            ColumnarWindowExec(plan.windowExpression, plan.partitionSpec, plan.orderSpec, sortChild)
          } else {
            ColumnarWindowExec(plan.windowExpression, plan.partitionSpec, plan.orderSpec, child)
          }
        case _ =>
          ColumnarWindowExec(plan.windowExpression, plan.partitionSpec, plan.orderSpec, child)
      }
    case plan: UnionExec if enableColumnarUnion =>
      val children = plan.children.map(replaceWithColumnarPlan)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarUnionExec(children)
    case plan: ShuffleExchangeExec if enableColumnarShuffle =>
      val child = replaceWithColumnarPlan(plan.child)
      if (child.output.nonEmpty) {
        logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
        new ColumnarShuffleExchangeExec(plan.outputPartitioning, child, plan.shuffleOrigin)
      } else {
        plan
      }
    case plan: CustomShuffleReaderExec if columnarConf.enableColumnarShuffle =>
      plan.child match {
        case shuffle: ColumnarShuffleExchangeExec =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ColumnarCustomShuffleReaderExec(plan.child, plan.partitionSpecs)
        case ShuffleQueryStageExec(_, shuffle: ColumnarShuffleExchangeExec) =>
          logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
          ColumnarCustomShuffleReaderExec(plan.child, plan.partitionSpecs)
        case ShuffleQueryStageExec(_, reused: ReusedExchangeExec) =>
          reused match {
            case ReusedExchangeExec(_, shuffle: ColumnarShuffleExchangeExec) =>
              logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
              ColumnarCustomShuffleReaderExec(
                plan.child,
                plan.partitionSpecs)
            case _ =>
              plan
          }
        case _ =>
          plan
      }
    case plan: LocalLimitExec if enableLocalColumnarLimit =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarLocalLimitExec(plan.limit, child)
    case plan: GlobalLimitExec if enableGlobalColumnarLimit =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarGlobalLimitExec(plan.limit, child)
    case plan: CoalesceExec if enableColumnarCoalesce =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarCoalesceExec(plan.numPartitions, child)
    case p =>
      val children = plan.children.map(replaceWithColumnarPlan)
      logInfo(s"Columnar Processing for ${p.getClass} is currently not supported.")
      p.withNewChildren(children)
  }

  def fallBackBroadcastQueryStage(curPlan: BroadcastQueryStageExec): BroadcastQueryStageExec = {
    curPlan.plan match {
      case originalBroadcastPlan: ColumnarBroadcastExchangeExec =>
        BroadcastQueryStageExec(
          curPlan.id,
          BroadcastExchangeExec(
            originalBroadcastPlan.mode,
            ColumnarBroadcastExchangeAdaptorExec(originalBroadcastPlan, 1)))
      case ReusedExchangeExec(_, originalBroadcastPlan: ColumnarBroadcastExchangeExec) =>
        BroadcastQueryStageExec(
          curPlan.id,
          BroadcastExchangeExec(
            originalBroadcastPlan.mode,
            ColumnarBroadcastExchangeAdaptorExec(curPlan.plan, 1)))
      case _ =>
        curPlan
    }
  }
}

case class ColumnarPostOverrides() extends Rule[SparkPlan] {

  val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf
  var isSupportAdaptive: Boolean = true

  def apply(plan: SparkPlan): SparkPlan = {
    handleColumnarToRowPartialFetch(replaceWithColumnarPlan(plan))
  }

  private def handleColumnarToRowPartialFetch(plan: SparkPlan): SparkPlan = {
    // simple check plan tree have OmniColumnarToRow and no LimitExec and TakeOrderedAndProjectExec plan
    val noPartialFetch = if (plan.find(_.isInstanceOf[OmniColumnarToRowExec]).isDefined) {
        (!plan.find(node =>
            node.isInstanceOf[LimitExec] || node.isInstanceOf[TakeOrderedAndProjectExec] ||
            node.isInstanceOf[SortMergeJoinExec]).isDefined)
    } else {
      false
    }
    val newPlan = plan.transformUp {
      case c: OmniColumnarToRowExec if noPartialFetch =>
        c.copy(c.child, false)
    }
    newPlan
  }

  def setAdaptiveSupport(enable: Boolean): Unit = { isSupportAdaptive = enable }

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: RowToColumnarExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported")
      RowToOmniColumnarExec(child)
    case ColumnarToRowExec(child: ColumnarShuffleExchangeExec) =>
      replaceWithColumnarPlan(child)
    case ColumnarToRowExec(child: ColumnarBroadcastExchangeExec) =>
      replaceWithColumnarPlan(child)
    case plan: ColumnarToRowExec =>
      val child = replaceWithColumnarPlan(plan.child)
      if (conf.getConf(ENABLE_OMNI_COLUMNAR_TO_ROW)) {
        OmniColumnarToRowExec(child)
      } else {
        ColumnarToRowExec(child)
      }
    case r: SparkPlan
      if !r.isInstanceOf[QueryStageExec] && !r.supportsColumnar && r.children.exists(c =>
        c.isInstanceOf[ColumnarToRowExec]) =>
      val children = r.children.map {
        case c: ColumnarToRowExec =>
          val child = replaceWithColumnarPlan(c.child)
          OmniColumnarToRowExec(child)
        case other =>
          replaceWithColumnarPlan(other)
      }
      r.withNewChildren(children)
    case p =>
      val children = p.children.map(replaceWithColumnarPlan)
      p.withNewChildren(children)
  }
}

case class ColumnarOverrideRules(session: SparkSession) extends ColumnarRule with Logging {
  def columnarEnabled: Boolean = session.sqlContext.getConf(
    "org.apache.spark.sql.columnar.enabled", "true").trim.toBoolean

  def rowGuardOverrides: ColumnarGuardRule = ColumnarGuardRule()
  def preOverrides: ColumnarPreOverrides = ColumnarPreOverrides()
  def postOverrides: ColumnarPostOverrides = ColumnarPostOverrides()

  var isSupportAdaptive: Boolean = true

  private def supportAdaptive(plan: SparkPlan): Boolean = {
    // Only QueryStage will have Exchange as Leaf Plan
    val isLeafPlanExchange = plan match {
      case e: Exchange => true
      case other => false
    }
    isLeafPlanExchange || (SQLConf.get.adaptiveExecutionEnabled && (sanityCheck(plan) &&
      !plan.logicalLink.exists(_.isStreaming) &&
      !plan.expressions.exists(_.find(_.isInstanceOf[DynamicPruningSubquery]).isDefined) &&
      plan.children.forall(supportAdaptive)))
  }

  private def sanityCheck(plan: SparkPlan): Boolean =
    plan.logicalLink.isDefined

  override def preColumnarTransitions: Rule[SparkPlan] = plan => {
    if (columnarEnabled) {
      isSupportAdaptive = supportAdaptive(plan)
      val rule = preOverrides
      rule.setAdaptiveSupport(isSupportAdaptive)
      logInfo("Using BoostKit Spark Native Sql Engine Extension ColumnarPreOverrides")
      rule(rowGuardOverrides(plan))
    } else {
      plan
    }
  }

  override def postColumnarTransitions: Rule[SparkPlan] = plan => {
    if (columnarEnabled) {
      val rule = postOverrides
      rule.setAdaptiveSupport(isSupportAdaptive)
      logInfo("Using BoostKit Spark Native Sql Engine Extension ColumnarPostOverrides")
      rule(plan)
    } else {
      plan
    }
  }
}

class ColumnarPlugin extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    logInfo("Using BoostKit Spark Native Sql Engine Extension to Speed Up Your Queries.")
    extensions.injectColumnar(session => ColumnarOverrideRules(session))
    extensions.injectPlannerStrategy(_ => ShuffleJoinStrategy)
    extensions.injectOptimizerRule(_ => RewriteSelfJoinInInPredicate)
    extensions.injectOptimizerRule(_ => DelayCartesianProduct)
  }
}

private class OmniTaskStartExecutorPlugin extends ExecutorPlugin {
  override def onTaskStart(): Unit = {
    addLeakSafeTaskCompletionListener[Unit](_ => {
      MemoryManager.clearMemory()
    })
  }
}

class OmniSparkPlugin extends SparkPlugin {
  override def executorPlugin(): ExecutorPlugin = {
    new OmniTaskStartExecutorPlugin()
  }

  override def driverPlugin(): DriverPlugin = {
    null
  }
}