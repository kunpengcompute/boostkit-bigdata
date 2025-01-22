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
import com.huawei.boostkit.spark.Constant.OMNI_IS_ADAPTIVE_CONTEXT
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor
import com.huawei.boostkit.spark.util.{ModifyUtilAdaptor, PhysicalPlanSelector}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Ascending, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, IntegerLiteral, LessThan, LessThanOrEqual, Literal, NamedExpression, Rank, RowNumber, SortOrder, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.optimizer.{DelayCartesianProduct, RewriteSelfJoinInInPredicate}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, AdaptiveSparkPlanExec, BroadcastQueryStageExec, OmniAQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.{DummyLogicalPlan, ExtendedAggUtils, HashAggregateExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.window.{WindowExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.execution.util.SparkMemoryUtils.addLeakSafeTaskCompletionListener
import org.apache.spark.sql.execution.aggregate.PushOrderedLimitThroughAgg
import org.apache.spark.sql.util.ShimUtil
import nova.hetu.omniruntime.memory.MemoryManager

import scala.collection.mutable.ListBuffer

case class ColumnarPreOverrides(isSupportAdaptive: Boolean = true)
  extends Rule[SparkPlan] {
  val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf

  val enableColumnarShuffle: Boolean = columnarConf.enableColumnarShuffle
  val enableFusion: Boolean = columnarConf.enableFusion
  val enableColumnarProjectFusion: Boolean = columnarConf.enableColumnarProjectFusion
  val enableDedupLeftSemiJoin: Boolean = columnarConf.enableDedupLeftSemiJoin
  val dedupLeftSemiJoinThreshold: Int = columnarConf.dedupLeftSemiJoinThreshold
  val enableRollupOptimization: Boolean = columnarConf.enableRollupOptimization
  val enableRowShuffle: Boolean = columnarConf.enableRowShuffle
  val columnsThreshold: Int = columnarConf.columnsThreshold
  val enableColumnarDataWritingCommand: Boolean = columnarConf.enableColumnarDataWritingCommand
  val enableColumnarTopNSort: Boolean = columnarConf.enableColumnarTopNSort
  val topNSortThreshold: Int = columnarConf.topNSortThreshold

  private def checkBhjRightChild(plan: Any): Boolean =
    plan match {
      case _: ColumnarFilterExec => true
      case _: ColumnarConditionProjectExec => true
      case _ => false
    }

  def isTopNExpression(expr: Expression): Boolean = expr match {
    case Alias(child, _) => isTopNExpression(child)
    case WindowExpression(_: Rank, _) => true
    case _ => false
  }

  def isStrictTopN(expr: Expression): Boolean = expr match {
    case Alias(child, _) => isStrictTopN(child)
    case WindowExpression(_: RowNumber, _) => true
    case _ => false
  }

  private def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = {
    TransformHints.getHint(plan) match {
      case _: TRANSFORM_SUPPORTED =>
      // supported, break
      case _: TRANSFORM_UNSUPPORTED =>
        logDebug(s"Columnar Processing for ${plan.getClass} is under RowGuard.")
        return plan.withNewChildren(
          plan.children.map(replaceWithColumnarPlan))
    }
    plan match {
      case plan: FileSourceScanExec =>
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
      case plan: ProjectExec =>
        val child = replaceWithColumnarPlan(plan.child)
        logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
        child match {
          case ColumnarFilterExec(condition, child) =>
            ColumnarConditionProjectExec(plan.projectList, condition, child)
          case join: ColumnarBroadcastHashJoinExec =>
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
          case join: ColumnarShuffledHashJoinExec =>
            if (plan.projectList.forall(project => OmniExpressionAdaptor.isSimpleProjectForAll(project)) && enableColumnarProjectFusion) {
              ColumnarShuffledHashJoinExec(
                join.leftKeys,
                join.rightKeys,
                join.joinType,
                ShimUtil.buildBuildSide(join.buildSide, join.joinType),
                join.condition,
                join.left,
                join.right,
                join.isSkewJoin,
                plan.projectList)
            } else {
              ColumnarProjectExec(plan.projectList, child)
            }
          case join: ColumnarBroadcastNestedLoopJoinExec =>
            if (plan.projectList.forall(project => OmniExpressionAdaptor.isSimpleProjectForAll(project)) && enableColumnarProjectFusion) {
              ColumnarBroadcastNestedLoopJoinExec(
                join.left,
                join.right,
                join.buildSide,
                join.joinType,
                join.condition,
                plan.projectList)
            } else {
              ColumnarProjectExec(plan.projectList, child)
            }
          case join: ColumnarSortMergeJoinExec =>
            if (plan.projectList.forall(project => OmniExpressionAdaptor.isSimpleProjectForAll(project)) && enableColumnarProjectFusion) {
              ColumnarSortMergeJoinExec(
                join.leftKeys,
                join.rightKeys,
                join.joinType,
                join.condition,
                join.left,
                join.right,
                join.isSkewJoin,
                plan.projectList)
            } else {
              ColumnarProjectExec(plan.projectList, child)
            }
          case _ =>
            ColumnarProjectExec(plan.projectList, child)
        }
      case plan: FilterExec =>
        if(enableColumnarTopNSort) {
          plan.transform {
            case f@FilterExec(condition,
            w@WindowExec(Seq(windowExpression), _, orderSpec, sort: SortExec))
              if orderSpec.nonEmpty && isTopNExpression(windowExpression) =>
              var topn = Int.MaxValue
              val nonTopNConditions = splitConjunctivePredicates(condition).filter {
                case LessThan(e: NamedExpression, IntegerLiteral(n))
                  if e.exprId == windowExpression.exprId =>
                  topn = Math.min(topn, n - 1)
                  false
                case GreaterThan(IntegerLiteral(n), e: NamedExpression)
                  if e.exprId == windowExpression.exprId =>
                  topn = Math.min(topn, n - 1)
                  false
                case LessThanOrEqual(e: NamedExpression, IntegerLiteral(n))
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
                case GreaterThanOrEqual(IntegerLiteral(n), e: NamedExpression)
                  if e.exprId == windowExpression.exprId =>
                  topn = Math.min(topn, n)
                  false
                case _ => true
              }
              // topn <= SQLConf.get.topNPushDownForWindowThreshold 100.
              val strictTopN = isStrictTopN(windowExpression)
              val omniSupport: Boolean = try {
                ColumnarTopNSortExec(topn, strictTopN, w.partitionSpec, w.orderSpec, sort.global, sort.child).buildCheck()
                true
              } catch {
                case _: Throwable => false
              }
              if (topn > 0 && topn <= topNSortThreshold && omniSupport) {
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
      case plan: ExpandExec =>
        val child = replaceWithColumnarPlan(plan.child)
        logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
        ColumnarExpandExec(plan.projections, plan.output, child)
      case plan: HashAggregateExec =>
        val child = replaceWithColumnarPlan(plan.child)
        logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
        if (enableFusion && !isSupportAdaptive) {
          if (plan.aggregateExpressions.forall(_.mode == Partial)) {
            child match {
              case proj1@ColumnarProjectExec(_,
              join1@ColumnarBroadcastHashJoinExec(_, _, _, _, _,
              proj2@ColumnarProjectExec(_,
              join2@ColumnarBroadcastHashJoinExec(_, _, _, _, _,
              proj3@ColumnarProjectExec(_,
              join3@ColumnarBroadcastHashJoinExec(_, _, _, _, _,
              proj4@ColumnarProjectExec(_,
              join4@ColumnarBroadcastHashJoinExec(_, _, _, _, _,
              filter@ColumnarFilterExec(_,
              scan@ColumnarFileSourceScanExec(_, _, _, _, _, _, _, _, _)
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
              case proj1@ColumnarProjectExec(_,
              join1@ColumnarBroadcastHashJoinExec(_, _, _, _, _,
              proj2@ColumnarProjectExec(_,
              join2@ColumnarBroadcastHashJoinExec(_, _, _, _, _,
              proj3@ColumnarProjectExec(_,
              join3@ColumnarBroadcastHashJoinExec(_, _, _, _, _, _,
              filter@ColumnarFilterExec(_,
              scan@ColumnarFileSourceScanExec(_, _, _, _, _, _, _, _, _)), _, _)), _, _, _)), _, _, _))
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
              case proj1@ColumnarProjectExec(_,
              join1@ColumnarBroadcastHashJoinExec(_, _, _, _, _,
              proj2@ColumnarProjectExec(_,
              join2@ColumnarBroadcastHashJoinExec(_, _, _, _, _,
              proj3@ColumnarProjectExec(_,
              join3@ColumnarBroadcastHashJoinExec(_, _, _, _, _,
              filter@ColumnarFilterExec(_,
              scan@ColumnarFileSourceScanExec(_, _, _, _, _, _, _, _, _)), _, _, _)), _, _, _)), _, _, _))
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
                  plan.isStreaming,
                  plan.numShufflePartitions,
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
              plan.isStreaming,
              plan.numShufflePartitions,
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
                plan.isStreaming,
                plan.numShufflePartitions,
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
                plan.isStreaming,
                plan.numShufflePartitions,
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
              plan.isStreaming,
              plan.numShufflePartitions,
              plan.groupingExpressions,
              plan.aggregateExpressions,
              plan.aggregateAttributes,
              plan.initialInputBufferOffset,
              plan.resultExpressions,
              child)
          }
        }

      case plan@TakeOrderedAndProjectExecShim(_, _, _, _, offset) =>
        val child = replaceWithColumnarPlan(plan.child)
        logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
        ColumnarTakeOrderedAndProjectExec(
          plan.limit,
          plan.sortOrder,
          plan.projectList,
          child,
          offset)
      case plan: BroadcastExchangeExec =>
        val child = replaceWithColumnarPlan(plan.child)
        logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
        new ColumnarBroadcastExchangeExec(plan.mode, child)
      case plan: BroadcastHashJoinExec =>
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
      case plan: BroadcastNestedLoopJoinExec =>
        logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
        val left = replaceWithColumnarPlan(plan.left)
        val right = replaceWithColumnarPlan(plan.right)
        ColumnarBroadcastNestedLoopJoinExec(
          left,
          right,
          plan.buildSide,
          plan.joinType,
          plan.condition,
        )
      case plan: ShuffledHashJoinExec if enableDedupLeftSemiJoin && !SQLConf.get.adaptiveExecutionEnabled => {
        plan.joinType match {
          case LeftSemi => {
            if (plan.condition.isEmpty && plan.right.output.size >= dedupLeftSemiJoinThreshold) {
              val left = replaceWithColumnarPlan(plan.left)
              val right = replaceWithColumnarPlan(plan.right)
              val partialAgg = PhysicalAggregation.unapply(Aggregate(plan.right.output, plan.right.output, new DummyLogicalPlan)) match {
                case Some((groupingExpressions, aggExpressions, resultExpressions, _))
                  if aggExpressions.forall(expr => expr.isInstanceOf[AggregateExpression]) =>
                  ExtendedAggUtils.planPartialAggregateWithoutDistinct(
                    ExtendedAggUtils.normalizeGroupingExpressions(groupingExpressions),
                    aggExpressions.map(_.asInstanceOf[AggregateExpression]),
                    resultExpressions,
                    right).asInstanceOf[HashAggregateExec]
              }
              val newHashAgg = new ColumnarHashAggregateExec(
                partialAgg.requiredChildDistributionExpressions,
                partialAgg.isStreaming,
                partialAgg.numShufflePartitions,
                partialAgg.groupingExpressions,
                partialAgg.aggregateExpressions,
                partialAgg.aggregateAttributes,
                partialAgg.initialInputBufferOffset,
                partialAgg.resultExpressions,
                right)

              ColumnarShuffledHashJoinExec(
                plan.leftKeys,
                plan.rightKeys,
                plan.joinType,
                ShimUtil.buildBuildSide(plan.buildSide, plan.joinType),
                plan.condition,
                left,
                newHashAgg,
                plan.isSkewJoin)
            } else {
              val left = replaceWithColumnarPlan(plan.left)
              val right = replaceWithColumnarPlan(plan.right)
              logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
              ColumnarShuffledHashJoinExec(
                plan.leftKeys,
                plan.rightKeys,
                plan.joinType,
                ShimUtil.buildBuildSide(plan.buildSide, plan.joinType),
                plan.condition,
                left,
                right,
                plan.isSkewJoin)
            }
          }
          case _ => {
            val left = replaceWithColumnarPlan(plan.left)
            val right = replaceWithColumnarPlan(plan.right)
            logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
            ColumnarShuffledHashJoinExec(
              plan.leftKeys,
              plan.rightKeys,
              plan.joinType,
              ShimUtil.buildBuildSide(plan.buildSide, plan.joinType),
              plan.condition,
              left,
              right,
              plan.isSkewJoin)
          }
        }
      }
      case plan: ShuffledHashJoinExec =>
        val left = replaceWithColumnarPlan(plan.left)
        val right = replaceWithColumnarPlan(plan.right)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        ColumnarShuffledHashJoinExec(
          plan.leftKeys,
          plan.rightKeys,
          plan.joinType,
          ShimUtil.buildBuildSide(plan.buildSide, plan.joinType),
          plan.condition,
          left,
          right,
          plan.isSkewJoin)
      case plan: SortMergeJoinExec =>
        logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
        val left = replaceWithColumnarPlan(plan.left)
        val right = replaceWithColumnarPlan(plan.right)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        new ColumnarSortMergeJoinExec(
          plan.leftKeys,
          plan.rightKeys,
          plan.joinType,
          plan.condition,
          left,
          right,
          plan.isSkewJoin)
      case plan: TopNSortExec =>
        val child = replaceWithColumnarPlan(plan.child)
        logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
        ColumnarTopNSortExec(plan.n, plan.strictTopN, plan.partitionSpec, plan.sortOrder, plan.global, child)
      case plan: SortExec =>
        val child = replaceWithColumnarPlan(plan.child)
        logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
        ColumnarSortExec(plan.sortOrder, plan.global, child, plan.testSpillFrequency)
      case plan: WindowExec =>
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
      case plan: UnionExec =>
        val children = plan.children.map(replaceWithColumnarPlan)
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        ColumnarUnionExec(children)
      case plan@ShuffleExchangeExecShim(_, _, _, advisoryPartitionSize) =>
        val child = replaceWithColumnarPlan(plan.child)
        if (child.output.nonEmpty) {
          logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
          if (child.output.size > columnsThreshold && enableRowShuffle) {
            new ColumnarShuffleExchangeExec(plan.outputPartitioning, child, plan.shuffleOrigin, advisoryPartitionSize, true)
          } else {
            new ColumnarShuffleExchangeExec(plan.outputPartitioning, child, plan.shuffleOrigin, advisoryPartitionSize, false)
          }
        } else {
          plan
        }
      case plan: AQEShuffleReadExec if columnarConf.enableColumnarShuffle =>
        plan.child match {
          case shuffle: ColumnarShuffleExchangeExec =>
            logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
            OmniAQEShuffleReadExec(plan.child, plan.partitionSpecs)
          case ShuffleQueryStageExec(_, shuffle: ColumnarShuffleExchangeExec, _) =>
            logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
            OmniAQEShuffleReadExec(plan.child, plan.partitionSpecs)
          case ShuffleQueryStageExec(_, reused: ReusedExchangeExec, _) =>
            reused match {
              case ReusedExchangeExec(_, shuffle: ColumnarShuffleExchangeExec) =>
                logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
                OmniAQEShuffleReadExec(
                  plan.child,
                  plan.partitionSpecs)
              case _ =>
                plan
            }
          case _ =>
            plan
        }
      case plan: LocalLimitExec =>
        val child = replaceWithColumnarPlan(plan.child)
        logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
        ColumnarLocalLimitExec(plan.limit, child)
      case plan@GlobalLimitExecShim(_, _, offset) =>
        val child = replaceWithColumnarPlan(plan.child)
        logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
        ColumnarGlobalLimitExec(plan.limit, child, offset)
      case plan: CoalesceExec =>
        val child = replaceWithColumnarPlan(plan.child)
        logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
        ColumnarCoalesceExec(plan.numPartitions, child)
      case p =>
        val newPlan = ModifyUtilAdaptor.preReplaceSparkPlanFunc(p, replaceWithColumnarPlan)
        if (newPlan != null) {
          return newPlan
        }
        val children = plan.children.map(replaceWithColumnarPlan)
        logInfo(s"Columnar Processing for ${p.getClass} is currently not supported.")
        p.withNewChildren(children)
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    logInfo("Using BoostKit Spark Native Sql Engine Extension ColumnarPreOverrides")
    replaceWithColumnarPlan(plan)
  }
}

case class ColumnarPostOverrides(isSupportAdaptive: Boolean = true) extends Rule[SparkPlan] {

  val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf

  def apply(plan: SparkPlan): SparkPlan = {
    logInfo("Using BoostKit Spark Native Sql Engine Extension ColumnarPostOverrides")
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

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: RowToColumnarExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported")
      RowToOmniColumnarExec(child)
    case ColumnarToRowExec(child: ColumnarShuffleExchangeExec) if isSupportAdaptive =>
      replaceWithColumnarPlan(child)
    case ColumnarToRowExec(child: ColumnarBroadcastExchangeExec) =>
      replaceWithColumnarPlan(child)
    case plan: ColumnarToRowExec =>
      plan.child match {
        case child: BroadcastQueryStageExec =>
          child.plan match {
            case originalBroadcastPlan: ColumnarBroadcastExchangeExec =>
              child
            case ReusedExchangeExec(_, originalBroadcastPlan: ColumnarBroadcastExchangeExec) =>
              child
            case _ =>
              replaceColumnarToRow(plan, conf)
          }
        case _ =>
          replaceColumnarToRow(plan, conf)
      }
    case p =>
      val newPlan = ModifyUtilAdaptor.postReplaceSparkPlanFunc(p, replaceWithColumnarPlan)
      if (newPlan != null) {
        return newPlan
      }
      val children = p.children.map(replaceWithColumnarPlan)
      p.withNewChildren(children)
  }

  def replaceColumnarToRow(plan: ColumnarToRowExec, conf: SQLConf): SparkPlan = {
    val child = replaceWithColumnarPlan(plan.child)
    if (conf.getConf(ENABLE_OMNI_COLUMNAR_TO_ROW)) {
      OmniColumnarToRowExec(child)
    } else {
      ColumnarToRowExec(child)
    }
  }
}

case class ColumnarOverrideRules(session: SparkSession) extends ColumnarRule with Logging {

  private def preOverrides(): List[SparkSession => Rule[SparkPlan]] = List(
    FallbackMultiCodegens,
    (_: SparkSession) => AddTransformHintRule(),
    (_: SparkSession) => ColumnarPreOverrides(isAdaptiveContext))

  private def postOverrides(): List[SparkSession => Rule[SparkPlan]] = List(
    (_: SparkSession) => ColumnarPostOverrides(isAdaptiveContext)
  )

  private def finallyRules(): List[SparkSession => Rule[SparkPlan]] = {
    List(
      (_: SparkSession) => RemoveTransformHintRule()
    )
  }

  private def transformPlan(getRules: List[SparkSession => Rule[SparkPlan]],
                            plan: SparkPlan,
                            step: String) = {
    logDebug(
      s"${step}ColumnarTransitions preOverriden plan:\n${plan.toString}")
    val overridden = getRules.foldLeft(plan) {
      (p, getRule) =>
        val rule = getRule(session)
        val newPlan = rule(p)
        newPlan
    }
    logDebug(
      s"${step}ColumnarTransitions afterOverriden plan:\n${overridden.toString}")
    overridden
  }

  // Just for test use.
  def enableAdaptiveContext(): Unit = {
    session.sparkContext.setLocalProperty(OMNI_IS_ADAPTIVE_CONTEXT, "true")
  }

  // Holds the original plan for possible entire fallback.
  private val localOriginalPlans: ThreadLocal[ListBuffer[SparkPlan]] =
    ThreadLocal.withInitial(() => ListBuffer.empty[SparkPlan])
  private val localIsAdaptiveContextFlags: ThreadLocal[ListBuffer[Boolean]] =
    ThreadLocal.withInitial(() => ListBuffer.empty[Boolean])

  private def setOriginalPlan(plan: SparkPlan): Unit = {
    localOriginalPlans.get.prepend(plan)
  }

  private def originalPlan: SparkPlan = {
    val plan = localOriginalPlans.get.head
    assert(plan != null)
    plan
  }

  private def resetOriginalPlan(): Unit = localOriginalPlans.get.remove(0)

  private def fallbackPolicy(): List[SparkSession => Rule[SparkPlan]] = {
    List((_: SparkSession) => ExpandFallbackPolicy(isAdaptiveContext, originalPlan))
  }

  // This is an empirical value, may need to be changed for supporting other versions of spark.
  private val aqeStackTraceIndex = 14

  private def setAdaptiveContext(): Unit = {
    val traceElements = Thread.currentThread.getStackTrace
    assert(
      traceElements.length > aqeStackTraceIndex,
      s"The number of stack trace elements is expected to be more than $aqeStackTraceIndex")
    // ApplyColumnarRulesAndInsertTransitions is called by either QueryExecution or
    // AdaptiveSparkPlanExec. So by checking the stack trace, we can know whether
    // columnar rule will be applied in adaptive execution context. This part of code
    // needs to be carefully checked when supporting higher versions of spark to make
    // sure the calling stack has not been changed.
    localIsAdaptiveContextFlags
      .get()
      .prepend(
        traceElements(aqeStackTraceIndex).getClassName
          .equals(AdaptiveSparkPlanExec.getClass.getName))
  }

  private def resetAdaptiveContext(): Unit =
    localIsAdaptiveContextFlags.get().remove(0)

  def isAdaptiveContext: Boolean = Option(session.sparkContext.getLocalProperty(OMNI_IS_ADAPTIVE_CONTEXT))
    .getOrElse("false")
    .toBoolean || localIsAdaptiveContextFlags.get().head

  override def preColumnarTransitions: Rule[SparkPlan] = plan => PhysicalPlanSelector.
    maybe(session, plan) {
      setAdaptiveContext()
      setOriginalPlan(plan)
      transformPlan(preOverrides(), plan, "pre")
    }

  override def postColumnarTransitions: Rule[SparkPlan] = plan => PhysicalPlanSelector.
    maybe(session, plan) {
      val planWithFallBackPolicy = transformPlan(fallbackPolicy(), plan, "fallback")

      val finalPlan = planWithFallBackPolicy match {
        case FallbackNode(fallbackPlan) =>
          // skip c2r and r2c replaceWithColumnarPlan
          fallbackPlan
        case plan =>
          transformPlan(postOverrides(), plan, "post")
      }
      resetOriginalPlan()
      resetAdaptiveContext()
      transformPlan(finallyRules(), finalPlan, "final")
    }
}

case class RemoveTransformHintRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan.foreach(TransformHints.untag)
    plan
  }
}


class ColumnarPlugin extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    logInfo("Using BoostKit Spark Native Sql Engine Extension to Speed Up Your Queries.")
    extensions.injectColumnar(session => ColumnarOverrideRules(session))
    extensions.injectPlannerStrategy(_ => ShuffleJoinStrategy)
    extensions.injectOptimizerRule(_ => RewriteSelfJoinInInPredicate)
    extensions.injectOptimizerRule(_ => DelayCartesianProduct)
    extensions.injectQueryStagePrepRule(session => DedupLeftSemiJoinAQE(session))
    extensions.injectQueryStagePrepRule(session => FallbackBroadcastExchange(session))
    extensions.injectQueryStagePrepRule(session => PushOrderedLimitThroughAgg(session))
    ModifyUtilAdaptor.injectRule(extensions)
  }
}

private class OmniTaskStartExecutorPlugin extends ExecutorPlugin {
  override def onTaskStart(): Unit = {
    addLeakSafeTaskCompletionListener[Unit](_ => {
      MemoryManager.reclaimMemory()
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