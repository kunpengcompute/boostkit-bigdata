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

package com.huawei.boostkit.spark

import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.DynamicPruningSubquery
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{RowToOmniColumnarExec, _}
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, OmniAQEShuffleReadExec, AQEShuffleReadExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.ColumnarBatchSupportUtil.checkColumnarBatchSupport

case class ColumnarPreOverrides() extends Rule[SparkPlan] {
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
  val enableColumnarSortMergeJoin: Boolean = columnarConf.enableColumnarSortMergeJoin
  val enableColumnarSort: Boolean = columnarConf.enableColumnarSort
  val enableColumnarWindow: Boolean = columnarConf.enableColumnarWindow
  val enableColumnarShuffle: Boolean = columnarConf.enableColumnarShuffle
  val enableShuffledHashJoin: Boolean = columnarConf.enableShuffledHashJoin
  val enableColumnarUnion: Boolean = columnarConf.enableColumnarUnion
  val enableFusion: Boolean = columnarConf.enableFusion
  var isSupportAdaptive: Boolean = true
  val enableColumnarProjectFusion: Boolean = columnarConf.enableColumnarProjectFusion
  val enableColumnarLimit: Boolean = columnarConf.enableColumnarLimit

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
        case _ =>
          ColumnarProjectExec(plan.projectList, child)
      }
    case plan: FilterExec if enableColumnarFilter =>
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
        right,
        plan.isSkewJoin)
    case plan: SortMergeJoinExec if enableColumnarSortMergeJoin =>
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
    case plan: SortExec if enableColumnarSort =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarSortExec(plan.sortOrder, plan.global, child, plan.testSpillFrequency)
    case plan: WindowExec if enableColumnarWindow =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarWindowExec(plan.windowExpression, plan.partitionSpec, plan.orderSpec, child)
    case plan: UnionExec if enableColumnarUnion =>
      val children = plan.children.map(replaceWithColumnarPlan)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarUnionExec(children)
    case plan: ShuffleExchangeExec if enableColumnarShuffle =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
      new ColumnarShuffleExchangeExec(plan.outputPartitioning, child, plan.shuffleOrigin)
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
    case plan: LocalLimitExec if enableColumnarLimit =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarLocalLimitExec(plan.limit, child)
    case plan: GlobalLimitExec if enableColumnarLimit =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.")
      ColumnarGlobalLimitExec(plan.limit, child)
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
            ColumnarBroadcastExchangeAdaptorExec(originalBroadcastPlan, 1)),
          curPlan._canonicalized)
      case ReusedExchangeExec(_, originalBroadcastPlan: ColumnarBroadcastExchangeExec) =>
        BroadcastQueryStageExec(
          curPlan.id,
          BroadcastExchangeExec(
            originalBroadcastPlan.mode,
            ColumnarBroadcastExchangeAdaptorExec(curPlan.plan, 1)),
          curPlan._canonicalized)
      case _ =>
        curPlan
    }
  }
}

case class ColumnarPostOverrides() extends Rule[SparkPlan] {

  val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf
  var isSupportAdaptive: Boolean = true

  def apply(plan: SparkPlan): SparkPlan = {
    replaceWithColumnarPlan(plan)
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
      plan.child match {
        case child: BroadcastQueryStageExec =>
          child.plan match {
            case originalBroadcastPlan: ColumnarBroadcastExchangeExec =>
              BroadcastQueryStageExec(
                child.id,
                BroadcastExchangeExec(
                  originalBroadcastPlan.mode,
                  ColumnarBroadcastExchangeAdaptorExec(originalBroadcastPlan, 1)), child._canonicalized)
            case ReusedExchangeExec(_, originalBroadcastPlan: ColumnarBroadcastExchangeExec) =>
              BroadcastQueryStageExec(
                child.id,
                BroadcastExchangeExec(
                  originalBroadcastPlan.mode,
                  ColumnarBroadcastExchangeAdaptorExec(child.plan, 1)), child._canonicalized)
            case _ =>
              replaceColumnarToRow(plan, conf)
          }
        case _ =>
          replaceColumnarToRow(plan, conf)
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

  def replaceColumnarToRow(plan: ColumnarToRowExec, conf: SQLConf) : SparkPlan = {
    val child = replaceWithColumnarPlan(plan.child)
    if (conf.getConfString("spark.omni.sql.columnar.columnarToRow", "true").toBoolean) {
      OmniColumnarToRowExec(child)
    } else {
      ColumnarToRowExec(child)
    }
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
  }
}