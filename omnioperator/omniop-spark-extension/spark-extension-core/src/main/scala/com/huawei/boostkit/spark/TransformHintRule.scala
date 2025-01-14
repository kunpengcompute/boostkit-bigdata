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

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, OmniAQEShuffleReadExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ColumnarBroadcastHashJoinExec, ColumnarBroadcastNestedLoopJoinExec, ColumnarShuffledHashJoinExec, ColumnarSortMergeJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.execution.{CoalesceExec, CodegenSupport, ColumnarBroadcastExchangeExec, ColumnarCoalesceExec, ColumnarDataWritingCommandExec, ColumnarExpandExec, ColumnarFileSourceScanExec, ColumnarFilterExec, ColumnarGlobalLimitExec, ColumnarHashAggregateExec, ColumnarLocalLimitExec, ColumnarProjectExec, ColumnarShuffleExchangeExec, ColumnarSortExec, ColumnarTakeOrderedAndProjectExec, ColumnarTopNSortExec, ColumnarUnionExec, ColumnarWindowExec, ExpandExec, FileSourceScanExec, FilterExec, GlobalLimitExec, GlobalLimitExecShim, LocalLimitExec, ProjectExec, ShuffleExchangeExecShim, SortExec, SparkPlan, TakeOrderedAndProjectExecShim, TopNSortExec, UnionExec}
import org.apache.spark.sql.types.ColumnarBatchSupportUtil.checkColumnarBatchSupport
import org.apache.spark.sql.util.ShimUtil

trait TransformHint {
  val stacktrace: Option[String] =
    if (TransformHints.DEBUG) {
      Some(ExceptionUtils.getStackTrace(new Throwable()))
    } else None
}

case class TRANSFORM_SUPPORTED() extends TransformHint
case class TRANSFORM_UNSUPPORTED(reason: Option[String]) extends TransformHint

object TransformHints {
  val TAG: TreeNodeTag[TransformHint] =
    TreeNodeTag[TransformHint]("omni.transformhint")

  val DEBUG = false

  def isAlreadyTagged(plan: SparkPlan): Boolean = {
    plan.getTagValue(TAG).isDefined
  }

  def isTransformable(plan: SparkPlan): Boolean = {
    if (plan.getTagValue(TAG).isDefined) {
      return plan.getTagValue(TAG).get.isInstanceOf[TRANSFORM_SUPPORTED]
    }
    false
  }

  def isNotTransformable(plan: SparkPlan): Boolean = {
    if (plan.getTagValue(TAG).isDefined) {
      return plan.getTagValue(TAG).get.isInstanceOf[TRANSFORM_UNSUPPORTED]
    }
    false
  }

  def tag(plan: SparkPlan, hint: TransformHint): Unit = {
    if (isAlreadyTagged(plan)) {
      if (isNotTransformable(plan) && hint.isInstanceOf[TRANSFORM_SUPPORTED]) {
        throw new UnsupportedOperationException(
          "Plan was already tagged as non-transformable, " +
            s"cannot mark it as transformable after that:\n${plan.toString()}")
      }
    }
    plan.setTagValue(TAG, hint)
  }

  def untag(plan: SparkPlan): Unit = {
    plan.unsetTagValue(TAG)
  }

  def tagTransformable(plan: SparkPlan): Unit = {
    tag(plan, TRANSFORM_SUPPORTED())
  }

  def tagNotTransformable(plan: SparkPlan, reason: String = ""): Unit = {
    tag(plan, TRANSFORM_UNSUPPORTED(Some(reason)))
  }

  def tagAllNotTransformable(plan: SparkPlan, reason: String): Unit = {
    plan.foreach(other => tagNotTransformable(other, reason))
  }

  def getHint(plan: SparkPlan): TransformHint = {
    if (!isAlreadyTagged(plan)) {
      throw new IllegalStateException("Transform hint tag not set in plan: " + plan.toString())
    }
    plan.getTagValue(TAG).getOrElse(throw new IllegalStateException())
  }

  def getHintOption(plan: SparkPlan): Option[TransformHint] = {
    plan.getTagValue(TAG)
  }
}

case class AddTransformHintRule() extends Rule[SparkPlan] {

  val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf
  val enableColumnarShuffle: Boolean = columnarConf.enableColumnarShuffle
  val enableColumnarTopNSort: Boolean = columnarConf.enableColumnarTopNSort
  val enableColumnarSort: Boolean = columnarConf.enableColumnarSort
  val enableTakeOrderedAndProject: Boolean = columnarConf.enableTakeOrderedAndProject &&
    columnarConf.enableColumnarShuffle
  val enableColumnarUnion: Boolean = columnarConf.enableColumnarUnion
  val enableColumnarWindow: Boolean = columnarConf.enableColumnarWindow
  val enableColumnarHashAgg: Boolean = columnarConf.enableColumnarHashAgg
  val enableColumnarProject: Boolean = columnarConf.enableColumnarProject
  val enableColumnarFilter: Boolean = columnarConf.enableColumnarFilter
  val enableColumnarExpand: Boolean = columnarConf.enableColumnarExpand
  val enableColumnarBroadcastExchange: Boolean = columnarConf.enableColumnarBroadcastExchange
  val enableColumnarBroadcastJoin: Boolean = columnarConf.enableColumnarBroadcastJoin
  val enableColumnarBroadcastNestedJoin: Boolean = columnarConf.enableColumnarBroadcastNestedJoin
  val enableColumnarSortMergeJoin: Boolean = columnarConf.enableColumnarSortMergeJoin
  val enableShuffledHashJoin: Boolean = columnarConf.enableShuffledHashJoin
  val enableColumnarFileScan: Boolean = columnarConf.enableColumnarFileScan
  val enableLocalColumnarLimit: Boolean = columnarConf.enableLocalColumnarLimit
  val enableGlobalColumnarLimit: Boolean = columnarConf.enableGlobalColumnarLimit
  val enableColumnarCoalesce: Boolean = columnarConf.enableColumnarCoalesce
  val enableColumnarDataWritingCommand: Boolean = columnarConf.enableColumnarDataWritingCommand

  override def apply(plan: SparkPlan): SparkPlan = {
    addTransformableTags(plan)
  }

  /** Inserts a transformable tag on top of those that are not supported. */
  private def addTransformableTags(plan: SparkPlan): SparkPlan = {
    // Walk the tree with post-order
    val out = plan.withNewChildren(plan.children.map(addTransformableTags))
    addTransformableTag(out)
    out
  }

  private def addTransformableTag(plan: SparkPlan): Unit = {
    if (TransformHints.isAlreadyTagged(plan)) {
      logDebug(
        s"Skip adding transformable tag, since plan already tagged as " +
          s"${TransformHints.getHint(plan)}: ${plan.toString()}")
      return
    }
    try {
      plan match {
        case plan: FileSourceScanExec =>
          if (!checkColumnarBatchSupport(conf, plan)) {
            TransformHints.tagNotTransformable(
              plan,
              "columnar Batch is not enabled in FileSourceScanExec")
            return
          }
          if (!enableColumnarFileScan) {
            TransformHints.tagNotTransformable(
              plan, "columnar FileScan is not enabled in FileSourceScanExec")
            return
          }
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
          ).buildCheck()
          TransformHints.tagTransformable(plan)
        case plan: ProjectExec =>
          if (!enableColumnarProject) {
            TransformHints.tagNotTransformable(
              plan, "columnar Project is not enabled in ProjectExec")
            return
          }
          ColumnarProjectExec(plan.projectList, plan.child).buildCheck()
          TransformHints.tagTransformable(plan)
        case plan: FilterExec =>
          if (!enableColumnarFilter) {
            TransformHints.tagNotTransformable(
              plan, "columnar Filter is not enabled in FilterExec")
            return
          }
          ColumnarFilterExec(plan.condition, plan.child).buildCheck()
          TransformHints.tagTransformable(plan)
        case plan: ExpandExec =>
          if (!enableColumnarExpand) {
            TransformHints.tagNotTransformable(
              plan, "columnar Expand is not enabled in ExpandExec")
            return
          }
          ColumnarExpandExec(plan.projections, plan.output, plan.child).buildCheck()
          TransformHints.tagTransformable(plan)
        case plan: HashAggregateExec =>
          if (!enableColumnarHashAgg) {
            TransformHints.tagNotTransformable(
              plan, "columnar HashAggregate is not enabled in HashAggregateExec")
            return
          }
          new ColumnarHashAggregateExec(
            plan.requiredChildDistributionExpressions,
            plan.isStreaming,
            plan.numShufflePartitions,
            plan.groupingExpressions,
            plan.aggregateExpressions,
            plan.aggregateAttributes,
            plan.initialInputBufferOffset,
            plan.resultExpressions,
            plan.child).buildCheck()
          TransformHints.tagTransformable(plan)
        case plan: TopNSortExec =>
          if (!enableColumnarTopNSort) {
            TransformHints.tagNotTransformable(
              plan, "columnar TopNSort is not enabled in TopNSortExec")
            return
          }
          ColumnarTopNSortExec(plan.n, plan.strictTopN, plan.partitionSpec,
            plan.sortOrder, plan.global, plan.child).buildCheck()
          TransformHints.tagTransformable(plan)
        case plan: SortExec =>
          if (!enableColumnarSort) {
            TransformHints.tagNotTransformable(
              plan, "columnar Sort is not enabled in SortExec")
            return
          }
          ColumnarSortExec(plan.sortOrder, plan.global,
            plan.child, plan.testSpillFrequency).buildCheck()
          TransformHints.tagTransformable(plan)
        case plan: BroadcastExchangeExec =>
          if (!enableColumnarBroadcastExchange) {
            TransformHints.tagNotTransformable(
              plan, "columnar BroadcastExchange is not enabled in BroadcastExchangeExec")
            return
          }
          new ColumnarBroadcastExchangeExec(plan.mode, plan.child).buildCheck()
          TransformHints.tagTransformable(plan)
        case plan@TakeOrderedAndProjectExecShim(_, _, _, _, offset) =>
          if (!enableTakeOrderedAndProject) {
            TransformHints.tagNotTransformable(
              plan, "columnar TakeOrderedAndProject is not enabled in TakeOrderedAndProjectExec")
            return
          }
          ColumnarTakeOrderedAndProjectExec(
            plan.limit,
            plan.sortOrder,
            plan.projectList,
            plan.child,
            offset).buildCheck()
          TransformHints.tagTransformable(plan)
        case plan: UnionExec =>
          if (!enableColumnarUnion) {
            TransformHints.tagNotTransformable(
              plan, "columnar Union is not enabled in UnionExec")
            return
          }
          ColumnarUnionExec(plan.children).buildCheck()
          TransformHints.tagTransformable(plan)
        case plan@ShuffleExchangeExecShim(_, _, _, advisoryPartitionSize) =>
          if (!enableColumnarShuffle) {
            TransformHints.tagNotTransformable(
              plan, "columnar ShuffleExchange is not enabled in ShuffleExchangeExec")
            return
          }
          ColumnarShuffleExchangeExec(plan.outputPartitioning, plan.child, plan.shuffleOrigin, advisoryPartitionSize)
            .buildCheck()
          TransformHints.tagTransformable(plan)
        case plan: BroadcastHashJoinExec =>
          // We need to check if BroadcastExchangeExec can be converted to columnar-based.
          // If not, BHJ should also be row-based.
          if (!enableColumnarBroadcastJoin) {
            TransformHints.tagNotTransformable(
              plan, "columnar BroadcastHashJoin is not enabled in BroadcastHashJoinExec")
            return
          }
          val left = plan.left
          left match {
            case exec: BroadcastExchangeExec =>
              new ColumnarBroadcastExchangeExec(exec.mode, exec.child)
            case BroadcastQueryStageExec(_, plan: BroadcastExchangeExec, _) =>
              new ColumnarBroadcastExchangeExec(plan.mode, plan.child)
            case BroadcastQueryStageExec(_, plan: ReusedExchangeExec, _) =>
              plan match {
                case ReusedExchangeExec(_, b: BroadcastExchangeExec) =>
                  new ColumnarBroadcastExchangeExec(b.mode, b.child)
                case _ =>
              }
            case _ =>
          }
          val right = plan.right
          right match {
            case exec: BroadcastExchangeExec =>
              new ColumnarBroadcastExchangeExec(exec.mode, exec.child)
            case BroadcastQueryStageExec(_, plan: BroadcastExchangeExec, _) =>
              new ColumnarBroadcastExchangeExec(plan.mode, plan.child)
            case BroadcastQueryStageExec(_, plan: ReusedExchangeExec, _) =>
              plan match {
                case ReusedExchangeExec(_, b: BroadcastExchangeExec) =>
                  new ColumnarBroadcastExchangeExec(b.mode, b.child)
                case _ =>
              }
            case _ =>
          }
          ColumnarBroadcastHashJoinExec(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            plan.buildSide,
            plan.condition,
            plan.left,
            plan.right,
            plan.isNullAwareAntiJoin).buildCheck()
          TransformHints.tagTransformable(plan)
        case plan: BroadcastNestedLoopJoinExec =>
          if (!enableColumnarBroadcastNestedJoin) {
            TransformHints.tagNotTransformable(
              plan, "columnar BroadcastNestedLoopJoin is not enabled in BroadcastNestedLoopJoinExec")
            return
          }
          ColumnarBroadcastNestedLoopJoinExec(
            plan.left,
            plan.right,
            plan.buildSide,
            plan.joinType,
            plan.condition).buildCheck()
          TransformHints.tagTransformable(plan)
        case plan: SortMergeJoinExec =>
          if (!enableColumnarSortMergeJoin) {
            TransformHints.tagNotTransformable(
              plan, "columnar SortMergeJoin is not enabled in SortMergeJoinExec")
            return
          }
          ColumnarSortMergeJoinExec(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            plan.condition,
            plan.left,
            plan.right,
            plan.isSkewJoin).buildCheck()
          TransformHints.tagTransformable(plan)
        case plan: WindowExec =>
          if (!enableColumnarWindow) {
            TransformHints.tagNotTransformable(
              plan, "columnar Window is not enabled in WindowExec")
            return
          }
          ColumnarWindowExec(plan.windowExpression, plan.partitionSpec,
            plan.orderSpec, plan.child).buildCheck()
          TransformHints.tagTransformable(plan)
        case plan: ShuffledHashJoinExec =>
          if (!enableShuffledHashJoin) {
            TransformHints.tagNotTransformable(
              plan, "columnar ShuffledHashJoin is not enabled in ShuffledHashJoinExec")
            return
          }
          ColumnarShuffledHashJoinExec(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            plan.buildSide,
            plan.condition,
            plan.left,
            plan.right,
            plan.isSkewJoin).buildCheck()
          TransformHints.tagTransformable(plan)
        case plan: LocalLimitExec =>
          if (!enableLocalColumnarLimit) {
            TransformHints.tagNotTransformable(
              plan, "columnar LocalLimit is not enabled in LocalLimitExec")
            return
          }
          ColumnarLocalLimitExec(plan.limit, plan.child).buildCheck()
          TransformHints.tagTransformable(plan)
        case plan@GlobalLimitExecShim(_, _, offset) =>
          if (!enableGlobalColumnarLimit) {
            TransformHints.tagNotTransformable(
              plan, "columnar GlobalLimit is not enabled in GlobalLimitExec")
            return
          }
          ColumnarGlobalLimitExec(plan.limit, plan.child, offset).buildCheck()
          TransformHints.tagTransformable(plan)
        case plan: BroadcastNestedLoopJoinExec =>
          TransformHints.tagNotTransformable(
            plan, "columnar BroadcastNestedLoopJoin is not support")
          TransformHints.tagTransformable(plan)
        case plan: CoalesceExec =>
          if (!enableColumnarCoalesce) {
            TransformHints.tagNotTransformable(
              plan, "columnar Coalesce is not enabled in CoalesceExec")
            return
          }
          ColumnarCoalesceExec(plan.numPartitions, plan.child).buildCheck()
          TransformHints.tagTransformable(plan)
        case plan: DataWritingCommandExec =>
          if (!enableColumnarDataWritingCommand || !ShimUtil.isSupportDataWriter) {
            TransformHints.tagNotTransformable(
              plan, "columnar data writing is not support")
            return
          }
          ColumnarDataWritingCommandExec(plan.cmd, plan.child).buildCheck()
          TransformHints.tagTransformable(plan)
        case _ => TransformHints.tagTransformable(plan)

      }
    } catch {
      case e: UnsupportedOperationException =>
        val message = s"[OPERATOR FALLBACK] ${e} ${plan.getClass} falls back to Spark operator"
        logDebug(message)
        TransformHints.tagNotTransformable(plan, reason = message)
      case l: UnsatisfiedLinkError =>
        throw l
      case f: NoClassDefFoundError =>
        throw f
      case r: RuntimeException =>
        val message = s"[OPERATOR FALLBACK] ${r} ${plan.getClass} falls back to Spark operator"
        logDebug(message)
        TransformHints.tagNotTransformable(plan, reason = message)
      case t: Throwable =>
        val message = s"[OPERATOR FALLBACK] ${t} ${plan.getClass} falls back to Spark operator"
        logDebug(message)
        TransformHints.tagNotTransformable(plan, reason = message)
    }
  }
}

case class FallbackMultiCodegens(session: SparkSession) extends Rule[SparkPlan]  {

  val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf
  val optimizeLevel: Integer = columnarConf.joinOptimizationThrottle
  val preferColumnar: Boolean = columnarConf.enablePreferColumnar

  private def existsMultiCodegens(plan: SparkPlan, count: Int = 0): Boolean =
    plan match {
      case plan: CodegenSupport if plan.supportCodegen =>
        if ((count + 1) >= optimizeLevel) return true
        plan.children.map(existsMultiCodegens(_, count + 1)).exists(_ == true)
      case plan: ShuffledHashJoinExec =>
        if ((count + 1) >= optimizeLevel) return true
        plan.children.map(existsMultiCodegens(_, count + 1)).exists(_ == true)
      case _ => false
    }

  private def supportCodegen(plan: SparkPlan): Boolean = plan match {
    case plan: CodegenSupport =>
      plan.supportCodegen
    case _ => false
  }

  private def tagNotTransformable(plan: SparkPlan): SparkPlan = {
    TransformHints.tagNotTransformable(plan, "fallback multi codegens")
    plan
  }

  /**
   * Inserts an InputAdapter on top of those that do not support codegen.
   */
  private def tagNotTransformableRecursive(plan: SparkPlan): SparkPlan = {
    plan match {
      case p: ShuffleExchangeExec =>
        tagNotTransformable(p.withNewChildren(p.children.map(tagNotTransformableForMultiCodegens)))
      case p: BroadcastExchangeExec =>
        tagNotTransformable(p.withNewChildren(p.children.map(tagNotTransformableForMultiCodegens)))
      case p: ShuffledHashJoinExec =>
        tagNotTransformable(p.withNewChildren(p.children.map(tagNotTransformableRecursive)))
      case p if !supportCodegen(p) =>
        // tag them recursively
        p.withNewChildren(p.children.map(tagNotTransformableForMultiCodegens))
      case p: OmniAQEShuffleReadExec =>
        p.withNewChildren(p.children.map(tagNotTransformableForMultiCodegens))
      case p: BroadcastQueryStageExec =>
        p
      case p => tagNotTransformable(p.withNewChildren(p.children.map(tagNotTransformableRecursive)))
    }
  }

  /**
   * Inserts a WholeStageCodegen on top of those that support codegen.
   */
  private def tagNotTransformableForMultiCodegens(plan: SparkPlan): SparkPlan = {
    plan match {
      // For operators that will output domain object, do not insert WholeStageCodegen for it as
      // domain object can not be written into unsafe row.
      case plan if !preferColumnar && existsMultiCodegens(plan) =>
        tagNotTransformableRecursive(plan)
      case other =>
        other.withNewChildren(other.children.map(tagNotTransformableForMultiCodegens))
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    tagNotTransformableForMultiCodegens(plan)
  }

}

