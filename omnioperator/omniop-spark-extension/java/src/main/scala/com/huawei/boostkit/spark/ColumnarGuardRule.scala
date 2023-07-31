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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, CustomShuffleReaderExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.types.ColumnarBatchSupportUtil.checkColumnarBatchSupport

case class RowGuard(child: SparkPlan) extends SparkPlan {
  def output: Seq[Attribute] = child.output

  protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException
  }

  def children: Seq[SparkPlan] = Seq(child)
}

case class ColumnarGuardRule() extends Rule[SparkPlan] {
  val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf
  val preferColumnar: Boolean = columnarConf.enablePreferColumnar
  val enableColumnarShuffle: Boolean = columnarConf.enableColumnarShuffle
  val enableColumnarSort: Boolean = columnarConf.enableColumnarSort
  val enableTakeOrderedAndProject: Boolean = columnarConf.enableTakeOrderedAndProject &&
    columnarConf.enableColumnarShuffle
  val enableColumnarUnion: Boolean = columnarConf.enableColumnarUnion
  val enableColumnarWindow: Boolean = columnarConf.enableColumnarWindow
  val enableColumnarHashAgg: Boolean = columnarConf.enableColumnarHashAgg
  val enableColumnarProject: Boolean = columnarConf.enableColumnarProject
  val enableColumnarFilter: Boolean = columnarConf.enableColumnarFilter
  val enableColumnarExpand: Boolean = columnarConf.enableColumnarExpand
  val enableColumnarBroadcastExchange: Boolean = columnarConf.enableColumnarBroadcastExchange &&
    columnarConf.enableColumnarBroadcastJoin
  val enableColumnarBroadcastJoin: Boolean = columnarConf.enableColumnarBroadcastJoin
  val enableColumnarSortMergeJoin: Boolean = columnarConf.enableColumnarSortMergeJoin
  val enableSortMergeJoinFusion: Boolean = columnarConf.enableSortMergeJoinFusion
  val enableShuffledHashJoin: Boolean = columnarConf.enableShuffledHashJoin
  val enableColumnarFileScan: Boolean = columnarConf.enableColumnarFileScan
  val optimizeLevel: Integer = columnarConf.joinOptimizationThrottle

  private def tryConvertToColumnar(plan: SparkPlan): Boolean = {
    try {
      val columnarPlan = plan match {
        case plan: FileSourceScanExec =>
          if (!checkColumnarBatchSupport(conf, plan)) {
            return false
          }
          if (!enableColumnarFileScan) return false
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
        case plan: ProjectExec =>
          if (!enableColumnarProject) return false
          ColumnarProjectExec(plan.projectList, plan.child).buildCheck()
        case plan: FilterExec =>
          if (!enableColumnarFilter) return false
          ColumnarFilterExec(plan.condition, plan.child).buildCheck()
        case plan: ExpandExec =>
          if (!enableColumnarExpand) return false
          ColumnarExpandExec(plan.projections, plan.output, plan.child).buildCheck()
        case plan: HashAggregateExec =>
          if (!enableColumnarHashAgg) return false
          new ColumnarHashAggregateExec(
            plan.requiredChildDistributionExpressions,
            plan.groupingExpressions,
            plan.aggregateExpressions,
            plan.aggregateAttributes,
            plan.initialInputBufferOffset,
            plan.resultExpressions,
            plan.child).buildCheck()
        case plan: SortExec =>
          if (!enableColumnarSort) return false
          ColumnarSortExec(plan.sortOrder, plan.global,
            plan.child, plan.testSpillFrequency).buildCheck()
        case plan: BroadcastExchangeExec =>
          if (!enableColumnarBroadcastExchange) return false
          new ColumnarBroadcastExchangeExec(plan.mode, plan.child).buildCheck()
        case plan: TakeOrderedAndProjectExec =>
          if (!enableTakeOrderedAndProject) return false
          ColumnarTakeOrderedAndProjectExec(
            plan.limit,
            plan.sortOrder,
            plan.projectList,
            plan.child).buildCheck()
        case plan: UnionExec =>
          if (!enableColumnarUnion) return false
          ColumnarUnionExec(plan.children).buildCheck()
        case plan: ShuffleExchangeExec =>
          if (!enableColumnarShuffle) return false
          new ColumnarShuffleExchangeExec(plan.outputPartitioning, plan.child, plan.shuffleOrigin)
            .buildCheck()
        case plan: BroadcastHashJoinExec =>
          // We need to check if BroadcastExchangeExec can be converted to columnar-based.
          // If not, BHJ should also be row-based.
          if (!enableColumnarBroadcastJoin) return false
          val left = plan.left
          left match {
            case exec: BroadcastExchangeExec =>
              new ColumnarBroadcastExchangeExec(exec.mode, exec.child)
            case BroadcastQueryStageExec(_, plan: BroadcastExchangeExec) =>
              new ColumnarBroadcastExchangeExec(plan.mode, plan.child)
            case BroadcastQueryStageExec(_, plan: ReusedExchangeExec) =>
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
            case BroadcastQueryStageExec(_, plan: BroadcastExchangeExec) =>
              new ColumnarBroadcastExchangeExec(plan.mode, plan.child)
            case BroadcastQueryStageExec(_, plan: ReusedExchangeExec) =>
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
            plan.right).buildCheck()
        case plan: SortMergeJoinExec =>
          if (!enableColumnarSortMergeJoin) return false
          if (enableSortMergeJoinFusion) {
            new ColumnarSortMergeJoinFusionExec(
              plan.leftKeys,
              plan.rightKeys,
              plan.joinType,
              plan.condition,
              plan.left,
              plan.right,
              plan.isSkewJoin).buildCheck()
          } else {
            new ColumnarSortMergeJoinExec(
              plan.leftKeys,
              plan.rightKeys,
              plan.joinType,
              plan.condition,
              plan.left,
              plan.right,
              plan.isSkewJoin).buildCheck()
          }
        case plan: WindowExec =>
          if (!enableColumnarWindow) return false
          ColumnarWindowExec(plan.windowExpression, plan.partitionSpec,
            plan.orderSpec, plan.child).buildCheck()
        case plan: ShuffledHashJoinExec =>
          if (!enableShuffledHashJoin) return false
          ColumnarShuffledHashJoinExec(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            plan.buildSide,
            plan.condition,
            plan.left,
            plan.right).buildCheck()
        case plan: BroadcastNestedLoopJoinExec => return false
        case p =>
          p
      }
    }
    catch {
      case e: UnsupportedOperationException =>
        logDebug(s"[OPERATOR FALLBACK] ${e} ${plan.getClass} falls back to Spark operator")
        return false
      case l: UnsatisfiedLinkError =>
        throw l
      case f: NoClassDefFoundError =>
        throw f
      case r: RuntimeException =>
        logDebug(s"[OPERATOR FALLBACK] ${r} ${plan.getClass} falls back to Spark operator")
        return false
      case t: Throwable =>
        logDebug(s"[OPERATOR FALLBACK] ${t} ${plan.getClass} falls back to Spark operator")
        return false
    }
    true
  }

  private def existsMultiCodegens(plan: SparkPlan, count: Int = 0): Boolean =
    plan match {
      case plan: CodegenSupport if plan.supportCodegen =>
        if ((count + 1) >= optimizeLevel) return true
        plan.children.map(existsMultiCodegens(_, count + 1)).exists(_ == true)
      case plan: ShuffledHashJoinExec =>
        if ((count + 1) >= optimizeLevel) return true
        plan.children.map(existsMultiCodegens(_, count + 1)).exists(_ == true)
      case other => false
    }

  private def supportCodegen(plan: SparkPlan): Boolean = plan match {
    case plan: CodegenSupport =>
      plan.supportCodegen
    case _ => false
  }

  /**
   * Inserts an InputAdapter on top of those that do not support codegen.
   */
  private def insertRowGuardRecursive(plan: SparkPlan): SparkPlan = {
    plan match {
      case p: ShuffleExchangeExec =>
        RowGuard(p.withNewChildren(p.children.map(insertRowGuardOrNot)))
      case p: BroadcastExchangeExec =>
        RowGuard(p.withNewChildren(p.children.map(insertRowGuardOrNot)))
      case p: ShuffledHashJoinExec =>
        RowGuard(p.withNewChildren(p.children.map(insertRowGuardRecursive)))
      case p if !supportCodegen(p) =>
        // insert row guard them recursively
        p.withNewChildren(p.children.map(insertRowGuardOrNot))
      case p: CustomShuffleReaderExec =>
        p.withNewChildren(p.children.map(insertRowGuardOrNot))
      case p: BroadcastQueryStageExec =>
        p
      case p => RowGuard(p.withNewChildren(p.children.map(insertRowGuardRecursive)))
    }
  }

  private def insertRowGuard(plan: SparkPlan): SparkPlan = {
    RowGuard(plan.withNewChildren(plan.children.map(insertRowGuardOrNot)))
  }

  /**
   * Inserts a WholeStageCodegen on top of those that support codegen.
   */
  private def insertRowGuardOrNot(plan: SparkPlan): SparkPlan = {
    plan match {
      // For operators that will output domain object, do not insert WholeStageCodegen for it as
      // domain object can not be written into unsafe row.
      case plan if !preferColumnar && existsMultiCodegens(plan) =>
        insertRowGuardRecursive(plan)
      case plan if !tryConvertToColumnar(plan) =>
        insertRowGuard(plan)
      case p: BroadcastQueryStageExec =>
        p
      case other =>
        other.withNewChildren(other.children.map(insertRowGuardOrNot))
    }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    insertRowGuardOrNot(plan)
  }
}
