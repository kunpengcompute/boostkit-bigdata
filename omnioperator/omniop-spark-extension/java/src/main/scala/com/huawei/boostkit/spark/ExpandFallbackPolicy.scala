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

import com.huawei.boostkit.spark.util.InsertTransitions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, AdaptiveSparkPlanExec, BroadcastQueryStageExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.ColumnarBroadcastHashJoinExec
import org.apache.spark.sql.execution._

/**
 * Note, this rule should only fallback to row-based plan if there is no harm.
 * The follow case should be handled carefully
 *
 * @param isAdaptiveContext If is inside AQE
 * @param originalPlan      The vanilla SparkPlan without apply gluten transform rules
 *
 * */
case class ExpandFallbackPolicy(isAdaptiveContext: Boolean, originalPlan: SparkPlan)
  extends Rule[SparkPlan] {

  val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf

  private def countFallback(plan: SparkPlan): Int = {
    var fallbacks = 0

    def countFallbackInternal(plan: SparkPlan): Unit = {
      plan match {
        case _: QueryStageExec => // Another stage.
        case _: CommandResultExec | _: ExecutedCommandExec => // ignore
        case _: AQEShuffleReadExec => // ignore
        case leafPlan: LeafExecNode if !isOmniSparkPlan(leafPlan) =>
          // Possible fallback for leaf node.
          fallbacks = fallbacks + 1
        case p:ColumnarToRowExec => p.children.foreach(countFallbackInternal)
        case p:RowToColumnarExec => p.children.foreach(countFallbackInternal)
        case p =>
          if(!isOmniSparkPlan(p)) {
            fallbacks = fallbacks + 1
          }
          p.children.foreach(countFallbackInternal)
      }
    }

    countFallbackInternal(plan)
    fallbacks
  }

  private def isOmniSparkPlan(plan: SparkPlan): Boolean = plan.nodeName.startsWith("Omni") && plan.supportsColumnar

  private def hasColumnarBroadcastExchangeWithJoin(plan: SparkPlan): Boolean = {
    def isColumnarBroadcastExchange(p: SparkPlan): Boolean = p match {
      case BroadcastQueryStageExec(_, _: ColumnarBroadcastExchangeExec, _) => true
      case _ => false
    }

    plan.find {
      case j: ColumnarBroadcastHashJoinExec
        if isColumnarBroadcastExchange(j.left) ||
          isColumnarBroadcastExchange(j.right) =>
        true
      case _ => false
    }.isDefined
  }

  private def fallback(plan: SparkPlan): Option[String] = {
    val fallbackThreshold = if (isAdaptiveContext) {
      columnarConf.wholeStageFallbackThreshold
    } else if (plan.find(_.isInstanceOf[AdaptiveSparkPlanExec]).isDefined) {
      // if we are here, that means we are now at `QueryExecution.preparations` and
      // AQE is actually not applied. We do nothing for this case, and later in
      // AQE we can check `wholeStageFallbackThreshold`.
      return None
    } else {
      // AQE is not applied, so we use the whole query threshold to check if should fallback
      columnarConf.queryFallbackThreshold
    }
    if (fallbackThreshold < 0) {
      return None
    }

    // todo 157-162添加適配后是否需要添加這段邏輯
    //    // not safe to fallback row-based BHJ as the broadcast exchange is already columnar
    //    if (hasColumnarBroadcastExchangeWithJoin(plan)) {
    //      return None
    //    }

    val netFallbackNum = countFallback(plan)

    if (netFallbackNum >= fallbackThreshold) {
      Some(
        s"Fallback policy is taking effect, net fallback number: $netFallbackNum, " +
          s"threshold: $fallbackThreshold")
    } else {
      None
    }
  }

  private def fallbackToRowBasedPlan(): SparkPlan = {
    val columnarPostOverrides = ColumnarPostOverrides()
    val planWithColumnarToRow = InsertTransitions.insertTransitions(originalPlan, false)
    planWithColumnarToRow.transform {
      case ColumnarToRowExec(bqe: BroadcastQueryStageExec) if bqe.plan.isInstanceOf[ColumnarBroadcastExchangeExec] =>
        val columnarBroadcastExchangeExec = bqe.plan.asInstanceOf[ColumnarBroadcastExchangeExec]
        BroadcastQueryStageExec(bqe.id, BroadcastExchangeExec(columnarBroadcastExchangeExec.mode, ColumnarBroadcastExchangeAdaptorExec(columnarBroadcastExchangeExec, 1)), bqe._canonicalized)
      case ColumnarToRowExec(bqe: BroadcastQueryStageExec) if bqe.plan.isInstanceOf[ReusedExchangeExec] && bqe.plan.asInstanceOf[ReusedExchangeExec].child.isInstanceOf[ColumnarBroadcastExchangeExec] =>
        val columnarBroadcastExchangeExec = bqe.plan.asInstanceOf[ReusedExchangeExec].child.asInstanceOf[ColumnarBroadcastExchangeExec]
        BroadcastQueryStageExec(bqe.id, BroadcastExchangeExec(columnarBroadcastExchangeExec.mode, ColumnarBroadcastExchangeAdaptorExec(columnarBroadcastExchangeExec, 1)), bqe._canonicalized)
      case c2r@(ColumnarToRowExec(_: ShuffleQueryStageExec) | ColumnarToRowExec(_: AQEShuffleReadExec)) =>
        columnarPostOverrides.replaceColumnarToRow(c2r.asInstanceOf[ColumnarToRowExec], conf)
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    logInfo("Using BoostKit Spark Native Sql Engine Extension FallbackPolicy")
    val reason = fallback(plan)
    if (reason.isDefined) {
      val fallbackPlan = fallbackToRowBasedPlan()
      TransformHints.tagAllNotTransformable(fallbackPlan, reason.get)
      FallbackNode(fallbackPlan)
    } else {
      plan
    }
  }
}

/** A wrapper to specify the plan is fallback plan, the caller side should unwrap it. */
case class FallbackNode(fallbackPlan: SparkPlan) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

  override def output: Seq[Attribute] = fallbackPlan.output
}
