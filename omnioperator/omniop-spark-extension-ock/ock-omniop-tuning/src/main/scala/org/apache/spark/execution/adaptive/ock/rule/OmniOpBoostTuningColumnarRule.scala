/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.sql.execution.adaptive.ock.rule

import com.huawei.boostkit.spark.ColumnarPluginConfig
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.ock.BoostTuningQueryManager
import org.apache.spark.sql.execution.adaptive.ock.common.BoostTuningLogger.TLogWarning
import org.apache.spark.sql.execution.adaptive.ock.common.BoostTuningUtil.{getQueryExecutionId, normalizedSparkPlan}
import org.apache.spark.sql.execution.adaptive.ock.common.OmniRuntimeConfiguration.enableColumnarShuffle
import org.apache.spark.sql.execution.adaptive.ock.common.StringPrefix.SHUFFLE_PREFIX
import org.apache.spark.sql.execution.adaptive.ock.exchange._
import org.apache.spark.sql.execution.adaptive.ock.reader._
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

import scala.collection.mutable

case class OmniOpBoostTuningColumnarRule(pre: Rule[SparkPlan], post: Rule[SparkPlan]) extends ColumnarRule {
  override def preColumnarTransitions: Rule[SparkPlan] = pre

  override def postColumnarTransitions: Rule[SparkPlan] = post
}

object OmniOpBoostTuningColumnarRule {
  val rollBackExchangeIdents: mutable.Set[String] = mutable.Set.empty
}

case class OmniOpBoostTuningPreColumnarRule() extends Rule[SparkPlan] {

  override val ruleName: String = "OmniOpBoostTuningPreColumnarRule"

  val delegate: BoostTuningPreNewQueryStageRule = BoostTuningPreNewQueryStageRule()

  override def apply(plan: SparkPlan): SparkPlan = {
    val executionId = getQueryExecutionId(plan)
    if (executionId < 0) {
      TLogWarning(s"Skipped to apply BoostTuning new query stage rule for unneeded plan: $plan")
      return plan
    }

    val query = BoostTuningQueryManager.getOrCreateQueryManager(executionId)

    delegate.prepareQueryExecution(query, plan)

    delegate.reportQueryShuffleMetrics(query, plan)

    tryMarkRollBack(plan)

    replaceOmniQueryExchange(plan)
  }

  private def tryMarkRollBack(plan: SparkPlan): Unit = {
    plan.foreach {
      case plan: BoostTuningShuffleExchangeLike =>
        if (!enableColumnarShuffle) {
          OmniOpBoostTuningColumnarRule.rollBackExchangeIdents += plan.getContext.ident
        }
        try {
          BoostTuningColumnarShuffleExchangeExec(plan.outputPartitioning, plan.child, plan.shuffleOrigin, null).buildCheck()
        } catch {
          case e: UnsupportedOperationException =>
            logDebug(s"[OPERATOR FALLBACK] ${e} ${plan.getClass} falls back to Spark operator")
            OmniOpBoostTuningColumnarRule.rollBackExchangeIdents += plan.getContext.ident
          case l: UnsatisfiedLinkError =>
            throw l
          case f: NoClassDefFoundError =>
            throw f
          case r: RuntimeException =>
            logDebug(s"[OPERATOR FALLBACK] ${r} ${plan.getClass} falls back to Spark operator")
            OmniOpBoostTuningColumnarRule.rollBackExchangeIdents += plan.getContext.ident
          case t: Throwable =>
            logDebug(s"[OPERATOR FALLBACK] ${t} ${plan.getClass} falls back to Spark operator")
            OmniOpBoostTuningColumnarRule.rollBackExchangeIdents += plan.getContext.ident
        }
      case _ =>
    }
  }

  def replaceOmniQueryExchange(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case ex: ColumnarShuffleExchangeExec =>
        BoostTuningColumnarShuffleExchangeExec(
          ex.outputPartitioning, ex.child, ex.shuffleOrigin,
          PartitionContext(normalizedSparkPlan(ex, SHUFFLE_PREFIX)))
    }
  }
}

case class OmniOpBoostTuningPostColumnarRule() extends Rule[SparkPlan] {

  override val ruleName: String = "OmniOpBoostTuningPostColumnarRule"

  override def apply(plan: SparkPlan): SparkPlan = {

    var newPlan = plan match {
      case b: BoostTuningShuffleExchangeLike if !OmniOpBoostTuningColumnarRule.rollBackExchangeIdents.contains(b.getContext.ident) =>
        b.child match {
          case ColumnarToRowExec(child) =>
            BoostTuningColumnarShuffleExchangeExec(b.outputPartitioning, child, b.shuffleOrigin, b.getContext)
          case plan if !plan.supportsColumnar =>
            BoostTuningColumnarShuffleExchangeExec(b.outputPartitioning, RowToOmniColumnarExec(plan), b.shuffleOrigin, b.getContext)
          case _ => b
        }
      case _ => plan
    }

    newPlan = additionalReplaceWithColumnarPlan(newPlan)

    newPlan.transformUp {
      case c: AQEShuffleReadExec if ColumnarPluginConfig.getConf.enableColumnarShuffle =>
        c.child match {
          case shuffle: BoostTuningColumnarShuffleExchangeExec =>
            logDebug(s"Columnar Processing for ${c.getClass} is currently supported.")
            BoostTuningOmniAQEShuffleReadExec(c.child, c.partitionSpecs)
          case ShuffleQueryStageExec(_, shuffle: BoostTuningColumnarShuffleExchangeExec, _) =>
            logDebug(s"Columnar Processing for ${c.getClass} is currently supported.")
            BoostTuningOmniAQEShuffleReadExec(c.child, c.partitionSpecs)
          case ShuffleQueryStageExec(_, reused: ReusedExchangeExec, _) =>
            reused match {
              case ReusedExchangeExec(_, shuffle: BoostTuningColumnarShuffleExchangeExec) =>
                logDebug(s"Columnar Processing for ${c.getClass} is currently supported.")
                BoostTuningOmniAQEShuffleReadExec(c.child, c.partitionSpecs)
              case _ =>
                c
            }
          case _ =>
            c
        }
    }
  }

  def additionalReplaceWithColumnarPlan(plan: SparkPlan): SparkPlan = plan match {
    case ColumnarToRowExec(child: BoostTuningShuffleExchangeLike) =>
      additionalReplaceWithColumnarPlan(child)
    case r: SparkPlan
      if !r.isInstanceOf[QueryStageExec] && !r.supportsColumnar && r.children.exists(c =>
        c.isInstanceOf[ColumnarToRowExec]) =>
      val children = r.children.map {
        case c: ColumnarToRowExec =>
          val child = additionalReplaceWithColumnarPlan(c.child)
          OmniColumnarToRowExec(child)
        case other =>
          additionalReplaceWithColumnarPlan(other)
      }
      r.withNewChildren(children)
    case p =>
      val children = p.children.map(additionalReplaceWithColumnarPlan)
      p.withNewChildren(children)
  }
}

