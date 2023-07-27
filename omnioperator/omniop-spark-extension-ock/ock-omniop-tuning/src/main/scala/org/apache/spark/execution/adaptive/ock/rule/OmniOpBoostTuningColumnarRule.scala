/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.sql.execution.adaptive.ock.rule

import com.huawei.boostkit.spark.ColumnarPluginConfig
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.ock.BoostTuningQueryManager
import org.apache.spark.sql.execution.adaptive.ock.BoostTuningQueryManager._
import org.apache.spark.sql.execution.adaptive.ock.common.BoostTuningUtil.normalizedSparkPlan
import org.apache.spark.sql.execution.adaptive.ock.common.StringPrefix.SHUFFLE_PREFIX
import org.apache.spark.sql.execution.adaptive.ock.exchange._
import org.apache.spark.sql.execution.adaptive.ock.reader._
import org.apache.spark.sql.execution.adaptive.{CustomShuffleReaderExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

case class OmniOpBoostTuningColumnarRule(pre: Rule[SparkPlan], post: Rule[SparkPlan]) extends ColumnarRule {
  override def preColumnarTransitions: Rule[SparkPlan] = pre
  override def postColumnarTransitions: Rule[SparkPlan] = post
}

case class OmniOpBoostTuningPreColumnarRule() extends Rule[SparkPlan] {

  override val ruleName: String = "OmniOpBoostTuningPreColumnarRule"

  val delegate: BoostTuningPreNewQueryStageRule = BoostTuningPreNewQueryStageRule()

  override def apply(plan: SparkPlan): SparkPlan = {
    val query = BoostTuningQueryManager.getOrCreateQueryManager(getExecutionId)

    delegate.prepareQueryExecution(query, plan)

    delegate.reportQueryShuffleMetrics(query, plan)

    replaceOmniQueryExchange(plan)  
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
      case b: BoostTuningShuffleExchangeLike =>
        b.child match {
          case ColumnarToRowExec(child) =>
            BoostTuningColumnarShuffleExchangeExec(b.outputPartitioning, child, b.shuffleOrigin, b.getContext)
          case _ => b
        }
      case _ => plan
    }

    newPlan = additionalReplaceWithColumnarPlan(newPlan)

    newPlan.transformUp {
      case c: CustomShuffleReaderExec if ColumnarPluginConfig.getConf.enableColumnarShuffle =>
        c.child match {
          case shuffle: BoostTuningColumnarShuffleExchangeExec =>
            logDebug(s"Columnar Processing for ${c.getClass} is currently supported.")
            BoostTuningColumnarCustomShuffleReaderExec(c.child, c.partitionSpecs)
          case ShuffleQueryStageExec(_, shuffle: BoostTuningColumnarShuffleExchangeExec) =>
            logDebug(s"Columnar Processing for ${c.getClass} is currently supported.")
            BoostTuningColumnarCustomShuffleReaderExec(c.child, c.partitionSpecs)
          case ShuffleQueryStageExec(_, reused: ReusedExchangeExec) =>
            reused match {
              case ReusedExchangeExec(_, shuffle: BoostTuningColumnarShuffleExchangeExec) =>
                logDebug(s"Columnar Processing for ${c.getClass} is currently supported.")
                BoostTuningColumnarCustomShuffleReaderExec(c.child, c.partitionSpecs)
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

