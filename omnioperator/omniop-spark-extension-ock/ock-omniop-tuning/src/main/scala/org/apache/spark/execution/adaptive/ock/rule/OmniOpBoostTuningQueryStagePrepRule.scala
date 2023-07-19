/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.sql.execution.adaptive.ock.rule

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.ock.BoostTuningQueryManager
import org.apache.spark.sql.execution.adaptive.ock.common.BoostTuningUtil.normalizedSparkPlan
import org.apache.spark.sql.execution.adaptive.ock.common.RuntimeConfiguration._
import org.apache.spark.sql.execution.adaptive.ock.common.StringPrefix.SHUFFLE_PREFIX
import org.apache.spark.sql.execution.adaptive.ock.exchange._
import org.apache.spark.sql.execution.adaptive.ock.rule.relation.ColumnarSMJRelationMarker
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

case class OmniOpBoostTuningQueryStagePrepRule() extends Rule[SparkPlan] {

  override val ruleName: String = "OmniOpBoostTuningQueryStagePrepRule"

  lazy val delegate: BoostTuningQueryStagePrepRule = BoostTuningQueryStagePrepRule()

  val markers = Seq(ColumnarSMJRelationMarker)

  override def apply(plan: SparkPlan): SparkPlan = {
    
    val executionId = BoostTuningQueryManager.getExecutionId

    val newPlan = replaceOmniShuffleExchange(plan)

    delegate.detectCostEvaluator(executionId, newPlan) match {
      case Some(keepPlan) => return keepPlan
      case _ =>
    }

    delegate.solveRelation(executionId, newPlan, markers)

    newPlan
  }

  def replaceOmniShuffleExchange(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case ex: ShuffleExchangeExec =>
        BoostTuningColumnarShuffleExchangeExec(ex.outputPartitioning, ex.child, ex.shuffleOrigin,
          adaptiveContextCache.getOrElseUpdate(ex.canonicalized,
            PartitionContext(normalizedSparkPlan(ex, SHUFFLE_PREFIX))))
    }
  }

}