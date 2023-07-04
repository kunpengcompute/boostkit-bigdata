package com.huawei.boostkit.omnioffload.spark

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.{ColumnarHashAggregateExec, ColumnarShuffleExchangeExec, ColumnarToRowExec, FileSourceScanExec, OmniColumnarToRowExec, SparkPlan}

object AnalyzedAggReplaceRule extends Rule[SparkPlan]{
  override def apply(plan: SparkPlan): SparkPlan = {
    if (isAnalyzedAggPlan(plan)) {
      rollbackAnalyzedAggOmniPlan(plan)
    } else if (isDistinctCountPlan(plan)) {
      plan
    } else {
      plan
    }
  }

  def isAnalyzedAggPlan(plan: SparkPlan): Boolean = {
    plan match {
      case HashAggregateExec(_, _, _, _, _, _,
      OmniColumnarToRowExec(
      ColumnarShuffleExchangeExec(_,
      ColumnarHashAggregateExec(_, _, _, _, _, _,
      ColumnarHashAggregateExec(_, _, _, _, _, _,
      ColumnarShuffleExchangeExec(_,
      ColumnarHashAggregateExec(_, _, _, _, _, _,
      scanExec: FileSourceScanExec), _))), _))
      ) =>
        if (!scanExec.relation.fileFormat.isInstanceOf[ParquetFileFormat]) {
          return false
        }
        true
      case _ => false
    }
  }

  def isDistinctCountPlan(plan: SparkPlan): Boolean = {
    plan match { //for analyzed agg
      case HashAggregateExec(_, _, _, _, _, _,
      OmniColumnarToRowExec(
      ColumnarShuffleExchangeExec(_,
      ColumnarHashAggregateExec(_, _, _, _, _, _,
      ColumnarHashAggregateExec(_, _, _, _, _, _,
      ColumnarShuffleExchangeExec(_,
      ColumnarHashAggregateExec(_, _, _, _, _, _,
      scanExec: FileSourceScanExec), _))), _))
      ) =>
        if (!scanExec.relation.fileFormat.isInstanceOf[ParquetFileFormat]) {
          return false
        }
        true
      case _ => false
    }
  }

  def rollbackAnalyzedAggOmniPlan(plan: SparkPlan): SparkPlan = plan match {
    case agg@ColumnarHashAggregateExec(requiredChildDistributionExpressions, groupingExpressions,
    aggregateExpressions, aggregateAttributes, initialInputBufferOffset, resultExpressions, child) =>
      val child = rollbackAnalyzedAggOmniPlan(agg.child)
      HashAggregateExec(requiredChildDistributionExpressions,groupingExpressions,aggregateExpressions,
        aggregateAttributes,initialInputBufferOffset,resultExpressions,child)
    case shuffle@ColumnarShuffleExchangeExec(outputPartitioning, child, shuffleOrigin) =>
      val child = rollbackAnalyzedAggOmniPlan(shuffle.child)
      ShuffleExchangeExec(outputPartitioning,child,shuffleOrigin)
    case omniColToRow: OmniColumnarToRowExec =>
      rollbackAnalyzedAggOmniPlan(omniColToRow.child)
    case scan: FileSourceScanExec =>
      ColumnarToRowExec(scan)
    case p =>
      val children = plan.children.map(rollbackAnalyzedAggOmniPlan)
      p.withNewChildren(children)
  }
}
