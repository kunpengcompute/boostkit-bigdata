package com.huawei.boostkit.omnidata.spark

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SimpleCountAggregateExec}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.{ColumnarHashAggregateExec, ColumnarShuffleExchangeExec, ColumnarToRowExec, CommandResultExec, FileSourceScanExec, OmniColumnarToRowExec, SimpleCountFileScanExec, SparkPlan}

object CountReplaceRule extends Rule[SparkPlan] {
  var columnStat: BigInt = -1

  override def apply(plan: SparkPlan): SparkPlan = {
    if (shouldReplaceDistinctCount(plan)) {
      replaceDistinctCountPlan(plan)
    } else {
      plan
    }
  }

  def shouldReplaceCount(plan: SparkPlan): Boolean = {
    plan match {
      case DataWritingCommandExec(_,
      finalAgg@HashAggregateExec(_, _, aggExps: Seq[AggregateExpression], _, _, _,
      shuffle@ShuffleExchangeExec(_,
      ptAgg@HashAggregateExec(_, _, _, _, _, _,
      ColumnarToRowExec(
      scan: FileSourceScanExec)), _))) =>
        if(aggExps.isEmpty){
          return false
        }
        val headAggExp = aggExps.head
        if (!headAggExp.aggregateFunction.isInstanceOf[Count]) {
          return false
        }
        val countFunc = headAggExp.aggregateFunction.asInstanceOf[Count]
        val countChild = countFunc.children
        if (countChild.size != 1) {
          return false
        }
        if (!countChild.head.isInstanceOf[Literal]) {
          return false
        }
        val LiteralNum = countChild.head.asInstanceOf[Literal]
        if (!LiteralNum.equals(Literal(1))) {
          return false
        }
        if (!scan.relation.fileFormat.isInstanceOf[ParquetFileFormat]) {
          return false
        }
        true
      case _ => false
    }
  }

  def shouldReplaceDistinctCount(plan: SparkPlan): Boolean = {
    plan match {
      case DataWritingCommandExec(_,
      topFinalAgg@HashAggregateExec(_, _, _, _, _, _,
      ShuffleExchangeExec(_,
      HashAggregateExec(_, _, _, _, _, _,
      HashAggregateExec(_, _, _, _, _, _,
      ShuffleExchangeExec(_,
      HashAggregateExec(_, _, _, _, _, _,
      ColumnarToRowExec(
      scanExec: FileSourceScanExec)), _))), _))) =>
        val aggExps = topFinalAgg.aggregateExpressions
        if (aggExps.size != 1) {
          return false
        }
        val headAggExp = aggExps.head
        if (!headAggExp.isDistinct) {
          return false
        }
        if (!headAggExp.aggregateFunction.isInstanceOf[Count]) {
          return false
        }
        val countFunc = headAggExp.aggregateFunction.asInstanceOf[Count]
        val countChild = countFunc.children
        if (countChild.size != 1) {
          return false
        }
        if (!countChild.head.isInstanceOf[AttributeReference]) {
          return false
        }
        val distinctColumn = scanExec.schema.head.name
        val distinctTable = scanExec.tableIdentifier.get
        val colStatsMap = plan.session.sqlContext.sparkSession.sessionState.catalog
          .getTableMetadata(distinctTable)
          .stats.map(_.colStats).getOrElse(Map.empty)
        if (colStatsMap.isEmpty) {
          return false
        }
        if (colStatsMap(distinctColumn) == null) {
          return false
        }
        columnStat = colStatsMap(distinctColumn).distinctCount.get
        true
      case _ => false
    }
  }

  def replaceDistinctCountPlan(plan: SparkPlan): SparkPlan = plan match {
    case agg: HashAggregateExec =>
      val child = replaceDistinctCountPlan(agg.child)
      SimpleCountAggregateExec(agg.requiredChildDistributionExpressions,
        agg.groupingExpressions,
        agg.aggregateExpressions,
        agg.aggregateAttributes,
        agg.initialInputBufferOffset,
        agg.resultExpressions,
        child,
        isDistinctCount = true,
        columnStat)
    case p =>
      val children = plan.children.map(replaceDistinctCountPlan)
      p.withNewChildren(children)
  }
}
