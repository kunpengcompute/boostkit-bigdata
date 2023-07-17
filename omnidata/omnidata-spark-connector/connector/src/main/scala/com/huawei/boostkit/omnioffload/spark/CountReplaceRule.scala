package com.huawei.boostkit.omnioffload.spark

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SimpleCountAggregateExec}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.execution.{ColumnarToRowExec, FileSourceScanExec, FilterExec, ProjectExec, SimpleCountFileScanExec, SortExec, SparkPlan}

object CountReplaceRule extends Rule[SparkPlan] {
  var columnStat: BigInt = -1
  var shouldReplaceScan: Boolean = false
  var shouldReplaceFinalAgg: Boolean = false
  var shouldReplaceAllAgg: Boolean = false

  override def apply(plan: SparkPlan): SparkPlan = {
    if (shouldReplaceDistinctCount(plan) || shouldReplaceCountOne(plan) || shouldReplaceBlendedCount(plan)) {
      replaceCountPlan(plan)
    } else {
      plan
    }
  }

  def shouldReplaceBlendedCount(plan: SparkPlan): Boolean = {
    plan match {
      case DataWritingCommandExec(_,
      finalAgg@HashAggregateExec(_, groups: Seq[NamedExpression], aggExps: Seq[AggregateExpression], _, _, _,
      shuffle@ShuffleExchangeExec(_,
      ptAgg@HashAggregateExec(_, _, _, _, _, _,
      ProjectExec(_, child)), _))) =>
        if (groups.nonEmpty) {
          return false
        }
        if (aggExps.isEmpty) {
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
        if (!child.isInstanceOf[BaseJoinExec]) {
          return false
        }
        val join = child.asInstanceOf[BaseJoinExec]
        if (!(isTargetFilterPlan(join.left) || isTargetFilterPlan(join.right))) {
          return false
        }
        shouldReplaceScan = true
        true
      case _ => false
    }
  }


  def isTargetCondition(condition: Expression): Boolean = {
    var result = false
    condition.foreach {
      case literal: Literal if literal.value.toString.equals("00000000000") =>
        result = true
      case _ =>
    }
    result
  }

  def isTargetFilterPlan(plan: SparkPlan): Boolean = {
    plan match {
      case BroadcastExchangeExec(_,
      FilterExec(condition,
      ColumnarToRowExec(
      scan: FileSourceScanExec), _)) => isTargetCondition(condition)

      case SortExec(_, _,
      ShuffleExchangeExec(_,
      FilterExec(condition,
      ColumnarToRowExec(
      scan: FileSourceScanExec),_), _), _) => isTargetCondition(condition)

      case _ => false
    }
  }

  def shouldReplaceCountOne(plan: SparkPlan): Boolean = {
    plan match {
      case DataWritingCommandExec(_,
      finalAgg@HashAggregateExec(_, groups: Seq[NamedExpression], aggExps: Seq[AggregateExpression], _, _, _,
      shuffle@ShuffleExchangeExec(_,
      ptAgg@HashAggregateExec(_, _, _, _, _, _,
      ColumnarToRowExec(
      scan: FileSourceScanExec)), _))) =>
        if (groups.nonEmpty) {
          return false
        }
        if (aggExps.isEmpty) {
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

        val countTable = scan.tableIdentifier.get
        val stats = plan.session.sqlContext.sparkSession.sessionState.catalog
          .getTableMetadata(countTable).stats
        if (stats.isEmpty) {
          return false
        }
        val countValue = stats.get.rowCount
        if (countValue.isEmpty) {
          return false
        }
        columnStat = countValue.get
        shouldReplaceScan = true
        shouldReplaceFinalAgg = true
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
        if (topFinalAgg.groupingExpressions.nonEmpty) {
          return false
        }
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

        val stats = plan.session.sqlContext.sparkSession.sessionState.catalog
          .getTableMetadata(distinctTable).stats
        if (stats.isEmpty) {
          return false
        }
        val colStatsMap = stats.map(_.colStats).getOrElse(Map.empty)
        if (colStatsMap.isEmpty) {
          return false
        }
        if (colStatsMap(distinctColumn) == null) {
          return false
        }
        columnStat = colStatsMap(distinctColumn).distinctCount.get
        shouldReplaceAllAgg = true
        true
      case _ => false
    }
  }

  def replaceCountPlan(plan: SparkPlan): SparkPlan = plan match {
    case scan: FileSourceScanExec if (shouldReplaceScan) =>
      SimpleCountFileScanExec(scan.relation,
        scan.output,
        scan.requiredSchema,
        scan.partitionFilters,
        scan.optionalBucketSet,
        scan.optionalNumCoalescedBuckets,
        scan.dataFilters,
        scan.tableIdentifier,
        scan.disableBucketedScan,
        isEmptyIter = true)
    case agg@HashAggregateExec(_, _, _, _, _, _, shuffle: ShuffleExchangeExec) if (shouldReplaceFinalAgg) =>
      val child = replaceCountPlan(agg.child)
      SimpleCountAggregateExec(agg.requiredChildDistributionExpressions,
        agg.groupingExpressions,
        agg.aggregateExpressions,
        agg.aggregateAttributes,
        agg.initialInputBufferOffset,
        agg.resultExpressions,
        child,
        isDistinctCount = true,
        columnStat)
    case agg: HashAggregateExec if (shouldReplaceAllAgg) =>
      val child = replaceCountPlan(agg.child)
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
      val children = plan.children.map(replaceCountPlan)
      p.withNewChildren(children)
  }
}
