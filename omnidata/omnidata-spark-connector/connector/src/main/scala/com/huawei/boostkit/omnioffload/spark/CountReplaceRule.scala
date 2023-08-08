/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
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
package com.huawei.boostkit.omnioffload.spark

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SimpleCountAggregateExec}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.{ColumnarToRowExec, FileSourceScanExec, SimpleCountFileScanExec, SparkPlan}
import org.apache.spark.sql.types.DataTypes

object CountReplaceRule extends Rule[SparkPlan] {
  var columnStat: BigInt = -1
  var isCountPlan: Boolean = false

  override def apply(plan: SparkPlan): SparkPlan = {
    if (shouldReplaceDistinctCount(plan) || shouldReplaceCountOne(plan)) {
      replaceCountPlan(plan)
    } else {
      plan
    }
  }

  def shouldReplaceCountOne(plan: SparkPlan): Boolean = {
    plan match {
      case DataWritingCommandExec(_,
      HashAggregateExec(_, groups: Seq[NamedExpression], aggExps: Seq[AggregateExpression], _, _, _,
      ShuffleExchangeExec(_,
      HashAggregateExec(_, _, _, _, _, _,
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
        headAggExp.aggregateFunction match {
          case Seq(Literal(1, DataTypes.IntegerType)) =>
          case _ => return false
        }
        if (!scan.relation.fileFormat.isInstanceOf[ParquetFileFormat]) {
          return false
        }

        val countTable = scan.tableIdentifier.get
        val stats = plan.sqlContext.sparkSession.sessionState.catalog
          .getTableMetadata(countTable).stats
        if (stats.isEmpty) {
          return false
        }
        val countValue = stats.get.rowCount
        if (countValue.isEmpty) {
          return false
        }
        columnStat = countValue.get
        isCountPlan = true
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

        val stats = plan.sqlContext.sparkSession.sessionState.catalog
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
        true
      case _ => false
    }
  }

  def replaceCountPlan(plan: SparkPlan): SparkPlan = plan match {
    case scan: FileSourceScanExec if isCountPlan =>
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
    case agg@HashAggregateExec(_, _, _, _, _, _, shuffle: ShuffleExchangeExec) if isCountPlan =>
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
    case agg: HashAggregateExec if !isCountPlan =>
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
