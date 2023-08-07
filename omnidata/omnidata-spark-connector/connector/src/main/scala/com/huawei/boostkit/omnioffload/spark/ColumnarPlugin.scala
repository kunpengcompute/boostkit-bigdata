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

import com.huawei.boostkit.omnidata.spark.NdpConnectorUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Contains, EndsWith, Expression, Literal, NamedExpression, StartsWith}
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftOuter}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.ndp.NdpConf.getOptimizerPushDownThreshold
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

import scala.collection.JavaConverters

case class NdpOverrides(sparkSession: SparkSession) extends Rule[SparkPlan] {

  var numPartitions: Int = -1
  var pushDownTaskCount: Int = -1
  var isSMJ = false
  var isSort = false
  var hasCoalesce = false
  var hasShuffle = false
  var accurateQueryHd = "153"
  var accurateQuery = "000"

  def apply(plan: SparkPlan): SparkPlan = {
    preRuleApply(plan)
    val ruleList = Seq(CountReplaceRule)
    val afterPlan = ruleList.foldLeft(plan) { case (sp, rule) =>
      val result = rule.apply(sp)
      result
    }
    val optimizedPlan = replaceWithOptimizedPlan(afterPlan)
    val finalPlan = replaceWithScanPlan(optimizedPlan)
    postRuleApply(finalPlan)
    finalPlan
  }

  def preRuleApply(plan: SparkPlan): Unit = {
    numPartitions = SQLConf.get.getConfString("spark.omni.sql.ndpPlugin.coalesce.numPartitions",
      NdpConnectorUtils.getNdpNumPartitionsStr("10000")).toInt
    pushDownTaskCount = NdpConnectorUtils.getPushDownTaskTotal(getOptimizerPushDownThreshold(sparkSession))
    if (CountReplaceRule.shouldReplaceCountOne(plan)) {
      pushDownTaskCount = NdpConnectorUtils.getCountTaskTotal(200)
      SQLConf.get.setConfString(SQLConf.FILES_MAX_PARTITION_BYTES.key,
        NdpConnectorUtils.getCountMaxPartSize("512MB"))
    }
    if (CountReplaceRule.shouldReplaceDistinctCount(plan)) {
      pushDownTaskCount = NdpConnectorUtils.getCountDistinctTaskTotal(2000)
    }
  }

  def postRuleApply(plan: SparkPlan): Unit = {
    if (isSMJ) {
      SQLConf.get.setConfString(SQLConf.FILES_MAX_PARTITION_BYTES.key,
        NdpConnectorUtils.getSMJMaxPartSize("536870912"))
    }
  }

  //now set task total number, we can use this number pushDown task in thread
  def replaceWithScanPlan(plan: SparkPlan): SparkPlan = {
    val p = plan.transformUp {
      case scan: FileSourceScanExec =>
        scan.setRuntimePushDownSum(pushDownTaskCount)
        if (hasCoalesce && !hasShuffle) {
          // without shuffle , coalesce num is task num
          scan.setRuntimePartSum(numPartitions)
        }
        scan
      case p => p
    }
    p
  }

  def replaceWithOptimizedPlan(plan: SparkPlan): SparkPlan = {
    val p = plan.transformUp {
      case p@DataWritingCommandExec(cmd, child) =>
        if (isSort || isVagueAndAccurateHd(child)) {
          p
        } else {
          hasCoalesce = true
          DataWritingCommandExec(cmd, CoalesceExec(numPartitions, child))
        }
      case p@ColumnarSortMergeJoinExec(_, _, joinType, _, _, _, _, projectList)
        if joinType.equals(LeftOuter) =>
        isSMJ = true
        numPartitions = NdpConnectorUtils.getSMJNumPartitions(5000)
        ColumnarSortMergeJoinExec(leftKeys = p.leftKeys, rightKeys = p.rightKeys, joinType = LeftAnti,
          condition = p.condition, left = p.left, right = p.right, isSkewJoin = p.isSkewJoin, projectList)
      case p@SortMergeJoinExec(_, _, joinType, _, _, _, _)
        if joinType.equals(LeftOuter) =>
        isSMJ = true
        numPartitions = NdpConnectorUtils.getSMJNumPartitions(5000)
        SortMergeJoinExec(leftKeys = p.leftKeys, rightKeys = p.rightKeys, joinType = LeftAnti, condition = p.condition,
          left = p.left, right = p.right, isSkewJoin = p.isSkewJoin)
      case p@ColumnarBroadcastHashJoinExec(_, _, joinType, _, _, _, _, _, projectList) if joinType.equals(LeftOuter) =>
        ColumnarBroadcastHashJoinExec(leftKeys = p.leftKeys, rightKeys = p.rightKeys,
          joinType = LeftAnti, buildSide = p.buildSide, condition = p.condition, left = p.left,
          right = p.right, isNullAwareAntiJoin = p.isNullAwareAntiJoin, projectList)
      case p@BroadcastHashJoinExec(_, _, joinType, _, _, _, _, _) if joinType.equals(LeftOuter) =>
        BroadcastHashJoinExec(leftKeys = p.leftKeys, rightKeys = p.rightKeys, joinType = LeftAnti,
          buildSide = p.buildSide, condition = p.condition, left = p.left, right = p.right,
          isNullAwareAntiJoin = p.isNullAwareAntiJoin)
      case p@ColumnarShuffledHashJoinExec(_, _, joinType, _, _, _, _, projectList)
        if joinType.equals(LeftOuter) =>
        ColumnarShuffledHashJoinExec(p.leftKeys, p.rightKeys, LeftAnti, p.buildSide, p.condition,
          p.left, p.right, projectList)
      case p@ShuffledHashJoinExec(_, _, joinType, _, _, _, _) if joinType.equals(LeftOuter) =>
        ShuffledHashJoinExec(p.leftKeys, p.rightKeys, LeftAnti, p.buildSide, p.condition, p.left, p.right)
      case p@FilterExec(condition, child: OmniColumnarToRowExec, selectivity) =>
        val childPlan = child.transform {
          case p@OmniColumnarToRowExec(child: NdpFileSourceScanExec) =>
            ColumnarToRowExec(FileSourceScanExec(child.relation,
              child.output,
              child.requiredSchema,
              child.partitionFilters,
              child.optionalBucketSet,
              child.optionalNumCoalescedBuckets,
              child.dataFilters,
              child.tableIdentifier,
              child.partitionColumn,
              child.disableBucketedScan))
          case p@OmniColumnarToRowExec(child: FileSourceScanExec) =>
            ColumnarToRowExec(child)
          case p => p
        }
        FilterExec(condition, childPlan, selectivity)
      case c1@OmniColumnarToRowExec(c2@ColumnarFilterExec(condition, c3: FileSourceScanExec)) =>
        numPartitions = NdpConnectorUtils.getOmniColumnarNumPartitions(1000)
        if (isAccurate(condition)) {
          pushDownTaskCount = NdpConnectorUtils.getOmniColumnarTaskCount(400)
        }
        FilterExec(condition, ColumnarToRowExec(c3))
      case p@FilterExec(condition, _, _) if isAccurate(condition) =>
        numPartitions = NdpConnectorUtils.getFilterPartitions(1000)
        pushDownTaskCount = NdpConnectorUtils.getFilterTaskCount(400)
        p
      case p@ColumnarConditionProjectExec(projectList, condition, child)
        if condition.toString().startsWith("isnull") && (child.isInstanceOf[ColumnarSortMergeJoinExec]
          || child.isInstanceOf[ColumnarBroadcastHashJoinExec] || child.isInstanceOf[ColumnarShuffledHashJoinExec]) =>
        ColumnarProjectExec(changeProjectList(projectList), child)
      case p@ProjectExec(projectList, filter: FilterExec)
        if filter.condition.toString().startsWith("isnull") && (filter.child.isInstanceOf[SortMergeJoinExec]
          || filter.child.isInstanceOf[BroadcastHashJoinExec] || filter.child.isInstanceOf[ShuffledHashJoinExec]) =>
        ProjectExec(changeProjectList(projectList), filter.child)
      case p: SortAggregateExec if p.child.isInstanceOf[OmniColumnarToRowExec]
        && p.child.asInstanceOf[OmniColumnarToRowExec].child.isInstanceOf[ColumnarSortExec]
        && isAggPartial(p.aggregateAttributes) =>
        val omniColumnarToRow = p.child.asInstanceOf[OmniColumnarToRowExec]
        val omniColumnarSort = omniColumnarToRow.child.asInstanceOf[ColumnarSortExec]
        SortAggregateExec(p.requiredChildDistributionExpressions,
          p.groupingExpressions,
          p.aggregateExpressions,
          p.aggregateAttributes,
          p.initialInputBufferOffset,
          p.resultExpressions,
          SortExec(omniColumnarSort.sortOrder,
            omniColumnarSort.global,
            ColumnarToRowExec(omniColumnarSort.child),
            omniColumnarSort.testSpillFrequency))
      case p: SortAggregateExec if p.child.isInstanceOf[OmniColumnarToRowExec]
        && p.child.asInstanceOf[OmniColumnarToRowExec].child.isInstanceOf[ColumnarSortExec]
        && isAggFinal(p.aggregateAttributes) =>
        val omniColumnarToRow = p.child.asInstanceOf[OmniColumnarToRowExec]
        val omniColumnarSort = omniColumnarToRow.child.asInstanceOf[ColumnarSortExec]
        val omniShuffleExchange = omniColumnarSort.child.asInstanceOf[ColumnarShuffleExchangeExec]
        val rowToOmniColumnar = omniShuffleExchange.child.asInstanceOf[RowToOmniColumnarExec]
        SortAggregateExec(p.requiredChildDistributionExpressions,
          p.groupingExpressions,
          p.aggregateExpressions,
          p.aggregateAttributes,
          p.initialInputBufferOffset,
          p.resultExpressions,
          SortExec(omniColumnarSort.sortOrder,
            omniColumnarSort.global,
            ShuffleExchangeExec(omniShuffleExchange.outputPartitioning, rowToOmniColumnar.child,
              omniShuffleExchange.shuffleOrigin),
            omniColumnarSort.testSpillFrequency))
      case p@OmniColumnarToRowExec(agg: ColumnarHashAggregateExec)
        if agg.groupingExpressions.nonEmpty && agg.child.isInstanceOf[ColumnarShuffleExchangeExec] =>
        val omniExchange = agg.child.asInstanceOf[ColumnarShuffleExchangeExec]
        val omniHashAgg = omniExchange.child.asInstanceOf[ColumnarHashAggregateExec]
        HashAggregateExec(agg.requiredChildDistributionExpressions,
          agg.groupingExpressions,
          agg.aggregateExpressions,
          agg.aggregateAttributes,
          agg.initialInputBufferOffset,
          agg.resultExpressions,
          ShuffleExchangeExec(omniExchange.outputPartitioning,
            HashAggregateExec(omniHashAgg.requiredChildDistributionExpressions,
              omniHashAgg.groupingExpressions,
              omniHashAgg.aggregateExpressions,
              omniHashAgg.aggregateAttributes,
              omniHashAgg.initialInputBufferOffset,
              omniHashAgg.resultExpressions,
              ColumnarToRowExec(omniHashAgg.child)),
            omniExchange.shuffleOrigin))
      case p => p
    }
    p
  }

  def isAggPartial(aggAttributes: Seq[Attribute]): Boolean = {
    aggAttributes.exists(x => x.name.equals("max") || x.name.equals("maxxx"))
  }

  def isAggFinal(aggAttributes: Seq[Attribute]): Boolean = {
    aggAttributes.exists(x => x.name.contains("avg(cast"))
  }

  def isVagueAndAccurateHd(child: SparkPlan): Boolean = {
    var result = false
    child match {
      case filter: FilterExec =>
        filter.child match {
          case columnarToRow: ColumnarToRowExec =>
            if (columnarToRow.child.isInstanceOf[FileSourceScanExec]) {
              filter.condition.foreach { x =>
                if (x.isInstanceOf[StartsWith] || x.isInstanceOf[EndsWith] || x.isInstanceOf[Contains]) {
                  result = true
                }
                x match {
                  case literal: Literal if literal.value.toString.startsWith(accurateQueryHd) =>
                    result = true
                  case _ =>
                }
              }
            }
          case _ =>
        }
      case _ =>
    }
    result
  }

  def isAccurate(condition: Expression): Boolean = {
    var result = false
    condition.foreach {
      case literal: Literal if literal.value.toString.startsWith(accurateQuery) =>
        result = true
      case _ =>
    }
    result
  }

  def changeProjectList(projectList: Seq[NamedExpression]): Seq[NamedExpression] = {
    val p = projectList.map {
      case exp: Alias =>
        Alias(Literal(null, exp.dataType), exp.name)(
          exprId = exp.exprId,
          qualifier = exp.qualifier,
          explicitMetadata = exp.explicitMetadata,
          nonInheritableMetadataKeys = exp.nonInheritableMetadataKeys
        )
      case exp => exp
    }
    p
  }
}

case class NdpRules(session: SparkSession) extends ColumnarRule with Logging {

  def ndpOverrides: NdpOverrides = NdpOverrides(session)

  override def preColumnarTransitions: Rule[SparkPlan] = plan => {
    plan
  }

  override def postColumnarTransitions: Rule[SparkPlan] = plan => {
    if (NdpPluginEnableFlag.isEnable(plan.sqlContext.sparkSession)) {
      val rule = ndpOverrides
      rule(plan)
    } else {
      plan
    }
  }
}

object NdpPluginEnableFlag {

  val ndpEnabledStr = "spark.omni.sql.ndpPlugin.enabled"

  def isMatchedIpAddress: Boolean = {
    val ipSet = Set("xxx.xxx.xxx.xxx")
    val hostAddrSet = JavaConverters.asScalaSetConverter(NdpConnectorUtils.getIpAddress).asScala
    val res = ipSet & hostAddrSet
    res.nonEmpty
  }

  def isEnable(session: SparkSession): Boolean = {
    def ndpEnabled: Boolean = session.sqlContext.getConf(
      ndpEnabledStr, "true").trim.toBoolean
    ndpEnabled && (isMatchedIpAddress || NdpConnectorUtils.getNdpEnable)
  }

  def isEnable: Boolean = {
    def ndpEnabled: Boolean = sys.props.getOrElse(
      ndpEnabledStr, "true").trim.toBoolean
    ndpEnabled && (isMatchedIpAddress || NdpConnectorUtils.getNdpEnable)
  }
}
