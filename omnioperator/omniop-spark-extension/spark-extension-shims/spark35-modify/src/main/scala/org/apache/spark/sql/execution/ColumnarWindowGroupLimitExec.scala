/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
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

package org.apache.spark.sql.execution

import java.util.concurrent.TimeUnit.NANOSECONDS
import com.huawei.boostkit.spark.Constant.IS_SKIP_VERIFY_EXP
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor._
import com.huawei.boostkit.spark.util.OmniAdaptorUtil
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.{addAllAndGetIterator, genSortParam}
import nova.hetu.omniruntime.operator.config.{OperatorConfig, OverflowConfig, SpillConfig}
import nova.hetu.omniruntime.operator.window.OmniWindowGroupLimitWithExprOperatorFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, SortOrder, Rank, RowNumber, DenseRank}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.util.SparkMemoryUtils
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.execution.window.{WindowGroupLimitMode, Partial, Final}

/**
 * This operator is designed to filter out unnecessary rows before WindowExec
 * for top-k computation.
 * @param partitionSpec Should be the same as [[WindowExec#partitionSpec]].
 * @param orderSpec Should be the same as [[WindowExec#orderSpec]].
 * @param rankLikeFunction The function to compute row rank, should be RowNumber/Rank/DenseRank.
 * @param limit The limit for rank value.
 * @param mode The mode describes [[WindowGroupLimitExec]] before or after shuffle.
 * @param child The child spark plan.
 */
case class ColumnarWindowGroupLimitExec(
                                 partitionSpec: Seq[Expression],
                                 orderSpec: Seq[SortOrder],
                                 rankLikeFunction: Expression,
                                 limit: Int,
                                 mode: WindowGroupLimitMode,
                                 child: SparkPlan) extends UnaryExecNode {

  override def supportsColumnar: Boolean = true

  override def nodeName: String = "OmniColumnarWindowGroupLimit"

  override def outputOrdering: Seq[SortOrder] = orderSpec

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarWindowGroupLimitExec =
    copy(child = newChild)

  override def requiredChildDistribution: Seq[Distribution] = mode match {
    case Partial => super.requiredChildDistribution
    case Final =>
      if (partitionSpec.isEmpty) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(partitionSpec) :: Nil
      }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics = Map(
    "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
    "numInputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of input vecBatches"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "omniCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni codegen"),
    "getOutputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni getOutput"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "outputDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "output data size"),
    "numOutputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatches"))

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  def buildCheck(): Unit = {
    val omniAttrExpsIdMap = getExprIdMap(child.output)
    val omniPartitionChanels: Array[AnyRef] = partitionSpec.map(
      exp => rewriteToOmniJsonExpressionLiteral(exp, omniAttrExpsIdMap)).toArray
    checkOmniJsonWhiteList("", omniPartitionChanels)
    genSortParam(child.output, orderSpec)
  }


  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val omniCodegenTime = longMetric("omniCodegenTime")
    val omniAttrExpsIdMap = getExprIdMap(child.output)
    val omniPartitionChanels = partitionSpec.map(
      exp => rewriteToOmniJsonExpressionLiteral(exp, omniAttrExpsIdMap)).toArray
    val (sourceTypes, ascending, nullFirsts, sortColsExp) = genSortParam(child.output, orderSpec)

    child.executeColumnar().mapPartitionsWithIndexInternal { (_, iter) =>
      val startCodegen = System.nanoTime()
      val windowGroupLimitOperatorFactory = rankLikeFunction match {
        case _: RowNumber =>
          new OmniWindowGroupLimitWithExprOperatorFactory(sourceTypes, limit,
            "row_number", omniPartitionChanels, sortColsExp, ascending, nullFirsts,
            new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
        case _: Rank =>
          new OmniWindowGroupLimitWithExprOperatorFactory(sourceTypes, limit,
            "rank", omniPartitionChanels, sortColsExp, ascending, nullFirsts,
            new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
        case _: DenseRank =>
          throw new UnsupportedOperationException(s"This operator doesn't support DenseRank() doExecuteColumnar().")
      }
      val windowGroupLimitOperator = windowGroupLimitOperatorFactory.createOperator
      omniCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startCodegen)
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        windowGroupLimitOperator.close()
        windowGroupLimitOperatorFactory.close()
      })
      addAllAndGetIterator(windowGroupLimitOperator, iter, this.schema,
        longMetric("addInputTime"), longMetric("numInputVecBatches"), longMetric("numInputRows"),
        longMetric("getOutputTime"), longMetric("numOutputVecBatches"), longMetric("numOutputRows"),
        longMetric("outputDataSize"))
    }
  }

}