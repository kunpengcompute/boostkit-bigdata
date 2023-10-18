/*
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
import nova.hetu.omniruntime.operator.topnsort.OmniTopNSortWithExprOperatorFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.util.SparkMemoryUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

case class ColumnarTopNSortExec(
    n: Int,
    strictTopN: Boolean,
    partitionSpec: Seq[Expression],
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan)
  extends UnaryExecNode {


  override def supportsColumnar: Boolean = true

  override def nodeName: String = "OmniColumnarTopNSort"

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarTopNSortExec =
    copy(child = newChild)

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  override lazy val metrics = Map(

    "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
    "numInputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of input vecBatchs"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "omniCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni codegen"),
    "getOutputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni getOutput"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "outputDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "output data size"),
    "numOutputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatchs"))

  def buildCheck(): Unit = {
    val omniAttrExpsIdMap = getExprIdMap(child.output)
    val omniPartitionChanels: Array[AnyRef] = partitionSpec.map(
      exp => rewriteToOmniJsonExpressionLiteral(exp, omniAttrExpsIdMap)).toArray
    checkOmniJsonWhiteList("", omniPartitionChanels)
    genSortParam(child.output, sortOrder)
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val omniCodegenTime = longMetric("omniCodegenTime")
    val omniAttrExpsIdMap = getExprIdMap(child.output)
    val omniPartitionChanels = partitionSpec.map(
      exp => rewriteToOmniJsonExpressionLiteral(exp, omniAttrExpsIdMap)).toArray
    val (sourceTypes, ascendings, nullFirsts, sortColsExp) = genSortParam(child.output, sortOrder)

    child.executeColumnar().mapPartitionsWithIndexInternal { (_, iter) =>
      val startCodegen = System.nanoTime()
      val topNSortOperatorFactory = new OmniTopNSortWithExprOperatorFactory(sourceTypes, n,
        strictTopN, omniPartitionChanels, sortColsExp, ascendings, nullFirsts,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val topNSortOperator = topNSortOperatorFactory.createOperator
      omniCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startCodegen)
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        topNSortOperator.close()
      })
      addAllAndGetIterator(topNSortOperator, iter, this.schema,
        longMetric("addInputTime"), longMetric("numInputVecBatchs"), longMetric("numInputRows"),
        longMetric("getOutputTime"), longMetric("numOutputVecBatchs"), longMetric("numOutputRows"),
        longMetric("outputDataSize"))
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }
}