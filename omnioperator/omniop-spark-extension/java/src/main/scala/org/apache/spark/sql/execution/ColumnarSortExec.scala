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

import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit.NANOSECONDS

import com.huawei.boostkit.spark.ColumnarPluginConfig
import com.huawei.boostkit.spark.Constant.IS_SKIP_VERIFY_EXP
import com.huawei.boostkit.spark.util.OmniAdaptorUtil
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.{addAllAndGetIterator, genSortParam}
import nova.hetu.omniruntime.operator.config.{OperatorConfig, OverflowConfig, SparkSpillConfig}
import nova.hetu.omniruntime.operator.sort.OmniSortWithExprOperatorFactory
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.util.SparkMemoryUtils
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

case class ColumnarSortExec(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan,
    testSpillFrequency: Int = 0)
  extends UnaryExecNode {
  override def supportsColumnar: Boolean = true

  override def nodeName: String = "OmniColumnarSort"

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarSortExec =
    copy(child = newChild)

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  override lazy val metrics = Map(
    "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
    "numInputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of input vecBatches"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "omniCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni codegen"),
    "getOutputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni getOutput"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "outputDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "output data size"),
    "numOutputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatches"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

  def buildCheck(): Unit = {
    genSortParam(child.output, sortOrder)
  }

  val tmpSparkConf = sparkContext.conf

  def generateSpillDir(conf: SparkConf, subDir: String): String = {
    val localDirs: Array[String] = Utils.getConfiguredLocalDirs(conf)
    val hash = Utils.nonNegativeHash(UUID.randomUUID.toString)
    val root = localDirs(hash % localDirs.length)
    val dir = new File(root, subDir)
    dir.getCanonicalPath
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val omniCodegenTime = longMetric("omniCodegenTime")
    val spillSize = longMetric("spillSize")

    val (sourceTypes, ascending, nullFirsts, sortColsExp) = genSortParam(child.output, sortOrder)
    val outputCols = output.indices.toArray

    child.executeColumnar().mapPartitionsWithIndexInternal { (_, iter) =>
      val columnarConf = ColumnarPluginConfig.getSessionConf
      val spillWriteBufferSize = columnarConf.columnarSpillWriteBufferSize
      val spillMemPctThreshold = columnarConf.columnarSpillMemPctThreshold
      val spillDirDiskReserveSize = columnarConf.columnarSpillDirDiskReserveSize
      val sortSpillEnable = columnarConf.enableSortSpill
      val sortSpillRowThreshold = columnarConf.columnarSortSpillRowThreshold
      val spillDirectory = generateSpillDir(tmpSparkConf, "columnarSortSpill")
      val sparkSpillConf = new SparkSpillConfig(sortSpillEnable, spillDirectory, spillDirDiskReserveSize,
        sortSpillRowThreshold, spillMemPctThreshold, spillWriteBufferSize)
      val startCodegen = System.nanoTime()

      val radixSortEnable = columnarConf.enableRadixSort
      val radixSortRowCountThreshold = if(radixSortEnable) {columnarConf.radixSortThreshold} else {-1}

      val sortOperatorFactory = new OmniSortWithExprOperatorFactory(sourceTypes, outputCols, sortColsExp, ascending, nullFirsts,
        new OperatorConfig(sparkSpillConf, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP, radixSortRowCountThreshold.asInstanceOf[Int]))
      val sortOperator = sortOperatorFactory.createOperator
      omniCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startCodegen)
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        spillSize += sortOperator.getSpilledBytes()
        sortOperator.close()
        sortOperatorFactory.close()
      })
      addAllAndGetIterator(sortOperator, iter, this.schema,
        longMetric("addInputTime"), longMetric("numInputVecBatches"), longMetric("numInputRows"),
        longMetric("getOutputTime"), longMetric("numOutputVecBatches"), longMetric("numOutputRows"),
        longMetric("outputDataSize"))
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }
}
