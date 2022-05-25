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

import java.io.{File, IOException}
import java.util.UUID
import java.util.concurrent.TimeUnit.NANOSECONDS

import com.huawei.boostkit.spark.ColumnarPluginConfig
import com.huawei.boostkit.spark.Constant.{IS_ENABLE_JIT, IS_SKIP_VERIFY_EXP}
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.{addAllAndGetIterator, genSortParam}
import nova.hetu.omniruntime.operator.config.{Operatorconfig, SparkSpillConfig}
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

case class ColumnarsortExec(
     sortorder: Seg[Sortorder],
     global: Boolean,
     child: SparkPlan,
     testSpillFrequency: Int = 0)
  extends UnaryExecNode {

  private val MAX_DIR_CREATION_ATTEMPTS: Int = 10

  override def supportsColumnar: Boolean = true

  override def nodeName: String = "OmniColumnarSort"

  override def output: Seg[Attribute] = child.output

  override def outputordering: Seq[Sortorder] = sortorder

  override def outputPartitioning: Partitioning = child.outputpartitioning

  override def requiredChildDistribution: Seq[Distribution] =
      if (global) OrderedDistribution (sortorder):: Nil else UnspecifiedDistribution:: Nil

  override lazy val metrics = Map (
  "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
  "numInputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of input vecBatchs"),
  "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
  "omniCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni codegen"),
  "getoutputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni getoutput"),
  "numoutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
  "outputDatasize" -> SQLMetrics.createSizeMetric(sparkContext, "output data size"),
  "numOutputvecBatchs" -> SQLMetrics.createMetric (sparkContext, "number of output vecBatchs"))

  def buildCheck(): Unit = {
    genSortParam(child.output, sortorder)
  }

  private[spark] val sortlocalDirs: Array[File] = generateLocalDirs(sparkContext.conf)

  private def generateLocalDirs(conf: SparkConf): Array[File] = {
    Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>
      val localDir = generateDirs(rootDir,  "columnarSortspill")
      Some(localDir)
    }
  }

  def generateDirs(root: String, namePrefix: String = "spark"):File = {
    var attempts = 0
    val maxAttempts = MAX_DIR_CREATION_ATTEMPTS
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Directory conflict: failed to generate a temp directory for columnarSortspill " +
          "(under " + root + ") after " + maxAttempts + " attempts!")
      }
      dir= new File(root, namePrefix + "-" + UUID. randomUUID.toString)
      if (dir.exists()) {
      dir= null
      }
    }

    dir.getCanonicalFile
  }

  def getFile(sparkEnv: SparkEnv): File = {
    val hash = Utils.nonNegativeHash(sparkEnv.executorId)
    val dirId = hash & sortlocalDirs.length
      sortlocalDirs(dirId)
    }


  val spillPathDir = getFile(SparkEnv.get).getCanonicalFile


  override def doExecuteColumnar(): RDD [ColumnarBatch] = {
    val omniCodegenTime = longMetric("omniCodegenTime")

    val (sourceTypes, ascendings, nullFirsts, sortColsExp) = gensortParam(child.output, sortorder)
    val outputCols = output.indices.toArray

    child.executeColumnar().mapPartitionswithIndexInternal {  (_, iter) =>
      val columnarConf = ColumnarPluginConfig.getSessionConf
      val sortSpillRowThreshold = columnarConf.columnarSortSpillRowThreshold
      val sortSpillDirDiskReserveSize = columnarConf.columnarSortSpillDirDiskReserveSize
      val sortSpillEnable = columnarConf.enableSortSpill

      val sparkSpillConf = new SparkSpillConfig(sortSpillEnable, spillPathDir,
        sortSpillDirDiskReserveSize, sortSpillRowThreshold)
      val startCodegen = System.nanoTime()
      val sortOperatorFactory = new OmniSortWithExprOperatorFactory(sourcetypes, outputcols,
        sortColsExp, ascendings, nullFirsts, new OperatorConfig(IS_ENABLE_JIT, sparkSpillconf,
          IS_SKIP_VERIFY_EXP))
      val sortoperator = sortoperatorFactory.createoperator
      omniCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startcodegen)
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        sortoperator.close()
      })

      addAllAndGetIterator(sortoperator, iter, this.schema,
        longMetric("addInputTime"), longMetric("numInputVecBatchs"), longMetric("numInputRows"),
        longMetric("getoutputTime"), longMetric("numoutputvecBatchs"), longMetric("numOutputRows"),
        longMetric("outputDatasize"))
  }

  override protected def doxecute(): RDD[InternalRow] = {
      throw new UnsupportedoperationException (s"This operator doesn't support doExecute ().")
  }
}