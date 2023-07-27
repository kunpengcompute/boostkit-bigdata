/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

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

package org.apache.spark.sql.execution.adaptive.ock.exchange

import com.huawei.boostkit.spark.ColumnarPluginConfig
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor._
import com.huawei.boostkit.spark.serialize.ColumnarBatchSerializer

import nova.hetu.omniruntime.`type`.DataType

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.adaptive.ock.common.BoostTuningLogger._
import org.apache.spark.sql.execution.adaptive.ock.common.BoostTuningUtil._
import org.apache.spark.sql.execution.adaptive.ock.exchange.estimator._
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleExchangeExec, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.execution.metric._
import org.apache.spark.sql.execution.util.MergeIterator
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.{MapOutputStatistics, ShuffleDependency}
import org.apache.spark.util.MutablePair

import scala.concurrent.Future

case class BoostTuningColumnarShuffleExchangeExec(
                                   override val outputPartitioning: Partitioning,
                                   child: SparkPlan,
                                   shuffleOrigin: ShuffleOrigin = ENSURE_REQUIREMENTS,
                                   @transient context: PartitionContext) extends BoostTuningShuffleExchangeLike{

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "bytesSpilled" -> SQLMetrics.createSizeMetric(sparkContext, "shuffle bytes spilled"),
    "splitTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_split"),
    "spillTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "shuffle spill time"),
    "compressTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_compress"),
    "avgReadBatchNumRows" -> SQLMetrics
      .createAverageMetric(sparkContext, "avg read batch num rows"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numMergedVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of merged vecBatchs"),
    "numOutputRows" -> SQLMetrics
      .createMetric(sparkContext, "number of output rows")) ++ readMetrics ++ writeMetrics

  override def nodeName: String = "BoostTuningOmniColumnarShuffleExchange"
  
  override def getContext: PartitionContext = context

  override def getDependency: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = boostTuningColumnarShuffleDependency

  override def getUpStreamDataSize: Long = collectUpStreamInputDataSize(this)

  override def getPartitionEstimators: Seq[PartitionEstimator] = estimators

  @transient val helper: BoostTuningShuffleExchangeHelper = 
    new BoostTuningColumnarShuffleExchangeHelper(this, sparkContext)

  @transient lazy val estimators: Seq[PartitionEstimator] = Seq(
    UpStreamPartitionEstimator(),
    ColumnarSamplePartitionEstimator(helper.executionMem)) ++ Seq(
    SinglePartitionEstimator(),
    ColumnarElementsForceSpillPartitionEstimator()
  )

  override def supportsColumnar: Boolean = true

  val serializer: Serializer = new ColumnarBatchSerializer(
    longMetric("avgReadBatchNumRows"),
    longMetric("numOutputRows"))

  @transient lazy val inputColumnarRDD: RDD[ColumnarBatch] = child.executeColumnar()

  // 'mapOutputStatisticsFuture' is only needed when enable AQE.
  @transient override lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (inputColumnarRDD.getNumPartitions == 0) {
      context.setSelfAndDepPartitionNum(outputPartitioning.numPartitions)
      Future.successful(null)
    } else {
      omniAdaptivePartitionWithMapOutputStatistics()
    }
  }

  private def omniAdaptivePartitionWithMapOutputStatistics(): Future[MapOutputStatistics] = {
      helper.cachedSubmitMapStage() match {
        case Some(f) => return f
        case _ =>
      }

      helper.onlineSubmitMapStage() match {
        case f: Future[MapOutputStatistics] => f
        case _ => Future.failed(null)
      }
  }

  override def numMappers: Int = boostTuningColumnarShuffleDependency.rdd.getNumPartitions

  override def numPartitions: Int = boostTuningColumnarShuffleDependency.partitioner.numPartitions

  override def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[InternalRow] = {
      throw new IllegalArgumentException("Failed to getShuffleRDD, exec should use ColumnarBatch but not InternalRow")
  }

  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN).value
    Statistics(dataSize, Some(rowCount))
  }

  @transient
  lazy val boostTuningColumnarShuffleDependency: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    val partitionInitTime = System.currentTimeMillis()
    val newOutputPartitioning = helper.replacePartitionWithNewNum()
    val dep = ColumnarShuffleExchangeExec.prepareShuffleDependency(
      inputColumnarRDD,
      child.output,
      newOutputPartitioning,
      serializer,
      writeMetrics,
      longMetric("dataSize"),
      longMetric("bytesSpilled"),
      longMetric("numInputRows"),
      longMetric("splitTime"),
      longMetric("spillTime"))
    val partitionReadyTime = System.currentTimeMillis()
    TLogInfo(s"BoostTuningShuffleExchange $id input partition ${inputColumnarRDD.getNumPartitions}" +
    s" modify ${if (helper.isAdaptive) "adaptive" else "global"}" + 
    s" partitionNum ${outputPartitioning.numPartitions} -> ${newOutputPartitioning.numPartitions}" + 
    s" cost ${partitionReadyTime - partitionInitTime} ms")
    dep
  }

  var cachedShuffleRDD: ShuffledColumnarRDD = _

  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }

  def buildCheck(): Unit = {
    val inputTypes = new Array[DataType](child.output.size)
    child.output.zipWithIndex.foreach {
      case (attr, i) =>
        inputTypes(i) = sparkTypeToOmniType(attr.dataType, attr.metadata)
    }

    outputPartitioning match {
      case HashPartitioning(expressions, numPartitions) =>
        val genHashExpressionFunc = ColumnarShuffleExchangeExec.genHashExpr()
        val hashJSonExpressions = genHashExpressionFunc(expressions, numPartitions, ColumnarShuffleExchangeExec.defaultMm3HashSeed, child.output)
        if (!isSimpleColumn(hashJSonExpressions)) {
          checkOmniJsonWhiteList("", Array(hashJSonExpressions))
        }
      case _ =>
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = new ShuffledColumnarRDD(boostTuningColumnarShuffleDependency, readMetrics)
    }
    val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf
    val enableShuffleBatchMerge: Boolean = columnarConf.enableShuffleBatchMerge
    if (enableShuffleBatchMerge) {
      cachedShuffleRDD.mapPartitionsWithIndexInternal { (index, iter) =>
        new MergeIterator(iter,
          StructType.fromAttributes(child.output),
          longMetric("numMergedVecBatchs"))
      }
    } else {
      cachedShuffleRDD
    }
  }
}