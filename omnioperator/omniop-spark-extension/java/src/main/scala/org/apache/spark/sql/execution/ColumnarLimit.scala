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

import com.huawei.boostkit.spark.Constant.IS_SKIP_VERIFY_EXP
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor.{checkOmniJsonWhiteList, getExprIdMap, isSimpleColumnForAll, rewriteToOmniJsonExpressionLiteral, sparkTypeToOmniType}
import com.huawei.boostkit.spark.serialize.ColumnarBatchSerializer
import com.huawei.boostkit.spark.util.OmniAdaptorUtil
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.{addAllAndGetIterator, genSortParam}
import nova.hetu.omniruntime.`type`.DataType
import nova.hetu.omniruntime.operator.config.{OperatorConfig, OverflowConfig, SpillConfig}
import nova.hetu.omniruntime.operator.topn.OmniTopNWithExprOperatorFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution, Partitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.ColumnarProjection.dealPartitionData
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.execution.util.SparkMemoryUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.concurrent.TimeUnit.NANOSECONDS

trait ColumnarBaseLimitExec extends LimitExec {

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = child.output

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar().mapPartitions { iter =>
      val hasInput = iter.hasNext
      if (hasInput) {
        new Iterator[ColumnarBatch] {
          var rowCount = 0
          override def hasNext: Boolean = {
            val hasNext = iter.hasNext
            hasNext && (rowCount < limit)
          }

          override def next(): ColumnarBatch = {
            val output = iter.next()
            val preRowCount = rowCount
            rowCount += output.numRows
            if (rowCount > limit) {
              val newSize = limit - preRowCount
              output.setNumRows(newSize)
            }
            output
          }
        }
      } else {
        Iterator.empty
      }
    }
  }

  protected override def doExecute() = {
    throw new UnsupportedOperationException("This operator doesn't support doExecute()")
  }
}

case class ColumnarLocalLimitExec(limit: Int, child: SparkPlan)
  extends ColumnarBaseLimitExec{

  override def nodeName: String = "OmniColumnarLocalLimit"

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

case class ColumnarGlobalLimitExec(limit: Int, child: SparkPlan)
  extends ColumnarBaseLimitExec{

  override def requiredChildDistribution: List[Distribution] = AllTuples :: Nil

  override def nodeName: String = "OmniColumnarGlobalLimit"

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

case class ColumnarTakeOrderedAndProjectExec(
                                              limit: Int,
                                              sortOrder: Seq[SortOrder],
                                              projectList: Seq[NamedExpression],
                                              child: SparkPlan)
  extends UnaryExecNode {

  override def supportsColumnar: Boolean = true

  override def nodeName: String = "OmniColumnarTakeOrderedAndProject"

  override protected def withNewChildInternal(newChild: SparkPlan):
  ColumnarTakeOrderedAndProjectExec = copy(child = newChild)

  val serializer: Serializer = new ColumnarBatchSerializer(
    longMetric("avgReadBatchNumRows"),
    longMetric("numOutputRows"))
  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "bytesSpilled" -> SQLMetrics.createSizeMetric(sparkContext, "shuffle bytes spilled"),
    "splitTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_split"),
    "spillTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "shuffle spill time"),
    "avgReadBatchNumRows" -> SQLMetrics
      .createAverageMetric(sparkContext, "avg read batch num rows"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics
      .createMetric(sparkContext, "number of output rows"),
    // omni
    "outputDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "output data size"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
    "getOutputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni getOutput"),
    "omniCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni codegen"),
    "numInputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of input vecBatchs"),
    "numOutputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatchs"),
    "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput")
  ) ++ readMetrics ++ writeMetrics

  override def output: Seq[Attribute] = {
    projectList.map(_.toAttribute)
  }

  override def executeCollect(): Array[InternalRow] = {
    throw new UnsupportedOperationException
  }

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException
  }

  def buildCheck(): Unit = {
    genSortParam(child.output, sortOrder)
    val projectEqualChildOutput = projectList == child.output
    var omniInputTypes: Array[DataType] = null
    var omniExpressions: Array[AnyRef] = null
    if (!projectEqualChildOutput) {
      omniInputTypes = child.output.map(
        exp => sparkTypeToOmniType(exp.dataType, exp.metadata)).toArray
      omniExpressions = projectList.map(
        exp => rewriteToOmniJsonExpressionLiteral(exp, getExprIdMap(child.output))).toArray
      if (!isSimpleColumnForAll(omniExpressions.map(expr => expr.toString))) {
        checkOmniJsonWhiteList("", omniExpressions)
      }
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val (sourceTypes, ascendings, nullFirsts, sortColsExp) = genSortParam(child.output, sortOrder)

    def computeTopN(iter: Iterator[ColumnarBatch], schema: StructType): Iterator[ColumnarBatch] = {
      val startCodegen = System.nanoTime()
      val topNOperatorFactory = new OmniTopNWithExprOperatorFactory(sourceTypes, limit, sortColsExp, ascendings, nullFirsts,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val topNOperator = topNOperatorFactory.createOperator
      longMetric("omniCodegenTime") += NANOSECONDS.toMillis(System.nanoTime() - startCodegen)
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit]( _ => {
        topNOperator.close()
      })
      addAllAndGetIterator(topNOperator, iter, schema,
        longMetric("addInputTime"), longMetric("numInputVecBatchs"), longMetric("numInputRows"),
        longMetric("getOutputTime"), longMetric("numOutputVecBatchs"), longMetric("numOutputRows"),
        longMetric("outputDataSize"))
    }

    val localTopK: RDD[ColumnarBatch] = {
      child.executeColumnar().mapPartitionsWithIndexInternal { (_, iter) =>
        computeTopN(iter, this.child.schema)
      }
    }

    val shuffled = new ShuffledColumnarRDD(
      ColumnarShuffleExchangeExec.prepareShuffleDependency(
        localTopK,
        child.output,
        SinglePartition,
        serializer,
        writeMetrics,
        longMetric("dataSize"),
        longMetric("bytesSpilled"),
        longMetric("numInputRows"),
        longMetric("splitTime"),
        longMetric("spillTime")),
      readMetrics)
    val projectEqualChildOutput = projectList == child.output
    var omniInputTypes: Array[DataType] = null
    var omniExpressions: Array[String] = null
    var addInputTime: SQLMetric = null
    var omniCodegenTime: SQLMetric = null
    var getOutputTime: SQLMetric = null
    if (!projectEqualChildOutput) {
      omniInputTypes = child.output.map(
        exp => sparkTypeToOmniType(exp.dataType, exp.metadata)).toArray
      omniExpressions = projectList.map(
        exp => rewriteToOmniJsonExpressionLiteral(exp, getExprIdMap(child.output))).toArray
      addInputTime = longMetric("addInputTime")
      omniCodegenTime = longMetric("omniCodegenTime")
      getOutputTime = longMetric("getOutputTime")
    }
    shuffled.mapPartitions { iter =>
      // TopN = omni-top-n + omni-project
      val topN: Iterator[ColumnarBatch] = computeTopN(iter, this.child.schema)
      if (!projectEqualChildOutput) {
        dealPartitionData(null, null, addInputTime, omniCodegenTime,
          getOutputTime, omniInputTypes, omniExpressions, topN, this.schema)
      } else {
        topN
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def outputPartitioning: Partitioning = SinglePartition

  override def simpleString(maxFields: Int): String = {
    val orderByString = truncatedString(sortOrder, "[", ",", "]", maxFields)
    val outputString = truncatedString(output, "[", ",", "]", maxFields)

    s"OmniColumnarTakeOrderedAndProject(limit=$limit, orderBy=$orderByString, output=$outputString)"
  }
}