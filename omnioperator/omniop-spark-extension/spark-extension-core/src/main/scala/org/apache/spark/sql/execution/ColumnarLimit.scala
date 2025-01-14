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
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.{addAllAndGetIterator, genSortParam, transColBatchToOmniVecs}
import nova.hetu.omniruntime.`type`.DataType
import nova.hetu.omniruntime.operator.config.{OperatorConfig, OverflowConfig, SpillConfig}
import nova.hetu.omniruntime.operator.limit.OmniLimitOperatorFactory
import nova.hetu.omniruntime.operator.topn.OmniTopNWithExprOperatorFactory
import nova.hetu.omniruntime.vector.VecBatch

import org.apache.spark.rdd.{ParallelCollectionRDD, RDD}
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution, Partitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.ColumnarProjection.dealPartitionData
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.execution.util.SparkMemoryUtils
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.concurrent.TimeUnit.NANOSECONDS

trait ColumnarBaseLimitExec extends LimitExec {

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = child.output

  override lazy val metrics = Map(
    "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
    "omniCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni codegen"),
    "getOutputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni getOutput"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatches"))

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val addInputTime = longMetric("addInputTime")
    val omniCodegenTime = longMetric("omniCodegenTime")
    val getOutputTime = longMetric("getOutputTime")
    val numOutputRows = longMetric("numOutputRows")
    val numOutputVecBatches= longMetric("numOutputVecBatches")

    child.executeColumnar().mapPartitions { iter =>

      val startCodegen = System.nanoTime()
      val limitOperatorFactory = new OmniLimitOperatorFactory(limit)
      val limitOperator = limitOperatorFactory.createOperator
      omniCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startCodegen)

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        limitOperator.close()
        limitOperatorFactory.close()
      })

      val localSchema = this.schema
      new Iterator[ColumnarBatch] {
        private var results: java.util.Iterator[VecBatch] = _

        override def hasNext: Boolean = {
          while ((results == null || !results.hasNext) && iter.hasNext) {
            val batch = iter.next()
            val input = transColBatchToOmniVecs(batch)
            val vecBatch = new VecBatch(input, batch.numRows())
            val startInput = System.nanoTime()
            limitOperator.addInput(vecBatch)
            addInputTime += NANOSECONDS.toMillis(System.nanoTime() - startInput)

            val startGetOp = System.nanoTime()
            results = limitOperator.getOutput
            getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)
          }
          if (results == null) {
            false
          } else {
            val startGetOp: Long = System.nanoTime()
            val hasNext = results.hasNext
            getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)
            hasNext
          }
        }

        override def next(): ColumnarBatch = {
          val startGetOp = System.nanoTime()
          val vecBatch = results.next()
          getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)
          val vectors: Seq[OmniColumnVector] = OmniColumnVector.allocateColumns(
            vecBatch.getRowCount, localSchema, false)
          vectors.zipWithIndex.foreach { case (vector, i) =>
            vector.reset()
            vector.setVec(vecBatch.getVectors()(i))
          }
          numOutputRows += vecBatch.getRowCount
          numOutputVecBatches+= 1
          vecBatch.close()
          new ColumnarBatch(vectors.toArray, vecBatch.getRowCount)
        }
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

  def buildCheck(): Unit = {
    child.output.foreach(attr => sparkTypeToOmniType(attr.dataType, attr.metadata))
  }
}

case class ColumnarGlobalLimitExec(limit: Int, child: SparkPlan, offset: Int = 0)
  extends ColumnarBaseLimitExec{

  override def requiredChildDistribution: List[Distribution] = AllTuples :: Nil

  override def nodeName: String = "OmniColumnarGlobalLimit"

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  def buildCheck(): Unit = {
    if (offset > 0) {
      throw new UnsupportedOperationException("ColumnarGlobalLimitExec doesn't support offset greater than 0.")
    }
    child.output.foreach(attr => sparkTypeToOmniType(attr.dataType, attr.metadata))
  }
}

case class ColumnarTakeOrderedAndProjectExec(
                                              limit: Int,
                                              sortOrder: Seq[SortOrder],
                                              projectList: Seq[NamedExpression],
                                              child: SparkPlan,
                                              offset: Int = 0)
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
    "numInputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of input vecBatches"),
    "numOutputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatches"),
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
    if (offset > 0) {
      throw new UnsupportedOperationException("ColumnarTakeOrderedAndProjectExec doesn't support offset greater than 0.")
    }
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
    val childRDD = child.executeColumnar()
    val childRDDPartitions = childRDD.getNumPartitions

    if (childRDDPartitions == 0) {
      new ParallelCollectionRDD(sparkContext, Seq.empty[ColumnarBatch], 1, Map.empty)
    } else {
      val (sourceTypes, ascending, nullFirsts, sortColsExp) = genSortParam(child.output, sortOrder)

      def computeTopN(iter: Iterator[ColumnarBatch], schema: StructType): Iterator[ColumnarBatch] = {
        val startCodegen = System.nanoTime()
        val topNOperatorFactory = new OmniTopNWithExprOperatorFactory(sourceTypes, limit, sortColsExp, ascending, nullFirsts,
          new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
        val topNOperator = topNOperatorFactory.createOperator
        longMetric("omniCodegenTime") += NANOSECONDS.toMillis(System.nanoTime() - startCodegen)
        SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
          topNOperator.close()
        })
        addAllAndGetIterator(topNOperator, iter, schema,
          longMetric("addInputTime"), longMetric("numInputVecBatches"), longMetric("numInputRows"),
          longMetric("getOutputTime"), longMetric("numOutputVecBatches"), longMetric("numOutputRows"),
          longMetric("outputDataSize"))
      }

      val singlePartitionRDD = if (childRDDPartitions == 1) {
        childRDD
      } else {
        val localTopK: RDD[ColumnarBatch] = {
          child.executeColumnar().mapPartitionsWithIndexInternal { (_, iter) =>
            computeTopN(iter, this.child.schema)
          }
        }

        new ShuffledColumnarRDD(
          ColumnarShuffleExchangeExec.prepareShuffleDependency(
            localTopK,
            child.output,
            SinglePartition,
            serializer,
            handleRow = false,
            writeMetrics,
            longMetric("dataSize"),
            longMetric("bytesSpilled"),
            longMetric("numInputRows"),
            longMetric("splitTime"),
            longMetric("spillTime")),
          readMetrics)
      }

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
      singlePartitionRDD.mapPartitions { iter =>
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
  }

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def outputPartitioning: Partitioning = SinglePartition

  override def simpleString(maxFields: Int): String = {
    val orderByString = truncatedString(sortOrder, "[", ",", "]", maxFields)
    val outputString = truncatedString(output, "[", ",", "]", maxFields)

    s"OmniColumnarTakeOrderedAndProject(limit=$limit, orderBy=$orderByString, output=$outputString)"
  }
}