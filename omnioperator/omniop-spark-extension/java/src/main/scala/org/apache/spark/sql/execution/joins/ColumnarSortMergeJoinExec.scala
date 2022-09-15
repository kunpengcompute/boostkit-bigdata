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

package org.apache.spark.sql.execution.joins

import com.huawei.boostkit.spark.ColumnarPluginConfig

import java.util.concurrent.TimeUnit.NANOSECONDS
import java.util.Optional
import com.huawei.boostkit.spark.Constant.IS_SKIP_VERIFY_EXP
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor.{checkOmniJsonWhiteList, isSimpleColumn, isSimpleColumnForAll}
import com.huawei.boostkit.spark.util.OmniAdaptorUtil
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.transColBatchToOmniVecs
import nova.hetu.omniruntime.`type`.DataType
import nova.hetu.omniruntime.constants.JoinType._
import nova.hetu.omniruntime.operator.config.{OperatorConfig, OverflowConfig, SpillConfig}
import nova.hetu.omniruntime.operator.join.{OmniSmjBufferedTableWithExprOperatorFactory, OmniSmjStreamedTableWithExprOperatorFactory}
import nova.hetu.omniruntime.vector.{BooleanVec, Decimal128Vec, DoubleVec, IntVec, LongVec, VarcharVec, Vec, VecBatch, ShortVec}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.util.{MergeIterator, SparkMemoryUtils}
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Performs a sort merge join of two child relations.
 */
class ColumnarSortMergeJoinExec(
                                 leftKeys: Seq[Expression],
                                 rightKeys: Seq[Expression],
                                 joinType: JoinType,
                                 condition: Option[Expression],
                                 left: SparkPlan,
                                 right: SparkPlan,
                                 isSkewJoin: Boolean = false)
  extends SortMergeJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean) with CodegenSupport {

  override def supportsColumnar: Boolean = true

  override def supportCodegen: Boolean = false

  override def nodeName: String = {
    if (isSkewJoin) "OmniColumnarSortMergeJoin(skew=true)" else "OmniColumnarSortMergeJoin"
  }

  val SMJ_NEED_ADD_STREAM_TBL_DATA = 2
  val SMJ_NEED_ADD_BUFFERED_TBL_DATA = 3
  val SMJ_NO_RESULT = 4
  val SMJ_FETCH_JOIN_DATA = 5

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "streamedAddInputTime" ->
      SQLMetrics.createMetric(sparkContext, "time in omni streamed addInput"),
    "streamedCodegenTime" ->
      SQLMetrics.createMetric(sparkContext, "time in omni streamed codegen"),
    "bufferedAddInputTime" ->
      SQLMetrics.createMetric(sparkContext, "time in omni buffered addInput"),
    "bufferedCodegenTime" ->
      SQLMetrics.createMetric(sparkContext, "time in omni buffered codegen"),
    "getOutputTime" ->
      SQLMetrics.createMetric(sparkContext, "time in omni buffered getOutput"),
    "numOutputVecBatchs" ->
      SQLMetrics.createMetric(sparkContext, "number of output vecBatchs"),
    "numMergedVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of merged vecBatchs"),
    "numStreamVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of streamed vecBatchs"),
    "numBufferVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of buffered vecBatchs")
  )

  def buildCheck(): Unit = {
    joinType match {
      case _: InnerLike | LeftOuter | FullOuter =>
        // SMJ join support InnerLike | LeftOuter | FullOuter
      case _ =>
        throw new UnsupportedOperationException(s"Join-type[${joinType}] is not supported " +
          s"in ${this.nodeName}")
    }

    val streamedTypes = new Array[DataType](left.output.size)
    left.output.zipWithIndex.foreach { case (attr, i) =>
      streamedTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    val streamedKeyColsExp: Array[AnyRef] = leftKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(left.output.map(_.toAttribute)))
    }.toArray

    val bufferedTypes = new Array[DataType](right.output.size)
    right.output.zipWithIndex.foreach { case (attr, i) =>
      bufferedTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    val bufferedKeyColsExp: Array[AnyRef] = rightKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(right.output.map(_.toAttribute)))
    }.toArray

    if (!isSimpleColumnForAll(streamedKeyColsExp.map(expr => expr.toString))) {
      checkOmniJsonWhiteList("", streamedKeyColsExp)
    }

    if (!isSimpleColumnForAll(bufferedKeyColsExp.map(expr => expr.toString))) {
      checkOmniJsonWhiteList("", bufferedKeyColsExp)
    }

    condition match {
      case Some(expr) =>
        val filterExpr: String = OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(expr,
          OmniExpressionAdaptor.getExprIdMap(output.map(_.toAttribute)))
        if (!isSimpleColumn(filterExpr)) {
          checkOmniJsonWhiteList(filterExpr, new Array[AnyRef](0))
        }
      case _ => null
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val numOutputVecBatchs = longMetric("numOutputVecBatchs")
    val numMergedVecBatchs = longMetric("numMergedVecBatchs")
    val streamedAddInputTime = longMetric("streamedAddInputTime")
    val streamedCodegenTime = longMetric("streamedCodegenTime")
    val bufferedAddInputTime = longMetric("bufferedAddInputTime")
    val bufferedCodegenTime = longMetric("bufferedCodegenTime")
    val getOutputTime = longMetric("getOutputTime")
    val streamVecBatchs = longMetric("numStreamVecBatchs")
    val bufferVecBatchs = longMetric("numBufferVecBatchs")

    val omniJoinType : nova.hetu.omniruntime.constants.JoinType = joinType match {
      case _: InnerLike => OMNI_JOIN_TYPE_INNER
      case LeftOuter => OMNI_JOIN_TYPE_LEFT
      case FullOuter => OMNI_JOIN_TYPE_FULL
      case x =>
        throw new UnsupportedOperationException(s"ColumnSortMergeJoin Join-type[$x] is not supported " +
          s"in ${this.nodeName}")
    }

    val streamedTypes = new Array[DataType](left.output.size)
    left.output.zipWithIndex.foreach { case (attr, i) =>
      streamedTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    val streamedKeyColsExp = leftKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(left.output.map(_.toAttribute)))
    }.toArray
    val streamedOutputChannel = left.output.indices.toArray

    val bufferedTypes = new Array[DataType](right.output.size)
    right.output.zipWithIndex.foreach { case (attr, i) =>
      bufferedTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    val bufferedKeyColsExp = rightKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(right.output.map(_.toAttribute)))
    }.toArray
    val bufferedOutputChannel = right.output.indices.toArray

    val filterString: String = condition match {
      case Some(expr) =>
        OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(expr,
          OmniExpressionAdaptor.getExprIdMap(output.map(_.toAttribute)))
      case _ => null
    }

    left.executeColumnar().zipPartitions(right.executeColumnar()) { (streamedIter, bufferedIter) =>
      val filter: Optional[String] = Optional.ofNullable(filterString)
      val startStreamedCodegen = System.nanoTime()
      val streamedOpFactory = new OmniSmjStreamedTableWithExprOperatorFactory(streamedTypes,
        streamedKeyColsExp, streamedOutputChannel, omniJoinType, filter,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val streamedOp = streamedOpFactory.createOperator
      streamedCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startStreamedCodegen)

      val startBufferedCodegen = System.nanoTime()
      val bufferedOpFactory = new OmniSmjBufferedTableWithExprOperatorFactory(bufferedTypes,
        bufferedKeyColsExp, bufferedOutputChannel, streamedOpFactory,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val bufferedOp = bufferedOpFactory.createOperator
      bufferedCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startBufferedCodegen)

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        streamedOp.close()
        bufferedOp.close()
        bufferedOpFactory.close()
        streamedOpFactory.close()
      })

      val resultSchema = this.schema
      val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf
      val enableSortMergeJoinBatchMerge: Boolean = columnarConf.enableSortMergeJoinBatchMerge
      val iterBatch = new Iterator[ColumnarBatch] {

        var isFinished : Boolean = joinType match {
          case _: InnerLike => !streamedIter.hasNext || !bufferedIter.hasNext
          case LeftOuter => !streamedIter.hasNext
          case FullOuter => !(streamedIter.hasNext || bufferedIter.hasNext)
          case x =>
            throw new UnsupportedOperationException(s"ColumnSortMergeJoin Join-type[$x] is not supported!")
        }

        var isStreamedFinished = false
        var isBufferedFinished = false
        var results: java.util.Iterator[VecBatch] = null

        def checkAndClose() : Unit = {
            while(streamedIter.hasNext) {
              streamVecBatchs += 1
              streamedIter.next().close()
            }
            while(bufferedIter.hasNext) {
              bufferVecBatchs += 1
              bufferedIter.next().close()
            }
        }

        override def hasNext: Boolean = {
          if (isFinished) {
            checkAndClose()
            return false
          }
          if (results != null && results.hasNext) {
            return true
          }
          // reset results and find next results
          results = null
          // Add streamed data first
          var inputReturnCode = SMJ_NEED_ADD_STREAM_TBL_DATA
          while (inputReturnCode == SMJ_NEED_ADD_STREAM_TBL_DATA
            || inputReturnCode == SMJ_NEED_ADD_BUFFERED_TBL_DATA) {
            if (inputReturnCode == SMJ_NEED_ADD_STREAM_TBL_DATA) {
              val startBuildStreamedInput = System.nanoTime()
              if (!isStreamedFinished && streamedIter.hasNext) {
                val batch = streamedIter.next()
                streamVecBatchs += 1
                val inputVecBatch = transColBatchToVecBatch(batch)
                inputReturnCode = streamedOp.addInput(inputVecBatch)
              } else {
                inputReturnCode = streamedOp.addInput(createEofVecBatch(streamedTypes))
                isStreamedFinished = true
              }
              streamedAddInputTime +=
                NANOSECONDS.toMillis(System.nanoTime() - startBuildStreamedInput)
            } else {
              val startBuildBufferedInput = System.nanoTime()
              if (!isBufferedFinished && bufferedIter.hasNext) {
                val batch = bufferedIter.next()
                bufferVecBatchs += 1
                val inputVecBatch = transColBatchToVecBatch(batch)
                inputReturnCode = bufferedOp.addInput(inputVecBatch)
              } else {
                inputReturnCode = bufferedOp.addInput(createEofVecBatch(bufferedTypes))
                isBufferedFinished = true
              }
              bufferedAddInputTime +=
                NANOSECONDS.toMillis(System.nanoTime() - startBuildBufferedInput)
            }
          }
          if (inputReturnCode == SMJ_FETCH_JOIN_DATA) {
            val startGetOutputTime = System.nanoTime()
            results = bufferedOp.getOutput
            val hasNext = results.hasNext
            getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOutputTime)
            if (hasNext) {
              return true
            } else {
              isFinished = true
              results = null
              checkAndClose()
              return false
            }
          }

          if (inputReturnCode == SMJ_NO_RESULT) {
            isFinished = true
            results = null
            checkAndClose()
            return false
          }

          throw new UnsupportedOperationException(s"Unknown return code ${inputReturnCode}")
        }

        override def next(): ColumnarBatch = {
          val startGetOutputTime = System.nanoTime()
          val result = results.next()
          getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOutputTime)
          val resultVecs = result.getVectors
          val vecs = OmniColumnVector.allocateColumns(result.getRowCount, resultSchema, false)
          for (index <- output.indices) {
            val v = vecs(index)
            v.reset()
            v.setVec(resultVecs(index))
          }
          numOutputVecBatchs += 1
          numOutputRows += result.getRowCount
          result.close()
          new ColumnarBatch(vecs.toArray, result.getRowCount)
        }

        def createEofVecBatch(types: Array[DataType]): VecBatch = {
          val vecs: Array[Vec] = new Array[Vec](types.length)
          for (i <- types.indices) {
            vecs(i) = types(i).getId match {
              case DataType.DataTypeId.OMNI_INT | DataType.DataTypeId.OMNI_DATE32 =>
                new IntVec(0)
              case DataType.DataTypeId.OMNI_LONG | DataType.DataTypeId.OMNI_DECIMAL64 =>
                new LongVec(0)
              case DataType.DataTypeId.OMNI_DOUBLE =>
                new DoubleVec(0)
              case DataType.DataTypeId.OMNI_BOOLEAN =>
                new BooleanVec(0)
              case DataType.DataTypeId.OMNI_CHAR | DataType.DataTypeId.OMNI_VARCHAR =>
                new VarcharVec(0, 0)
              case DataType.DataTypeId.OMNI_DECIMAL128 =>
                new Decimal128Vec(0)
              case DataType.DataTypeId.OMNI_SHORT =>
                new ShortVec(0)
              case _ =>
                throw new IllegalArgumentException(s"VecType [${types(i).getClass.getSimpleName}]" +
                  s" is not supported in [${getClass.getSimpleName}] yet")
            }
          }
          new VecBatch(vecs, 0)
        }

        def transColBatchToVecBatch(columnarBatch: ColumnarBatch): VecBatch = {
          val input = transColBatchToOmniVecs(columnarBatch)
          new VecBatch(input, columnarBatch.numRows())
        }
      }

      if (enableSortMergeJoinBatchMerge) {
        new MergeIterator(iterBatch, resultSchema, numMergedVecBatchs)
      } else {
        iterBatch
      }
    }
  }
}
