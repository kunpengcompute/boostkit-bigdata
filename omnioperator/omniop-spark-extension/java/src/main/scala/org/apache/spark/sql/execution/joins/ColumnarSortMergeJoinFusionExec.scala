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

import java.util.concurrent.TimeUnit.NANOSECONDS
import java.util.Optional

import com.huawei.boostkit.spark.ColumnarPluginConfig
import com.huawei.boostkit.spark.Constant.IS_SKIP_VERIFY_EXP
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor
import com.huawei.boostkit.spark.util.OmniAdaptorUtil
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.{getIndexArray, pruneOutput, reorderVecs, transColBatchToOmniVecs}
import nova.hetu.omniruntime.`type`.DataType
import nova.hetu.omniruntime.operator.config.{OperatorConfig, OverflowConfig, SpillConfig}
import nova.hetu.omniruntime.operator.join.{OmniSmjBufferedTableWithExprOperatorFactoryV3, OmniSmjStreamedTableWithExprOperatorFactoryV3}
import nova.hetu.omniruntime.vector.VecBatch
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.util.{MergeIterator, SparkMemoryUtils}
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Performs a sort merge join of two child relations.
 */
case class ColumnarSortMergeJoinFusionExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean = false,
    projectList: Seq[NamedExpression] = Seq.empty)
  extends BaseColumnarSortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right, isSkewJoin, projectList) {

  override def nodeName: String = {
    if (isSkewJoin) "OmniColumnarSortMergeJoinFusion(skew=true)" else "OmniColumnarSortMergeJoinFusion"
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

    val streamedTypes = new Array[DataType](left.output.size)
    left.output.zipWithIndex.foreach { case (attr, i) =>
      streamedTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    val streamedKeyColsExp = leftKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(left.output.map(_.toAttribute)))
    }.toArray
    val streamedOutputChannel = getIndexArray(left.output, projectList)

    val bufferedTypes = new Array[DataType](right.output.size)
    right.output.zipWithIndex.foreach { case (attr, i) =>
      bufferedTypes(i) = OmniExpressionAdaptor.sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    val bufferedKeyColsExp = rightKeys.map { x =>
      OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(x,
        OmniExpressionAdaptor.getExprIdMap(right.output.map(_.toAttribute)))
    }.toArray
    val bufferedOutputChannel: Array[Int] = joinType match {
      case Inner | LeftOuter | FullOuter =>
        getIndexArray(right.output, projectList)
      case LeftExistence(_) =>
        Array[Int]()
      case x =>
        throw new UnsupportedOperationException(s"ColumnSortMergeJoin Join-type[$x] is not supported!")
    }

    val filterString: String = condition match {
      case Some(expr) =>
        OmniExpressionAdaptor.rewriteToOmniJsonExpressionLiteral(expr,
          OmniExpressionAdaptor.getExprIdMap((left.output ++ right.output).map(_.toAttribute)))
      case _ => null
    }

    left.executeColumnar().zipPartitions(right.executeColumnar()) { (streamedIter, bufferedIter) =>
      val filter: Optional[String] = Optional.ofNullable(filterString)
      val startStreamedCodegen = System.nanoTime()
      val lookupJoinType = OmniExpressionAdaptor.toOmniJoinType(joinType)
      val streamedOpFactory = new OmniSmjStreamedTableWithExprOperatorFactoryV3(streamedTypes,
        streamedKeyColsExp, streamedOutputChannel, lookupJoinType, filter,
        new OperatorConfig(SpillConfig.NONE,
          new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))

      val streamedOp = streamedOpFactory.createOperator
      streamedCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startStreamedCodegen)

      val startBufferedCodegen = System.nanoTime()
      val bufferedOpFactory = new OmniSmjBufferedTableWithExprOperatorFactoryV3(bufferedTypes,
        bufferedKeyColsExp, bufferedOutputChannel, streamedOpFactory,
        new OperatorConfig(SpillConfig.NONE,
          new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val bufferedOp = bufferedOpFactory.createOperator
      bufferedCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startBufferedCodegen)

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        bufferedOp.close()
        streamedOp.close()
        bufferedOpFactory.close()
        streamedOpFactory.close()
      })

      while (bufferedIter.hasNext) {
        val cb = bufferedIter.next()
        val vecs = transColBatchToOmniVecs(cb, false)
        val startBuildInput = System.nanoTime()
        bufferedOp.addInput(new VecBatch(vecs, cb.numRows()))
        bufferedAddInputTime += NANOSECONDS.toMillis(System.nanoTime() -startBuildInput)
      }

      while (streamedIter.hasNext) {
        val cb = streamedIter.next()
        val vecs = transColBatchToOmniVecs(cb, false)
        val startBuildInput = System.nanoTime()
        streamedOp.addInput(new VecBatch(vecs, cb.numRows()))
        streamedAddInputTime += NANOSECONDS.toMillis(System.nanoTime() -startBuildInput)
      }

      val prunedStreamOutput = pruneOutput(left.output, projectList)
      val prunedBufferOutput = pruneOutput(right.output, projectList)
      val prunedOutput = prunedStreamOutput ++ prunedBufferOutput
      val resultSchema = this.schema
      val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf
      val enableSortMergeJoinBatchMerge: Boolean = columnarConf.enableSortMergeJoinBatchMerge

      val startGetOutputTime: Long = System.nanoTime()
      val results: java.util.Iterator[VecBatch] = bufferedOp.getOutput
      getOutputTime += NANOSECONDS.toMillis(System.nanoTime() -startGetOutputTime)

      val iterBatch = new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = {
            val startGetOp: Long = System.nanoTime()
            val hasNext = results.hasNext
            getOutputTime += NANOSECONDS.toMillis(System.nanoTime() -startGetOp)
            hasNext
        }

        override def next(): ColumnarBatch = {
            val startGetOp: Long = System.nanoTime()
            val result: VecBatch = results.next()
            getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)

            val resultVecs =result.getVectors
            val vecs = OmniColumnVector.allocateColumns(result.getRowCount, resultSchema, false)
            if (projectList.nonEmpty) {
                reorderVecs(prunedOutput, projectList, resultVecs, vecs)
            } else {
                for (index <- output.indices) {
                    val v = vecs(index)
                    v.reset()
                    v.setVec(resultVecs(index))
                }
            }
            numOutputVecBatchs += 1
            numOutputRows += result.getRowCount
            result.close()
            new ColumnarBatch(vecs.toArray, result.getRowCount)
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
