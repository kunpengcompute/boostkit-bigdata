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
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.transColBatchToOmniVecs
import nova.hetu.omniruntime.`type`.DataType
import nova.hetu.omniruntime.constants.FunctionType
import nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_COUNT_ALL
import nova.hetu.omniruntime.operator.aggregator.OmniHashAggregationWithExprOperatorFactory
import nova.hetu.omniruntime.operator.config.{OperatorConfig, OverflowConfig, SpillConfig}
import nova.hetu.omniruntime.vector.VecBatch
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution.ColumnarProjection.dealPartitionData
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.util.SparkMemoryUtils
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Hash-based aggregate operator that can also fallback to sorting when data exceeds memory size.
 */
case class ColumnarHashAggregateExec(
                                      requiredChildDistributionExpressions: Option[Seq[Expression]],
                                      groupingExpressions: Seq[NamedExpression],
                                      aggregateExpressions: Seq[AggregateExpression],
                                      aggregateAttributes: Seq[Attribute],
                                      initialInputBufferOffset: Int,
                                      resultExpressions: Seq[NamedExpression],
                                      child: SparkPlan)
  extends BaseAggregateExec
    with AliasAwareOutputPartitioning {

  override lazy val metrics = Map(
    "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numInputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of input vecBatchs"),
    "omniCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni codegen"),
    "getOutputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni getOutput"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatchs"))


  override def supportsColumnar: Boolean = true

  override def nodeName: String = "OmniColumnarHashAggregate"

  def buildCheck(): Unit = {
    val attrExpsIdMap = getExprIdMap(child.output)
    val omniGroupByChanel: Array[AnyRef] = groupingExpressions.map(
      exp => rewriteToOmniJsonExpressionLiteral(exp, attrExpsIdMap)).toArray

    var omniInputRaw = false
    var omniOutputPartial = false
    val omniAggTypes = new Array[DataType](aggregateExpressions.size)
    val omniAggFunctionTypes = new Array[FunctionType](aggregateExpressions.size)
    val omniAggOutputTypes = new Array[DataType](aggregateExpressions.size)
    var omniAggChannels = new Array[AnyRef](aggregateExpressions.size)
    var index = 0
    for (exp <- aggregateExpressions) {
      if (exp.filter.isDefined) {
        throw new UnsupportedOperationException("Unsupported filter in AggregateExpression")
      }
      if (exp.isDistinct) {
        throw new UnsupportedOperationException(s"Unsupported aggregate expression with distinct flag")
      }
      if (exp.mode == Final) {
        exp.aggregateFunction match {
          case Sum(_) | Min(_) | Max(_) | Count(_) =>
            val aggExp = exp.aggregateFunction.inputAggBufferAttributes.head
            omniAggTypes(index) = sparkTypeToOmniType(aggExp.dataType, aggExp.metadata)
            omniAggFunctionTypes(index) = toOmniAggFunType(exp, true, true)
            omniAggOutputTypes(index) =
              sparkTypeToOmniType(exp.aggregateFunction.dataType)
            omniAggChannels(index) =
              rewriteToOmniJsonExpressionLiteral(aggExp, attrExpsIdMap)
          case _ => throw new UnsupportedOperationException(s"Unsupported aggregate aggregateFunction: ${exp}")
        }
      } else if (exp.mode == Partial) {
        omniInputRaw = true
        omniOutputPartial = true
        exp.aggregateFunction match {
          case Sum(_) | Min(_) | Max(_) | Count(_) =>
            val aggExp = exp.aggregateFunction.children.head
            omniAggTypes(index) = sparkTypeToOmniType(aggExp.dataType)
            omniAggFunctionTypes(index) = toOmniAggFunType(exp, true)
            omniAggOutputTypes(index) =
              sparkTypeToOmniType(exp.aggregateFunction.dataType)
            omniAggChannels(index) =
              rewriteToOmniJsonExpressionLiteral(aggExp, attrExpsIdMap)
            if (omniAggFunctionTypes(index) == OMNI_AGGREGATION_TYPE_COUNT_ALL) {
              omniAggChannels(index) = null
            }
          case _ => throw new UnsupportedOperationException(s"Unsupported aggregate aggregateFunction: $exp")
        }
      } else {
        throw new UnsupportedOperationException(s"Unsupported aggregate mode: $exp.mode")
      }
      index += 1
    }
    omniAggChannels = omniAggChannels.filter(key => key != null)
    val omniSourceTypes = new Array[DataType](child.outputSet.size)
    val inputIter = child.outputSet.toIterator
    var i = 0
    while (inputIter.hasNext) {
      val inputAttr = inputIter.next()
      omniSourceTypes(i) = sparkTypeToOmniType(inputAttr.dataType, inputAttr.metadata)
      i += 1
    }

    checkOmniJsonWhiteList("", omniAggChannels)
    checkOmniJsonWhiteList("", omniGroupByChanel)

    // check for final project
    if (!omniOutputPartial) {
      val finalOut = groupingExpressions.map(_.toAttribute) ++ aggregateAttributes
      val projectInputTypes = finalOut.map(
        exp => sparkTypeToOmniType(exp.dataType, exp.metadata)).toArray
      val projectExpressions: Array[AnyRef] = resultExpressions.map(
        exp => rewriteToOmniJsonExpressionLiteral(exp, getExprIdMap(finalOut))).toArray
      checkOmniJsonWhiteList("", projectExpressions)
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val addInputTime = longMetric("addInputTime")
    val numInputRows = longMetric("numInputRows")
    val numInputVecBatchs = longMetric("numInputVecBatchs")
    val omniCodegenTime = longMetric("omniCodegenTime")
    val getOutputTime = longMetric("getOutputTime")
    val numOutputRows = longMetric("numOutputRows")
    val numOutputVecBatchs = longMetric("numOutputVecBatchs")

    val attrExpsIdMap = getExprIdMap(child.output)
    val omniGroupByChanel = groupingExpressions.map(
      exp => rewriteToOmniJsonExpressionLiteral(exp, attrExpsIdMap)).toArray

    var omniInputRaw = false
    var omniOutputPartial = false
    val omniAggTypes = new Array[DataType](aggregateExpressions.size)
    val omniAggFunctionTypes = new Array[FunctionType](aggregateExpressions.size)
    val omniAggOutputTypes = new Array[DataType](aggregateExpressions.size)
    var omniAggChannels = new Array[String](aggregateExpressions.size)
    var index = 0
    for (exp <- aggregateExpressions) {
      if (exp.filter.isDefined) {
        throw new UnsupportedOperationException("Unsupported filter in AggregateExpression")
      }
      if (exp.isDistinct) {
        throw new UnsupportedOperationException("Unsupported aggregate expression with distinct flag")
      }
      if (exp.mode == Final) {
        exp.aggregateFunction match {
          case Sum(_) | Min(_) | Max(_) | Count(_) =>
            val aggExp = exp.aggregateFunction.inputAggBufferAttributes.head
            omniAggTypes(index) = sparkTypeToOmniType(aggExp.dataType, aggExp.metadata)
            omniAggFunctionTypes(index) = toOmniAggFunType(exp, true, true)
            omniAggOutputTypes(index) =
              sparkTypeToOmniType(exp.aggregateFunction.dataType)
            omniAggChannels(index) =
              rewriteToOmniJsonExpressionLiteral(aggExp, attrExpsIdMap)
          case _ => throw new UnsupportedOperationException(s"Unsupported aggregate aggregateFunction: ${exp}")
        }
      } else if (exp.mode == Partial) {
        omniInputRaw = true
        omniOutputPartial = true
        exp.aggregateFunction match {
          case Sum(_) | Min(_) | Max(_) | Count(_) =>
            val aggExp = exp.aggregateFunction.children.head
            omniAggTypes(index) = sparkTypeToOmniType(aggExp.dataType)
            omniAggFunctionTypes(index) = toOmniAggFunType(exp, true)
            omniAggOutputTypes(index) =
              sparkTypeToOmniType(exp.aggregateFunction.dataType)
            omniAggChannels(index) =
              rewriteToOmniJsonExpressionLiteral(aggExp, attrExpsIdMap)
            if (omniAggFunctionTypes(index) == OMNI_AGGREGATION_TYPE_COUNT_ALL) {
              omniAggChannels(index) = null
            }
          case _ => throw new UnsupportedOperationException(s"Unsupported aggregate aggregateFunction: ${exp}")
        }
      } else {
        throw new UnsupportedOperationException(s"Unsupported aggregate mode: ${exp.mode}")
      }
      index += 1
    }

    omniAggChannels = omniAggChannels.filter(key => key != null)
    val omniSourceTypes = new Array[DataType](child.outputSet.size)
    val inputIter = child.outputSet.toIterator
    var i = 0
    while (inputIter.hasNext) {
      val inputAttr = inputIter.next()
      omniSourceTypes(i) = sparkTypeToOmniType(inputAttr.dataType, inputAttr.metadata)
      i += 1
    }

    child.executeColumnar().mapPartitionsWithIndex { (index, iter) =>
      val startCodegen = System.nanoTime()
      val factory = new OmniHashAggregationWithExprOperatorFactory(
        omniGroupByChanel,
        omniAggChannels,
        omniSourceTypes,
        omniAggFunctionTypes,
        omniAggOutputTypes,
        omniInputRaw,
        omniOutputPartial,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val operator = factory.createOperator
      omniCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startCodegen)

      while (iter.hasNext) {
        val batch = iter.next()
        val input = transColBatchToOmniVecs(batch)
        val startInput = System.nanoTime()
        val vecBatch = new VecBatch(input, batch.numRows())
        operator.addInput(vecBatch)
        addInputTime += NANOSECONDS.toMillis(System.nanoTime() - startInput)
        numInputVecBatchs += 1
        numInputRows += batch.numRows()
      }
      val startGetOp = System.nanoTime()
      val opOutput = operator.getOutput
      getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        operator.close()
      })

      var localSchema = this.schema
      if (!omniOutputPartial) {
          val omnifinalOutSchama = groupingExpressions.map(_.toAttribute) ++ aggregateAttributes
          localSchema = StructType.fromAttributes(omnifinalOutSchama)
      }

      val hashAggIter = new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = {
          val startGetOp: Long = System.nanoTime()
          val hasNext = opOutput.hasNext
          getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)
          hasNext
        }

        override def next(): ColumnarBatch = {
          val startGetOp = System.nanoTime()
          val vecBatch = opOutput.next()
          getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)
          val vectors: Seq[OmniColumnVector] = OmniColumnVector.allocateColumns(
            vecBatch.getRowCount, localSchema, false)
          vectors.zipWithIndex.foreach { case (vector, i) =>
            vector.reset()
            vector.setVec(vecBatch.getVectors()(i))
          }

          numOutputRows += vecBatch.getRowCount
          numOutputVecBatchs += 1

          vecBatch.close()
          new ColumnarBatch(vectors.toArray, vecBatch.getRowCount)
        }
      }
      if (!omniOutputPartial) {
        val finalOut = groupingExpressions.map(_.toAttribute) ++ aggregateAttributes
        val projectInputTypes = finalOut.map(
          exp => sparkTypeToOmniType(exp.dataType, exp.metadata)).toArray
        val projectExpressions = resultExpressions.map(
          exp => rewriteToOmniJsonExpressionLiteral(exp, getExprIdMap(finalOut))).toArray

        dealPartitionData(null, null, addInputTime, omniCodegenTime,
          getOutputTime, projectInputTypes, projectExpressions, hashAggIter, this.schema)
      } else {
         hashAggIter
      }
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException("This operator doesn't support doExecute().")
  }
}

object ColumnarHashAggregateExec {
  def supportsAggregate(aggregateBufferAttributes: Seq[Attribute]): Boolean = {
    val aggregationBufferSchema = StructType.fromAttributes(aggregateBufferAttributes)
    UnsafeFixedWidthAggregationMap.supportsAggregationBufferSchema(aggregationBufferSchema)
  }
}
