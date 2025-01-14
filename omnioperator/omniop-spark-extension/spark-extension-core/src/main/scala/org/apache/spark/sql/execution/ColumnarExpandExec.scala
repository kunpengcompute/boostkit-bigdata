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
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor.{checkOmniJsonWhiteList, getExprIdMap, rewriteToOmniJsonExpressionLiteral, sparkTypeToOmniType, toOmniAggFunType, toOmniAggInOutJSonExp, toOmniAggInOutType}
import com.huawei.boostkit.spark.util.OmniAdaptorUtil
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.{copyVecBatch, transColBatchToOmniVecs}
import nova.hetu.omniruntime.`type`.DataType
import nova.hetu.omniruntime.constants.FunctionType
import nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_COUNT_ALL
import nova.hetu.omniruntime.operator.OmniOperator
import nova.hetu.omniruntime.operator.config.{OperatorConfig, OverflowConfig, SpillConfig}
import nova.hetu.omniruntime.operator.project.OmniProjectOperatorFactory
import nova.hetu.omniruntime.vector.{LongVec, Vec, VecBatch}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Count, Final, First, Max, Min, Partial, PartialMerge, Sum}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.util.SparkMemoryUtils.addLeakSafeTaskCompletionListener
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.NANOSECONDS
import scala.math.pow

/**
 * Apply all of the GroupExpressions to every input row, hence we will get
 * multiple output rows for an input row.
 *
 * @param projections The group of expressions, all of the group expressions should
 *                    output the same schema specified bye the parameter `output`
 * @param output      The output Schema
 * @param child       Child operator
 */
case class ColumnarExpandExec(
                               projections: Seq[Seq[Expression]],
                               output: Seq[Attribute],
                               child: SparkPlan)
  extends UnaryExecNode {

  override def supportsColumnar: Boolean = true

  override def nodeName: String = "OmniColumnarExpand"

  override lazy val metrics = Map(
    "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
    "omniCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni codegen"),
    "getOutputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni getOutput"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatches"),
  )

  // The GroupExpressions can output data with arbitrary partitioning, so set it
  // as UNKNOWN partitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  @transient
  override lazy val references: AttributeSet =
    AttributeSet(projections.flatten.flatMap(_.references))

  def buildCheck(): Unit = {
    val omniAttrExpsIdMap = getExprIdMap(child.output)
    child.output.foreach(exp => sparkTypeToOmniType(exp.dataType, exp.metadata))
    val omniExpressions: Array[Array[AnyRef]] = projections.map(exps => exps.map(
      exp => {
        val omniExp: AnyRef = rewriteToOmniJsonExpressionLiteral(exp, omniAttrExpsIdMap)
        omniExp
      }
    ).toArray).toArray
    omniExpressions.foreach(exps => checkOmniJsonWhiteList("", exps))
  }

  def matchRollupOptimization(): Boolean = {
    // Expand operator contains "count(distinct)", "rollup", "cube", "grouping sets",
    // it checks whether match "rollup" operations and part "grouping sets" operation.
    // For example, grouping columns a and b, such as rollup(a, b), grouping sets((a, b), (a)).
    if (projections.length == 1){
      return false
    }
    var step = 0
    projections.foreach(projection => {
      projection.last match {
        case literal: Literal =>
          if (literal.value != (pow(2, step) - 1)) {
            return false
          }
        case _ =>
          return false
      }
      step += 1
    })
    true
  }

  def replace(newProjections: Seq[Seq[Expression]] = projections,
              newOutput: Seq[Attribute] = output,
              newChild: SparkPlan = child): ColumnarExpandExec = {
    copy(projections = newProjections, output = newOutput, child = newChild)
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRowsMetric = longMetric("numOutputRows")
    val numOutputVecBatchesMetric = longMetric("numOutputVecBatches")
    val addInputTimeMetric = longMetric("addInputTime")
    val omniCodegenTimeMetric = longMetric("omniCodegenTime")
    val getOutputTimeMetric = longMetric("getOutputTime")

    val omniAttrExpsIdMap = getExprIdMap(child.output)
    val omniInputTypes = child.output.map(exp => sparkTypeToOmniType(exp.dataType, exp.metadata)).toArray
    val omniExpressions = projections.map(exps => exps.map(
      exp => rewriteToOmniJsonExpressionLiteral(exp, omniAttrExpsIdMap)
    ).toArray).toArray

    child.executeColumnar().mapPartitionsWithIndexInternal { (index, iter) =>
      val startCodegen = System.nanoTime()
      var projectOperators = omniExpressions.map(exps => {
        val factory = new OmniProjectOperatorFactory(exps, omniInputTypes, 1,
          new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
        factory.createOperator
      })
      omniCodegenTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startCodegen)
      // close operator
      addLeakSafeTaskCompletionListener[Unit](_ => {
        projectOperators.foreach(operator => operator.close())
      })

      new Iterator[ColumnarBatch] {
        private var results: java.util.Iterator[VecBatch] = _
        private[this] var idx = -1 // -1 mean the initial state
        private[this] var input: ColumnarBatch = _

        def isInputHasNext: Boolean = {
          (-1 < idx && idx < projections.length) || iter.hasNext
        }

        override def hasNext: Boolean = {
          while ((results == null || !results.hasNext) && isInputHasNext) {
            if (idx <= 0) {
              // in the initial (-1) or beginning(0) of a new input row, fetch the next input tuple
              input = iter.next()
              idx = 0
            }
            // last group should not copy input, and let omnioperator free input vec
            val isSlice = idx < projections.length - 1
            val omniInput = transColBatchToOmniVecs(input, isSlice)
            val vecBatch = new VecBatch(omniInput, input.numRows())

            val startInput = System.nanoTime()
            projectOperators(idx).addInput(vecBatch)
            addInputTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startInput)

            val startGetOutput = System.nanoTime()
            results = projectOperators(idx).getOutput
            getOutputTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startGetOutput)

            idx += 1
            if (idx == projections.length && iter.hasNext) {
              idx = 0
            }
          }

          if (results == null) {
            return false
          }
          val startGetOutput = System.nanoTime()
          val hasNext = results.hasNext
          getOutputTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startGetOutput)
          hasNext
        }

        override def next(): ColumnarBatch = {
          val startGetOutput = System.nanoTime()
          val result = results.next()
          getOutputTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startGetOutput)

          val vectors: Seq[OmniColumnVector] = OmniColumnVector.allocateColumns(
            result.getRowCount, schema, false)
          vectors.zipWithIndex.foreach { case (vector, i) =>
            vector.reset()
            vector.setVec(result.getVectors()(i))
          }

          val rowCount = result.getRowCount
          numOutputRowsMetric += rowCount
          numOutputVecBatchesMetric += 1
          result.close()
          new ColumnarBatch(vectors.toArray, rowCount)
        }
      }
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"ColumnarExpandExec operator doesn't support doExecute().")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarExpandExec =
    copy(child = newChild)
}


/**
 * rollup optimization: handle 2~N combinations
 *
 * @param projections The group and aggregation of expressions, all of the group expressions should
 *                    output the same schema specified bye the parameter `output`
 * @param output      The output Schema
 * @param groupingExpressions       The group of expressions
 * @param aggregateExpressions      The aggregation of expressions
 * @param aggregateAttributes       The aggregation of attributes
 * @param child       Child operator
 */
case class ColumnarOptRollupExec(
                               projections: Seq[Seq[Expression]],
                               output: Seq[Attribute],
                               groupingExpressions: Seq[NamedExpression],
                               aggregateExpressions: Seq[AggregateExpression],
                               aggregateAttributes: Seq[Attribute],
                               child: SparkPlan)
  extends UnaryExecNode {

  override def supportsColumnar: Boolean = true

  override def nodeName: String = "OmniColumnarOptRollup"

  override lazy val metrics = Map(
    "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
    "omniCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni codegen"),
    "getOutputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni getOutput"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatches"),
  )

  // The GroupExpressions can output data with arbitrary partitioning, so set it
  // as UNKNOWN partitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  @transient
  override lazy val references: AttributeSet =
    AttributeSet(projections.flatten.flatMap(_.references))

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRowsMetric = longMetric("numInputRows")
    val numOutputRowsMetric = longMetric("numOutputRows")
    val numOutputVecBatchesMetric = longMetric("numOutputVecBatches")
    val addInputTimeMetric = longMetric("addInputTime")
    val omniCodegenTimeMetric = longMetric("omniCodegenTime")
    val getOutputTimeMetric = longMetric("getOutputTime")

    // handle expand logic
    val projectAttrExpsIdMap = getExprIdMap(child.output)
    val omniInputTypes = child.output.map(exp => sparkTypeToOmniType(exp.dataType, exp.metadata)).toArray
    val omniExpressions = projections.map(exps => exps.map(
      exp => rewriteToOmniJsonExpressionLiteral(exp, projectAttrExpsIdMap)
    ).toArray).toArray

    // handle hashagg logic
    val hashaggAttrExpsIdMap = getExprIdMap(child.output)
    val omniGroupByChannel = groupingExpressions.map(
      exp => rewriteToOmniJsonExpressionLiteral(exp, hashaggAttrExpsIdMap)
    ).toArray

    val omniInputRaws = new Array[Boolean](aggregateExpressions.size)
    val omniOutputPartials = new Array[Boolean](aggregateExpressions.size)
    val omniAggFunctionTypes = new Array[FunctionType](aggregateExpressions.size)
    val omniAggOutputTypes = new Array[Array[DataType]](aggregateExpressions.size)
    var omniAggChannels = new Array[Array[String]](aggregateExpressions.size)
    val omniAggChannelsFilter = new Array[String](aggregateExpressions.size)

    var index = 0
    for (exp <- aggregateExpressions) {
      if (exp.filter.isDefined) {
        omniAggChannelsFilter(index) =
          rewriteToOmniJsonExpressionLiteral(exp.filter.get, hashaggAttrExpsIdMap)
      }
      if (exp.mode == PartialMerge) {
        ColumnarHashAggregateExec.AssignOmniInfoWhenPartialMergeStage(exp,
          hashaggAttrExpsIdMap,
          index,
          omniInputRaws,
          omniOutputPartials,
          omniAggFunctionTypes,
          omniAggOutputTypes,
          omniAggChannels)
      } else {
        throw new UnsupportedOperationException(s"Unsupported aggregate mode: ${exp.mode} in ColumnarOptRollupExec")
      }
      index += 1
    }

    omniAggChannels = omniAggChannels.filter(key => key != null)
    val omniSourceTypes = new Array[DataType](child.output.size)
    child.output.zipWithIndex.foreach {
      case (attr, i) =>
        omniSourceTypes(i) = sparkTypeToOmniType(attr.dataType, attr.metadata)
    }

    child.executeColumnar().mapPartitionsWithIndexInternal { (index, iter) =>
      val startCodegen = System.nanoTime()
      var factory : OmniProjectOperatorFactory = null;
      val projectOperators = omniExpressions.map(exps => {
        factory = new OmniProjectOperatorFactory(exps, omniInputTypes, 1,
          new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
        factory.createOperator
      })

      var (hashaggOperator, hashAggregationWithExprOperatorFactory, aggregationWithExprOperatorFactory) = OmniAdaptorUtil.getAggOperator(groupingExpressions,
      omniGroupByChannel,
      omniAggChannels,
      omniAggChannelsFilter,
      omniSourceTypes,
      omniAggFunctionTypes,
      omniAggOutputTypes,
      omniInputRaws,
      omniOutputPartials)

      omniCodegenTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startCodegen)

      val results = new ListBuffer[VecBatch]()
      var hashaggResults: java.util.Iterator[VecBatch] = null

      // close operator
      addLeakSafeTaskCompletionListener[Unit](_ => {
        projectOperators.foreach(operator => operator.close())
        factory.close()
        hashaggOperator.close()
        if (hashAggregationWithExprOperatorFactory != null) {
          hashAggregationWithExprOperatorFactory.close()
        }
        if (aggregationWithExprOperatorFactory != null) {
          aggregationWithExprOperatorFactory.close()
        }
        results.foreach(vecBatch => {
          vecBatch.releaseAllVectors()
          vecBatch.close()
        })
      })

      for (index <- projectOperators.indices) {
        if (index == 0) {
          while (iter.hasNext) {
            val batch = iter.next()
            val input = transColBatchToOmniVecs(batch)
            val rowCount = batch.numRows()
            val vecBatch = new VecBatch(input, rowCount)
            results.append(vecBatch)
            numInputRowsMetric += rowCount

            val vecs = transColBatchToOmniVecs(batch, true)
            val projectInput = new VecBatch(vecs, rowCount)
            var startInput = System.nanoTime()
            projectOperators(index).addInput(projectInput)
            addInputTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startInput)

            val startGetOutput = System.nanoTime()
            val projectResults = projectOperators(index).getOutput
            getOutputTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startGetOutput)

            if (!projectResults.hasNext) {
              throw new RuntimeException("project operator failed!")
            }

            val hashaggInput = projectResults.next()

            startInput = System.nanoTime()
            hashaggOperator.addInput(hashaggInput)
            addInputTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startInput)
          }
        } else {
          val (newHashaggOperator, newHashAggregationWithExprOperatorFactory, newAggregationWithExprOperatorFactory) = OmniAdaptorUtil.getAggOperator(groupingExpressions,
            omniGroupByChannel,
            omniAggChannels,
            omniAggChannelsFilter,
            omniSourceTypes,
            omniAggFunctionTypes,
            omniAggOutputTypes,
            omniInputRaws,
            omniOutputPartials)

          while (results.nonEmpty && hashaggResults.hasNext) {
            val vecBatch = hashaggResults.next()
            results.append(vecBatch)
            val rowCount = vecBatch.getRowCount
            // The vecBatch is the output data of the previous round of combination
            // and the input data of the next round of combination
            numInputRowsMetric += rowCount
            numOutputRowsMetric += rowCount

            val projectInput = copyVecBatch(vecBatch)
            var startInput = System.nanoTime()
            projectOperators(index).addInput(projectInput)
            addInputTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startInput)

            val startGetOutput = System.nanoTime()
            val projectResults = projectOperators(index).getOutput
            getOutputTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startGetOutput)

            if (!projectResults.hasNext) {
              throw new RuntimeException("project operator failed!")
            }

            val hashaggInput = projectResults.next()

            startInput = System.nanoTime()
            newHashaggOperator.addInput(hashaggInput)
            addInputTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startInput)
          }
          // The iterator of the hashagg operator has been iterated.
          if (results.nonEmpty) {
            hashaggOperator.close()
            if (hashAggregationWithExprOperatorFactory != null) {
              hashAggregationWithExprOperatorFactory.close()
            }
            if (aggregationWithExprOperatorFactory != null) {
              aggregationWithExprOperatorFactory.close()
            }
            hashaggOperator = newHashaggOperator
            hashAggregationWithExprOperatorFactory = newHashAggregationWithExprOperatorFactory
            aggregationWithExprOperatorFactory = newAggregationWithExprOperatorFactory
          }
        }
        if (results.nonEmpty) {
          val startGetOutput = System.nanoTime()
          hashaggResults = hashaggOperator.getOutput
          getOutputTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startGetOutput)
        }
      }

      new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = {
          val startGetOutput = System.nanoTime()
          val hasNext = results.nonEmpty || (hashaggResults != null && hashaggResults.hasNext)
          getOutputTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startGetOutput)
          hasNext
        }

        override def next(): ColumnarBatch = {
          var vecBatch: VecBatch = null
          if (results.nonEmpty) {
            vecBatch = results.remove(0)
          } else {
            val startGetOutput = System.nanoTime()
            vecBatch = hashaggResults.next()
            getOutputTimeMetric += NANOSECONDS.toMillis(System.nanoTime() - startGetOutput)
            val rowCount = vecBatch.getRowCount
            numOutputRowsMetric += rowCount
          }

          val vectors: Seq[OmniColumnVector] = OmniColumnVector.allocateColumns(
            vecBatch.getRowCount, schema, false)
          vectors.zipWithIndex.foreach { case (vector, i) =>
            vector.reset()
            vector.setVec(vecBatch.getVectors()(i))
          }

          val rowCount = vecBatch.getRowCount
          numOutputVecBatchesMetric += 1
          vecBatch.close()
          new ColumnarBatch(vectors.toArray, rowCount)
        }
      }
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"ColumnarOptRollupExec operator doesn't support doExecute().")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarOptRollupExec =
    copy(child = newChild)
}
