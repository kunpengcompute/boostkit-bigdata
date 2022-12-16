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
import nova.hetu.omniruntime.constants.{FunctionType, OmniWindowFrameBoundType, OmniWindowFrameType}
import nova.hetu.omniruntime.operator.config.{OperatorConfig, OverflowConfig, SpillConfig}
import nova.hetu.omniruntime.operator.window.OmniWindowWithExprOperatorFactory
import nova.hetu.omniruntime.vector.VecBatch
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.ColumnarProjection.dealPartitionData
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.util.SparkMemoryUtils
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.execution.window.WindowExecBase
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

case class ColumnarWindowExec(windowExpression: Seq[NamedExpression],
                              partitionSpec: Seq[Expression],
                              orderSpec: Seq[SortOrder], child: SparkPlan)
  extends WindowExecBase {

  override def nodeName: String = "OmniColumnarWindow"

  override def supportsColumnar: Boolean = true

  override lazy val metrics = Map(
    "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
    "numInputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of input vecBatchs"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "omniCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni codegen"),
    "getOutputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni getOutput"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatchs"))

  override def output: Seq[Attribute] =
    child.output ++ windowExpression.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      // Only show warning when the number of bytes is larger than 100 MiB?
      logWarning("No Partition Defined for Window operation! Moving all data to a single "
        + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else ClusteredDistribution(partitionSpec) :: Nil
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  def getWindowFrameParam(frame: SpecifiedWindowFrame): (OmniWindowFrameType,
    OmniWindowFrameBoundType, OmniWindowFrameBoundType, Int, Int) = {
    var windowFrameStartChannel = -1
    var windowFrameEndChannel = -1
    val windowFrameType = frame.frameType match {
      case RangeFrame =>
        OmniWindowFrameType.OMNI_FRAME_TYPE_RANGE
      case RowFrame =>
        OmniWindowFrameType.OMNI_FRAME_TYPE_ROWS
    }

    val windowFrameStartType = frame.lower match {
      case UnboundedPreceding =>
        OmniWindowFrameBoundType.OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING
      case UnboundedFollowing =>
        OmniWindowFrameBoundType.OMNI_FRAME_BOUND_UNBOUNDED_FOLLOWING
      case CurrentRow =>
        OmniWindowFrameBoundType.OMNI_FRAME_BOUND_CURRENT_ROW
      case literal: Literal =>
        windowFrameStartChannel = literal.value.toString.toInt
        OmniWindowFrameBoundType.OMNI_FRAME_BOUND_PRECEDING
    }

    val windowFrameEndType = frame.upper match {
      case UnboundedPreceding =>
        OmniWindowFrameBoundType.OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING
      case UnboundedFollowing =>
        OmniWindowFrameBoundType.OMNI_FRAME_BOUND_UNBOUNDED_FOLLOWING
      case CurrentRow =>
        OmniWindowFrameBoundType.OMNI_FRAME_BOUND_CURRENT_ROW
      case literal: Literal =>
        windowFrameEndChannel = literal.value.toString.toInt
        OmniWindowFrameBoundType.OMNI_FRAME_BOUND_FOLLOWING
    }
    (windowFrameType, windowFrameStartType, windowFrameEndType,
      windowFrameStartChannel, windowFrameEndChannel)
  }

  def buildCheck(): Unit = {
    val sourceTypes = new Array[DataType](child.output.size)
    val winExpressions: Seq[Expression] = windowFrameExpressionFactoryPairs.flatMap(_._1)
    val windowFunType = new Array[FunctionType](winExpressions.size)
    var windowArgKeys = new Array[AnyRef](winExpressions.size)
    val windowFunRetType = new Array[DataType](winExpressions.size)
    val omniAttrExpsIdMap = getExprIdMap(child.output)
    val windowFrameTypes = new Array[OmniWindowFrameType](winExpressions.size)
    val windowFrameStartTypes = new Array[OmniWindowFrameBoundType](winExpressions.size)
    val winddowFrameStartChannels = new Array[Int](winExpressions.size)
    val windowFrameEndTypes = new Array[OmniWindowFrameBoundType](winExpressions.size)
    val winddowFrameEndChannels = new Array[Int](winExpressions.size)
    var attrMap: Map[String, Int] = Map()
    child.output.zipWithIndex.foreach {
      case (inputIter, i) =>
        sourceTypes(i) = sparkTypeToOmniType(inputIter.dataType, inputIter.metadata)
        attrMap += (inputIter.name -> i)
    }

    var windowExpressionWithProject = false
    winExpressions.zipWithIndex.foreach { case (x, index) =>
      x.foreach {
        case e@WindowExpression(function, spec) =>
          if (spec.frameSpecification.isInstanceOf[SpecifiedWindowFrame]) {
            val winFram = spec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
            if (winFram.lower != UnboundedPreceding && winFram.lower != CurrentRow) {
              throw new UnsupportedOperationException(s"Unsupported Specified frame_start: ${winFram.lower}")
            }
            if (winFram.upper != UnboundedFollowing && winFram.upper != CurrentRow) {
              throw new UnsupportedOperationException(s"Unsupported Specified frame_end: ${winFram.upper}")
            }
          }
          windowFunRetType(index) = sparkTypeToOmniType(function.dataType)
          val frame = spec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
          val winFrameParam = getWindowFrameParam(frame)
          windowFrameTypes(index) = winFrameParam._1
          windowFrameStartTypes(index) = winFrameParam._2
          windowFrameEndTypes(index) = winFrameParam._3
          winddowFrameStartChannels(index) = winFrameParam._4
          winddowFrameEndChannels(index) = winFrameParam._5
          function match {
            // AggregateWindowFunction
            case winfunc: WindowFunction =>
              windowFunType(index) = toOmniWindowFunType(winfunc)
              windowArgKeys = winfunc.children.map(
                exp => rewriteToOmniJsonExpressionLiteral(exp, omniAttrExpsIdMap)).toArray
            // AggregateExpression
            case agg@AggregateExpression(aggFunc, _, _, _, _) =>
              windowFunType(index) = toOmniAggFunType(agg)
              windowArgKeys = aggFunc.children.map(
                exp => {
                  rewriteToOmniJsonExpressionLiteral(exp, omniAttrExpsIdMap)
                }).toArray
            case _ => throw new UnsupportedOperationException(s"Unsupported window function: ${function}")
          }
        case _ =>
          windowExpressionWithProject = true
      }
    }

    val winExpToReferences = winExpressions.zipWithIndex.map { case (e, i) =>
      // Results of window expressions will be on the right side of child's output
      AttributeReference(String.valueOf(child.output.size + i), e.dataType, e.nullable)().toAttribute
    }
    val winExpToReferencesMap = winExpressions.zip(winExpToReferences).toMap
    val patchedWindowExpression = windowExpression.map(_.transform(winExpToReferencesMap))
    if (windowExpressionWithProject) {
      val finalOut = child.output ++ winExpToReferences
      val projectInputTypes = finalOut.map(
        exp => sparkTypeToOmniType(exp.dataType, exp.metadata)).toArray
      val projectExpressions: Array[AnyRef] = (child.output ++ patchedWindowExpression).map(
        exp => rewriteToOmniJsonExpressionLiteral(exp, getExprIdMap(finalOut))).toArray
      if (!isSimpleColumnForAll(projectExpressions.map(expr => expr.toString))) {
        checkOmniJsonWhiteList("", projectExpressions)
      }
    }
    if (!isSimpleColumnForAll(windowArgKeys.map(key => key.toString))) {
      checkOmniJsonWhiteList("", windowArgKeys)
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val addInputTime = longMetric("addInputTime")
    val numInputRows = longMetric("numInputRows")
    val numInputVecBatchs = longMetric("numInputVecBatchs")
    val omniCodegenTime = longMetric("omniCodegenTime")
    val numOutputRows = longMetric("numOutputRows")
    val numOutputVecBatchs = longMetric("numOutputVecBatchs")
    val getOutputTime = longMetric("getOutputTime")

    val sourceTypes = new Array[DataType](child.output.size)
    val sortCols = new Array[Int](orderSpec.size)
    val ascendings = new Array[Int](orderSpec.size)
    val nullFirsts = new Array[Int](orderSpec.size)
    val winExpressions: Seq[Expression] = windowFrameExpressionFactoryPairs.flatMap(_._1)
    val windowFunType = new Array[FunctionType](winExpressions.size)
    val omminPartitionChannels = new Array[Int](partitionSpec.size)
    val preGroupedChannels = new Array[Int](winExpressions.size)
    var windowArgKeys = new Array[String](winExpressions.size)
    val windowFunRetType = new Array[DataType](winExpressions.size)
    val omniAttrExpsIdMap = getExprIdMap(child.output)
    val windowFrameTypes = new Array[OmniWindowFrameType](winExpressions.size)
    val windowFrameStartTypes = new Array[OmniWindowFrameBoundType](winExpressions.size)
    val windowFrameStartChannels = new Array[Int](winExpressions.size)
    val windowFrameEndTypes = new Array[OmniWindowFrameBoundType](winExpressions.size)
    val windowFrameEndChannels = new Array[Int](winExpressions.size)

    var attrMap: Map[String, Int] = Map()
    child.output.zipWithIndex.foreach {
      case (inputIter, i) =>
        sourceTypes(i) = sparkTypeToOmniType(inputIter.dataType, inputIter.metadata)
        attrMap += (inputIter.name -> i)
    }
    // partition column parameters

    // sort column parameters
    var i = 0
    for (sortAttr <- orderSpec) {
      if (attrMap.contains(sortAttr.child.asInstanceOf[AttributeReference].name)) {
        sortCols(i) = attrMap(sortAttr.child.asInstanceOf[AttributeReference].name)
        ascendings(i) = sortAttr.isAscending match {
          case true => 1
          case _ => 0
        }
        nullFirsts(i) = sortAttr.nullOrdering.sql match {
          case "NULLS LAST" => 0
          case _ => 1
        }
      } else {
        throw new UnsupportedOperationException(s"Unsupported sort col not in inputset: ${sortAttr.nodeName}")
      }
      i += 1
    }

    i = 0
    // only window column no need to as output
    val outputCols = new Array[Int](child.output.size) // 0, 1
    for (outputAttr <- child.output) {
      if (attrMap.contains(outputAttr.name)) {
        outputCols(i) = attrMap.get(outputAttr.name).get
      } else {
        throw new UnsupportedOperationException(s"output col not in input cols: ${outputAttr.name}")
      }
      i += 1
    }

    //   partitionSpec: Seq[Expression]
    i = 0
    for (partitionAttr <- partitionSpec) {
      if (attrMap.contains(partitionAttr.asInstanceOf[AttributeReference].name)) {
        omminPartitionChannels(i) = attrMap(partitionAttr.asInstanceOf[AttributeReference].name)
      } else {
        throw new UnsupportedOperationException(s"output col not in input cols: ${partitionAttr}")
      }
      i += 1
    }

    var windowExpressionWithProject = false
    i = 0
    winExpressions.zipWithIndex.foreach { case (x, index) =>
      x.foreach {
        case e@WindowExpression(function, spec) =>
          if (spec.frameSpecification.isInstanceOf[SpecifiedWindowFrame]) {
            val winFram = spec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
            if (winFram.lower != UnboundedPreceding && winFram.lower != CurrentRow) {
              throw new UnsupportedOperationException(s"Unsupported Specified frame_start: ${winFram.lower}")
            }
            if (winFram.upper != UnboundedFollowing && winFram.upper != CurrentRow) {
              throw new UnsupportedOperationException(s"Unsupported Specified frame_end: ${winFram.upper}")
            }
          }
          windowFunRetType(index) = sparkTypeToOmniType(function.dataType)
          val frame = spec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
          val winFrameParam = getWindowFrameParam(frame)
          windowFrameTypes(index) = winFrameParam._1
          windowFrameStartTypes(index) = winFrameParam._2
          windowFrameEndTypes(index) = winFrameParam._3
          windowFrameStartChannels(index) = winFrameParam._4
          windowFrameEndChannels(index) = winFrameParam._5
          function match {
            // AggregateWindowFunction
            case winfunc: WindowFunction =>
              windowFunType(index) = toOmniWindowFunType(winfunc)
              windowArgKeys(index) = null
            // AggregateExpression
            case agg@AggregateExpression(aggFunc, _, _, _, _) =>
              windowFunType(index) = toOmniAggFunType(agg)
              windowArgKeys(index) = rewriteToOmniJsonExpressionLiteral(aggFunc.children.head,
                omniAttrExpsIdMap)
            case _ => throw new UnsupportedOperationException(s"Unsupported window function: ${function}")
          }
        case _ =>
          windowExpressionWithProject = true
      }
    }
    windowArgKeys = windowArgKeys.filter(key => key != null)

    val winExpToReferences = winExpressions.zipWithIndex.map { case (e, i) =>
      // Results of window expressions will be on the right side of child's output
      AttributeReference(String.valueOf(child.output.size + i), e.dataType, e.nullable)().toAttribute
    }
    val winExpToReferencesMap = winExpressions.zip(winExpToReferences).toMap
    val patchedWindowExpression = windowExpression.map(_.transform(winExpToReferencesMap))

    val windowExpressionWithProjectConstant = windowExpressionWithProject
    child.executeColumnar().mapPartitionsWithIndexInternal { (index, iter) =>
      val startCodegen = System.nanoTime()
      val windowOperatorFactory = new OmniWindowWithExprOperatorFactory(sourceTypes, outputCols,
        windowFunType, omminPartitionChannels, preGroupedChannels, sortCols, ascendings,
        nullFirsts, 0, 10000, windowArgKeys, windowFunRetType, windowFrameTypes, windowFrameStartTypes,
        windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels,
        new OperatorConfig(SpillConfig.NONE, new OverflowConfig(OmniAdaptorUtil.overflowConf()), IS_SKIP_VERIFY_EXP))
      val windowOperator = windowOperatorFactory.createOperator
      omniCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startCodegen)

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        windowOperator.close()
      })

      while (iter.hasNext) {
        val batch = iter.next()
        val input = transColBatchToOmniVecs(batch)
        val vecBatch = new VecBatch(input, batch.numRows())
        val startInput = System.nanoTime()
        windowOperator.addInput(vecBatch)
        addInputTime += NANOSECONDS.toMillis(System.nanoTime() - startInput)
        numInputVecBatchs += 1
        numInputRows += batch.numRows()
      }

      val startGetOp = System.nanoTime()
      val results = windowOperator.getOutput
      getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)

      var windowResultSchema = this.schema
      if (windowExpressionWithProjectConstant) {
        val omnifinalOutSchema = child.output ++ winExpToReferences.map(_.toAttribute)
        windowResultSchema = StructType.fromAttributes(omnifinalOutSchema)
      }
      val outputColSize = outputCols.length
      val omniWindowResultIter = new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = {
          val startGetOp: Long = System.nanoTime()
          val hasNext = results.hasNext
          getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)
          hasNext
        }

        override def next(): ColumnarBatch = {
          val startGetOp = System.nanoTime()
          val vecBatch = results.next()
          getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)
          val vectors: Seq[OmniColumnVector] = OmniColumnVector.allocateColumns(
            vecBatch.getRowCount, windowResultSchema, false)
          val offset = vecBatch.getVectors.length - vectors.size
          vectors.zipWithIndex.foreach { case (vector, i) =>
            vector.reset()
            if (i <= outputColSize - 1) {
              vector.setVec(vecBatch.getVectors()(i))
            } else {
              vector.setVec(vecBatch.getVectors()(i + offset))
            }
          }
          // release skip columnns memory
          for (i <- outputColSize until outputColSize + offset) {
            vecBatch.getVectors()(i).close()
          }
          numOutputRows += vecBatch.getRowCount
          numOutputVecBatchs += 1

          vecBatch.close()
          new ColumnarBatch(vectors.toArray, vecBatch.getRowCount)
        }
      }
      if (windowExpressionWithProjectConstant) {
        val finalOut = child.output ++ winExpToReferences
        val projectInputTypes = finalOut.map(
          exp => sparkTypeToOmniType(exp.dataType, exp.metadata)).toArray
        val projectExpressions = (child.output ++ patchedWindowExpression).map(
          exp => rewriteToOmniJsonExpressionLiteral(exp, getExprIdMap(finalOut))).toArray
        dealPartitionData(null, null, addInputTime, omniCodegenTime,
          getOutputTime, projectInputTypes, projectExpressions, omniWindowResultIter, this.schema)
      } else {
        omniWindowResultIter
      }
    }
  }
}