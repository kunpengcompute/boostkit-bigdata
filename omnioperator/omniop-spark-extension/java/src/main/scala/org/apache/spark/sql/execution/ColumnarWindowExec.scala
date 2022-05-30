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
import com.huawei.boostkit.spark.Constant.{IS_ENABLE_JIT, IS_SKIP_VERIFY_EXP}
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor._
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.transColBatchToOmniVecs
import nova.hetu.omniruntime.`type`.DataType
import nova.hetu.omniruntime.constants.FunctionType
import nova.hetu.omniruntime.operator.config.OperatorConfig
import nova.hetu.omniruntime.operator.window.OmniWindowWithExprOperatorFactory
import nova.hetu.omniruntime.vector.VecBatch
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.ColumnarProjection.dealPartitionData
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.util.SparkMemoryUtils
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.execution.window.{WindowExec, WindowExecBase}
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

  def buildCheck(): Unit = {
    val inputColSize = child.outputSet.size
    val sourceTypes = new Array[DataType](inputColSize) // 2,2

    val windowFunType = new Array[FunctionType](windowExpression.size)
    var windowArgKeys = new Array[String](0) // ?
    val windowFunRetType = new Array[DataType](windowExpression.size) // ?
    val omniAttrExpsIdMap = getExprIdMap(child.output)

    var attrMap: Map[String, Int] = Map()
    val inputIter = child.outputSet.toIterator
    var i = 0
    while (inputIter.hasNext) {
      val inputAttr = inputIter.next()
      sourceTypes(i) = sparkTypeToOmniType(inputAttr.dataType, inputAttr.metadata)
      attrMap += (inputAttr.name -> i)
      i += 1
    }

    windowExpression.foreach { x =>
      x.foreach {
        case e@WindowExpression(function, spec) =>
          windowFunRetType(0) = sparkTypeToOmniType(function.dataType)
          function match {
            // AggregateWindowFunction
            case winfunc: WindowFunction =>
              windowFunType(0) = toOmniWindowFunType(winfunc)
              windowArgKeys = winfunc.children.map(
                exp => rewriteToOmniJsonExpressionLiteral(exp, omniAttrExpsIdMap)).toArray
            // AggregateExpression
            case agg@AggregateExpression(aggFunc, _, _, _, _) =>
              windowFunType(0) = toOmniAggFunType(agg)
              windowArgKeys = aggFunc.children.map(
                exp => rewriteToOmniJsonExpressionLiteral(exp, omniAttrExpsIdMap)).toArray
            case _ => throw new RuntimeException(s"Unsupported window function: ${function}")

          }
          if (spec.frameSpecification.isInstanceOf[SpecifiedWindowFrame]) {
            val winFram = spec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
            if (winFram.lower != UnboundedPreceding) {
              throw new RuntimeException(s"Unsupported Specified frame_start: ${winFram.lower}")
            }
            else if (winFram.upper != UnboundedFollowing && winFram.upper != CurrentRow) {
              throw new RuntimeException(s"Unsupported Specified frame_end: ${winFram.upper}")
            }
          }
        case _ =>
      }
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

    val inputColSize = child.outputSet.size
    val sourceTypes = new Array[DataType](inputColSize) // 2,2
    val sortCols = new Array[Int](orderSpec.size) // 0, 1
    val ascendings = new Array[Int](orderSpec.size) // 1
    val nullFirsts = new Array[Int](orderSpec.size) // 0, 0

    val windowFunType = new Array[FunctionType](windowExpression.size)
    val omminPartitionChannels = new Array[Int](partitionSpec.size)
    val preGroupedChannels = new Array[Int](0) // ?
    var windowArgKeys = new Array[String](0) // ?
    var windowArgKeysForSkip = new Array[String](0) // ?
    val windowFunRetType = new Array[DataType](windowExpression.size) // ?
    val omniAttrExpsIdMap = getExprIdMap(child.output)

    var attrMap: Map[String, Int] = Map()
    val inputIter = child.outputSet.toIterator
    var i = 0
    while (inputIter.hasNext) {
      val inputAttr = inputIter.next()
      sourceTypes(i) = sparkTypeToOmniType(inputAttr.dataType, inputAttr.metadata)
      attrMap += (inputAttr.name -> i)
      i += 1
    }
    // partition column parameters

    // sort column parameters
    i = 0
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
        throw new RuntimeException(s"Unsupported sort col not in inputset: ${sortAttr.nodeName}")
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
        throw new RuntimeException(s"output col not in input cols: ${outputAttr.name}")
      }
      i += 1
    }

    //   partitionSpec: Seq[Expression]
    i = 0
    for (partitionAttr <- partitionSpec) {
      if (attrMap.contains(partitionAttr.asInstanceOf[AttributeReference].name)) {
        omminPartitionChannels(i) = attrMap(partitionAttr.asInstanceOf[AttributeReference].name)
      } else {
        throw new RuntimeException(s"output col not in input cols: ${partitionAttr}")
      }
      i += 1
    }

    var windowExpressionWithProject = false
    i = 0
    windowExpression.foreach { x =>
      x.foreach {
        case e@WindowExpression(function, spec) =>
          windowFunRetType(0) = sparkTypeToOmniType(function.dataType)
          function match {
            // AggregateWindowFunction
            case winfunc: WindowFunction =>
              windowFunType(0) = toOmniWindowFunType(winfunc)
              windowArgKeys = winfunc.children.map(
                exp => rewriteToOmniJsonExpressionLiteral(exp, omniAttrExpsIdMap)).toArray
              windowArgKeysForSkip = winfunc.children.map(
                exp => rewriteToOmniExpressionLiteral(exp, omniAttrExpsIdMap)).toArray
            // AggregateExpression
            case agg@AggregateExpression(aggFunc, _, _, _, _) =>
              windowFunType(0) = toOmniAggFunType(agg)
              windowArgKeys = aggFunc.children.map(
                exp => rewriteToOmniJsonExpressionLiteral(exp, omniAttrExpsIdMap)).toArray
              windowArgKeysForSkip = aggFunc.children.map(
                exp => rewriteToOmniExpressionLiteral(exp, omniAttrExpsIdMap)).toArray
            case _ => throw new RuntimeException(s"Unsupported window function: ${function}")

          }
          if (spec.frameSpecification.isInstanceOf[SpecifiedWindowFrame]) {
            var winFram = spec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
            if (winFram.lower != UnboundedPreceding) {
              throw new RuntimeException(s"Unsupported Specified frame_start: ${winFram.lower}")
            }
            else if (winFram.upper != UnboundedFollowing && winFram.upper != CurrentRow) {
              throw new RuntimeException(s"Unsupported Specified frame_end: ${winFram.upper}")
            }
          }
        case _ =>
          windowExpressionWithProject = true
      }
    }
    val skipColumns = windowArgKeysForSkip.count(x => !x.startsWith("#"))


    val winExpressions: Seq[Expression] = windowFrameExpressionFactoryPairs.flatMap(_._1)

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
        nullFirsts, 0, 10000, windowArgKeys, windowFunRetType, new OperatorConfig(IS_ENABLE_JIT, IS_SKIP_VERIFY_EXP))
      val windowOperator = windowOperatorFactory.createOperator
      omniCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startCodegen)

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

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        windowOperator.close()
      })

      var windowResultSchema = this.schema
      val skipWindowRstExpVecCnt = skipColumns
      if (windowExpressionWithProjectConstant) {
        val omnifinalOutSchema = child.output ++ winExpToReferences.map(_.toAttribute)
        windowResultSchema = StructType.fromAttributes(omnifinalOutSchema)
      }
      val sourceSize = sourceTypes.length

      var omniWindowResultIter = new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = {
          val startGetOp: Long = System.nanoTime()
          var hasNext = results.hasNext
          getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)
          hasNext
        }

        override def next(): ColumnarBatch = {
          val startGetOp = System.nanoTime()
          val vecBatch = results.next()
          getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)
          val vectors: Seq[OmniColumnVector] = OmniColumnVector.allocateColumns(
            vecBatch.getRowCount, windowResultSchema, false)
          vectors.zipWithIndex.foreach { case (vector, i) =>
            vector.reset()
            if (i <= sourceSize - 1) {
              vector.setVec(vecBatch.getVectors()(i))
            } else {
              vector.setVec(vecBatch.getVectors()(i + skipWindowRstExpVecCnt))
            }
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