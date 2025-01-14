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

import com.huawei.boostkit.spark.ColumnarPluginConfig

import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit.NANOSECONDS
import com.huawei.boostkit.spark.Constant.IS_SKIP_VERIFY_EXP
import com.huawei.boostkit.spark.expression.OmniExpressionAdaptor._
import com.huawei.boostkit.spark.util.OmniAdaptorUtil
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.transColBatchToOmniVecs
import nova.hetu.omniruntime.`type`.DataType
import nova.hetu.omniruntime.constants.FunctionType
import nova.hetu.omniruntime.constants.FunctionType.OMNI_AGGREGATION_TYPE_COUNT_ALL
import nova.hetu.omniruntime.operator.aggregator.OmniHashAggregationWithExprOperatorFactory
import nova.hetu.omniruntime.operator.config.{OperatorConfig, OverflowConfig, SparkSpillConfig}
import nova.hetu.omniruntime.vector.VecBatch
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.execution.ColumnarProjection.dealPartitionData
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.util.SparkMemoryUtils
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.ShimUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import scala.util.control.Breaks.{break, breakable}

/**
 * Hash-based aggregate operator that can also fallback to sorting when data exceeds memory size.
 */
case class ColumnarHashAggregateExec(
                                      requiredChildDistributionExpressions: Option[Seq[Expression]],
                                      isStreaming: Boolean,
                                      numShufflePartitions: Option[Int],
                                      groupingExpressions: Seq[NamedExpression],
                                      aggregateExpressions: Seq[AggregateExpression],
                                      aggregateAttributes: Seq[Attribute],
                                      initialInputBufferOffset: Int,
                                      resultExpressions: Seq[NamedExpression],
                                      child: SparkPlan)
  extends AggregateCodegenSupportShim {

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarHashAggregateExec =
    copy(child = newChild)

  override lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Input", child.output ++ child.output.map(_.dataType))}
       |${ExplainUtils.generateFieldString("Keys", groupingExpressions ++ groupingExpressions.map(_.dataType))}
       |${ExplainUtils.generateFieldString("Functions", aggregateExpressions ++ aggregateExpressions.map(_.dataType))}
       |${ExplainUtils.generateFieldString("Functions-modes", aggregateExpressions.map(_.mode))}
       |${ExplainUtils.generateFieldString("Functions-children", aggregateExpressions.flatMap(aggexp => aggexp.aggregateFunction.children) ++ aggregateExpressions.flatMap(aggexp => aggexp.aggregateFunction.children).map(_.dataType))}
       |${ExplainUtils.generateFieldString("Functions-inputAggBufferAttributes", aggregateExpressions.flatMap(aggexp => aggexp.aggregateFunction.inputAggBufferAttributes) ++ aggregateExpressions.flatMap(aggexp => aggexp.aggregateFunction.inputAggBufferAttributes).map(_.dataType))}
       |${ExplainUtils.generateFieldString("Aggregate Attributes", aggregateAttributes ++ aggregateAttributes.map(_.dataType))}
       |${ExplainUtils.generateFieldString("Results", resultExpressions ++ resultExpressions.map(_.dataType))}
       |""".stripMargin
  }

  override lazy val metrics = Map(
    "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numInputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of input vecBatches"),
    "omniCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni codegen"),
    "getOutputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni getOutput"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputVecBatches" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatches"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
    "numSkippedRows" -> SQLMetrics.createMetric(sparkContext, "number of skipped rows"))

  protected override def needHashTable: Boolean = true

  protected override def doConsumeWithKeys(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    throw new UnsupportedOperationException("ColumnarHashAgg code-gen does not support grouping keys")
  }

  protected override def doProduceWithKeys(ctx: CodegenContext): String = {
    throw new UnsupportedOperationException("ColumnarHashAgg code-gen does not support grouping keys")
  }

  override def supportsColumnar: Boolean = true

  override def supportCodegen: Boolean = false

  override def nodeName: String = "OmniColumnarHashAggregate"

  def buildCheck(): Unit = {
    val attrExpsIdMap = getExprIdMap(child.output)
    val omniGroupByChanel: Array[AnyRef] = groupingExpressions.map(
      exp => rewriteToOmniJsonExpressionLiteral(exp, attrExpsIdMap)).toArray

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
          rewriteToOmniJsonExpressionLiteral(exp.filter.get, attrExpsIdMap)
      }
      if (exp.mode == Final) {
        exp.aggregateFunction match {
          case Sum(_, _) | Min(_) | Max(_) | Count(_) | Average(_, _) | First(_, _) | StddevSamp(_, _) =>
            ShimUtil.aggFuncUnSupportInputTypeCheck(exp.aggregateFunction)
            omniAggFunctionTypes(index) = toOmniAggFunType(exp, true, true)
            omniAggOutputTypes(index) = toOmniAggInOutType(exp.aggregateFunction.dataType)
            omniAggChannels(index) =
              toOmniAggInOutJSonExp(exp.aggregateFunction.inputAggBufferAttributes, attrExpsIdMap)
            omniInputRaws(index) = false
            omniOutputPartials(index) = false
          case _ => throw new UnsupportedOperationException(s"Unsupported aggregate aggregateFunction: ${exp}")
        }
      } else if (exp.mode == PartialMerge) {
        exp.aggregateFunction match {
          case Sum(_, _) | Min(_) | Max(_) | Count(_) | Average(_, _) | First(_, _) | StddevSamp(_, _) =>
            omniAggFunctionTypes(index) = toOmniAggFunType(exp, true, true)
            omniAggOutputTypes(index) =
              toOmniAggInOutType(exp.aggregateFunction.inputAggBufferAttributes)
            omniAggChannels(index) =
              toOmniAggInOutJSonExp(exp.aggregateFunction.inputAggBufferAttributes, attrExpsIdMap)
            omniInputRaws(index) = false
            omniOutputPartials(index) = true
          case _ => throw new UnsupportedOperationException(s"Unsupported aggregate aggregateFunction: ${exp}")
        }
      } else if (exp.mode == Partial) {
        exp.aggregateFunction match {
          case Sum(_, _) | Min(_) | Max(_) | Count(_) | Average(_, _) | First(_, _) | StddevSamp(_, _) =>
            omniAggFunctionTypes(index) = toOmniAggFunType(exp, true)
            omniAggOutputTypes(index) =
              toOmniAggInOutType(exp.aggregateFunction.inputAggBufferAttributes)
            omniAggChannels(index) =
              toOmniAggInOutJSonExp(exp.aggregateFunction.children, attrExpsIdMap)
            omniInputRaws(index) = true
            omniOutputPartials(index) = true
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
    val omniSourceTypes = new Array[DataType](child.output.size)
    child.output.zipWithIndex.foreach {
      case (attr, i) =>
        omniSourceTypes(i) = sparkTypeToOmniType(attr.dataType, attr.metadata)
    }

    for (aggChannel <- omniAggChannels) {
      if (!isSimpleColumnForAll(aggChannel)) {
        checkOmniJsonWhiteList("", aggChannel.toArray)
      }
    }

    if (!isSimpleColumnForAll(omniGroupByChanel.map(groupByChannel => groupByChannel.toString))) {
      checkOmniJsonWhiteList("", omniGroupByChanel)
    }

    for (filter <- omniAggChannelsFilter) {
      if (filter != null && !isSimpleColumn(filter)) {
        checkOmniJsonWhiteList(filter, new Array[AnyRef](0))
      }
    }

    // final steps contail all Final mode aggregate
    if (aggregateExpressions.filter(_.mode == Final).size == aggregateExpressions.size) {
      val finalOut = groupingExpressions.map(_.toAttribute) ++ aggregateAttributes
      finalOut.map(
        exp => sparkTypeToOmniType(exp.dataType, exp.metadata)).toArray
      val finalAttrExprsIdMap = getExprIdMap(finalOut)
      val projectExpressions: Array[AnyRef] = resultExpressions.map(
        exp => rewriteToOmniJsonExpressionLiteral(exp, finalAttrExprsIdMap)).toArray
      if (!isSimpleColumnForAll(projectExpressions.map(expr => expr.toString))) {
        checkOmniJsonWhiteList("", projectExpressions)
      }
    }
  }

  val tmpSparkConf = sparkContext.conf

  def generateSpillDir(conf: SparkConf, subDir: String): String = {
    val localDirs: Array[String] = Utils.getConfiguredLocalDirs(conf)
    val hash = Utils.nonNegativeHash(UUID.randomUUID.toString)
    val root = localDirs(hash % localDirs.length)
    val dir = new File(root, subDir)
    dir.getCanonicalPath
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val addInputTime = longMetric("addInputTime")
    val numInputRows = longMetric("numInputRows")
    val numInputVecBatches= longMetric("numInputVecBatches")
    val omniCodegenTime = longMetric("omniCodegenTime")
    val getOutputTime = longMetric("getOutputTime")
    val numOutputRows = longMetric("numOutputRows")
    val numOutputVecBatches= longMetric("numOutputVecBatches")
    val spillSize = longMetric("spillSize")
    val numSkippedRows = longMetric("numSkippedRows")

    val attrExpsIdMap = getExprIdMap(child.output)
    val omniGroupByChanel = groupingExpressions.map(
      exp => rewriteToOmniJsonExpressionLiteral(exp, attrExpsIdMap)).toArray

    val omniInputRaws = new Array[Boolean](aggregateExpressions.size)
    val omniOutputPartials = new Array[Boolean](aggregateExpressions.size)
    val omniAggFunctionTypes = new Array[FunctionType](aggregateExpressions.size)
    val omniAggOutputTypes = new Array[Array[DataType]](aggregateExpressions.size)
    var omniAggChannels = new Array[Array[String]](aggregateExpressions.size)
    val omniAggChannelsFilter = new Array[String](aggregateExpressions.size)

    val containFirstAgg =
      aggregateExpressions.exists(_.aggregateFunction match {
        case First(_, _) => true
        case _ => false
      })

    var partialStep =
      (aggregateExpressions.count(_.mode == Partial) == aggregateExpressions.size)

    val finalStep =
      (aggregateExpressions.count(_.mode == Final) == aggregateExpressions.size)

    var index = 0
    for (exp <- aggregateExpressions) {
      if (exp.filter.isDefined) {
        omniAggChannelsFilter(index) =
          rewriteToOmniJsonExpressionLiteral(exp.filter.get, attrExpsIdMap)
      }
      if (exp.mode == Final) {
        exp.aggregateFunction match {
          case Sum(_, _) | Min(_) | Max(_) | Count(_) | Average(_, _) | First(_, _) | StddevSamp(_, _) =>
            omniAggFunctionTypes(index) = toOmniAggFunType(exp, true, true)
            omniAggOutputTypes(index) =
              toOmniAggInOutType(exp.aggregateFunction.dataType)
            omniAggChannels(index) =
              toOmniAggInOutJSonExp(exp.aggregateFunction.inputAggBufferAttributes, attrExpsIdMap)
            omniInputRaws(index) = false
            omniOutputPartials(index) = false
          case _ => throw new UnsupportedOperationException(s"Unsupported aggregate aggregateFunction: ${exp}")
        }
      } else if (exp.mode == PartialMerge) {
        ColumnarHashAggregateExec.AssignOmniInfoWhenPartialMergeStage(exp,
          attrExpsIdMap,
          index,
          omniInputRaws,
          omniOutputPartials,
          omniAggFunctionTypes,
          omniAggOutputTypes,
          omniAggChannels)
      } else if (exp.mode == Partial) {
        exp.aggregateFunction match {
          case Sum(_, _) | Min(_) | Max(_) | Count(_) | Average(_, _) | First(_, _) | StddevSamp(_, _) =>
            omniAggFunctionTypes(index) = toOmniAggFunType(exp, true, true)
            omniAggOutputTypes(index) =
              toOmniAggInOutType(exp.aggregateFunction.inputAggBufferAttributes)
            omniAggChannels(index) =
              toOmniAggInOutJSonExp(exp.aggregateFunction.children, attrExpsIdMap)
            omniInputRaws(index) = true
            omniOutputPartials(index) = true
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
    val omniSourceTypes = new Array[DataType](child.output.size)
    child.output.zipWithIndex.foreach {
      case (attr, i) =>
        omniSourceTypes(i) = sparkTypeToOmniType(attr.dataType, attr.metadata)
    }

    child.executeColumnar().mapPartitionsWithIndex { (index, iter) =>
      val columnarConf = ColumnarPluginConfig.getSessionConf
      val spillWriteBufferSize = columnarConf.columnarSpillWriteBufferSize
      val spillMemPctThreshold = columnarConf.columnarSpillMemPctThreshold
      val spillDirDiskReserveSize = columnarConf.columnarSpillDirDiskReserveSize
      val hashAggSpillEnable = columnarConf.enableHashAggSpill
      val hashAggSpillRowThreshold = columnarConf.columnarHashAggSpillRowThreshold
      val enableAdaptivePartialAgg = columnarConf.enableAdaptivePartialAggregation
      val adaptivePartialAggMinRows = columnarConf.adaptivePartialAggregationMinRows
      val adaptivePartialAggRatio = columnarConf.adaptivePartialAggregationRatio

      val spillDirectory = generateSpillDir(tmpSparkConf, "columnarHashAggSpill")
      val sparkSpillConf = new SparkSpillConfig(hashAggSpillEnable, spillDirectory,
        spillDirDiskReserveSize, hashAggSpillRowThreshold, spillMemPctThreshold, spillWriteBufferSize)

      val startCodegen = System.nanoTime()
      val (operator, hashAggregationWithExprOperatorFactory, aggregationWithExprOperatorFactory)  = OmniAdaptorUtil.getAggOperator(groupingExpressions,
        omniGroupByChanel,
        omniAggChannels,
        omniAggChannelsFilter,
        omniSourceTypes,
        omniAggFunctionTypes,
        omniAggOutputTypes,
        omniInputRaws,
        omniOutputPartials,
        sparkSpillConf)
      omniCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startCodegen)

      // close operator
      SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
        spillSize += operator.getSpilledBytes()
        operator.close()
        if (hashAggregationWithExprOperatorFactory != null) {
          hashAggregationWithExprOperatorFactory.close()
        }
        if (aggregationWithExprOperatorFactory != null) {
          aggregationWithExprOperatorFactory.close()
        }
      })

      // handle the condition: only group by cols not aggregator cols
      partialStep = partialStep && aggregateExpressions.nonEmpty

      // The following four conditions must be met:
      // 1. The optimization switch is truned on.
      // 2. It can only be used in the partial agg phase.
      // 3. Hashagg instead of agg.
      // 4. The first aggregator is not included because data inconsistency may occur.
      val hasBenefit = enableAdaptivePartialAgg && partialStep && hashAggregationWithExprOperatorFactory != null && !containFirstAgg

      breakable {
        while (iter.hasNext) {
          val batch = iter.next()
          val input = transColBatchToOmniVecs(batch)
          val startInput = System.nanoTime()
          val vecBatch = new VecBatch(input, batch.numRows())
          operator.addInput(vecBatch)
          addInputTime += NANOSECONDS.toMillis(System.nanoTime() - startInput)
          numInputVecBatches += 1
          numInputRows += batch.numRows()
          // adaptive partial hashagg verification.
          // if the aggregation rate reaches the ratio threshold, the partial phase is skipped.
          if (hasBenefit && numInputRows.value > adaptivePartialAggMinRows) {
            val keyNums: Long = operator.getHashMapUniqueKeys()
            if (keyNums > adaptivePartialAggRatio * numInputRows.value) {
              break
            }
          }
        }
      }

      val startGetOp = System.nanoTime()
      val opOutput = operator.getOutput
      getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)

      var localSchema = this.schema
      if (finalStep) {
        // for final step resultExpressions's inputs from omni-final aggregator
        val omnifinalOutSchema = groupingExpressions.map(_.toAttribute) ++ aggregateAttributes
        localSchema = StructType(omnifinalOutSchema.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
      }

      val hashAggIter = new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = {
          val startGetOp: Long = System.nanoTime()
          var hasNext = opOutput.hasNext
          getOutputTime += NANOSECONDS.toMillis(System.nanoTime() - startGetOp)
          // handle adaptive partial aggregation
          if (hasBenefit) {
            hasNext |= iter.hasNext
          }
          hasNext
        }

        override def next(): ColumnarBatch = {
          if (opOutput.hasNext) {
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
            numOutputVecBatches += 1

            vecBatch.close()
            new ColumnarBatch(vectors.toArray, vecBatch.getRowCount)
          } else if (hasBenefit && iter.hasNext) {
            // handle adaptive partial aggregation
            val batch = iter.next()
            numInputVecBatches += 1
            numInputRows += batch.numRows()
            numSkippedRows += batch.numRows()

            val input = transColBatchToOmniVecs(batch)
            val inputVecBatch = new VecBatch(input, batch.numRows())
            // schema alignment
            val outputVecBatch: VecBatch = operator.alignSchema(inputVecBatch)
            val vectors: Seq[OmniColumnVector] = OmniColumnVector.allocateColumns(
              outputVecBatch.getRowCount, localSchema, false)
            vectors.zipWithIndex.foreach { case (vector, i) =>
              vector.reset()
              vector.setVec(outputVecBatch.getVectors()(i))
            }

            numOutputRows += outputVecBatch.getRowCount
            numOutputVecBatches+= 1

            outputVecBatch.close()
            new ColumnarBatch(vectors.toArray, outputVecBatch.getRowCount)
          } else {
            throw new RuntimeException("The branch should not have gone")
          }
        }
      }
      if (finalStep) {
        val finalOut = groupingExpressions.map(_.toAttribute) ++ aggregateAttributes
        val finalAttrExprsIdMap = getExprIdMap(finalOut)
        val projectInputTypes = finalOut.map(
          exp => sparkTypeToOmniType(exp.dataType, exp.metadata)).toArray
        val projectExpressions = resultExpressions.map(
          exp => rewriteToOmniJsonExpressionLiteral(exp, finalAttrExprsIdMap)).toArray

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
  def AssignOmniInfoWhenPartialMergeStage(
                                           exp:AggregateExpression,
                                           exprsIdMap: Map[ExprId, Int],
                                           index: Int,
                                           omniInputRaws : Array[Boolean],
                                           omniOutputPartials : Array[Boolean],
                                           omniAggFunctionTypes : Array[FunctionType],
                                           omniAggOutputTypes : Array[Array[DataType]],
                                           omniAggChannels : Array[Array[String]]): Unit ={
    exp.aggregateFunction match {
      case Sum(_, _) | Min(_) | Max(_) | Count(_) | Average(_, _) | First(_, _) | StddevSamp(_, _) =>
        omniAggFunctionTypes(index) = toOmniAggFunType(exp, true)
        omniAggOutputTypes(index) =
          toOmniAggInOutType(exp.aggregateFunction.inputAggBufferAttributes)
        omniAggChannels(index) =
          toOmniAggInOutJSonExp(exp.aggregateFunction.inputAggBufferAttributes, exprsIdMap)
        omniInputRaws(index) = false
        omniOutputPartials(index) = true
      case _ => throw new UnsupportedOperationException(s"Unsupported aggregate aggregateFunction: ${exp}")
    }
  }
}
