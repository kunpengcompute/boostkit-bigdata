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
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.transColBatchToOmniVecs
import nova.hetu.omniruntime.`type`.DataType
import nova.hetu.omniruntime.operator.config.{OperatorConfig, SpillConfig}
import nova.hetu.omniruntime.operator.filter.OmniFilterAndProjectOperatorFactory
import nova.hetu.omniruntime.vector.VecBatch

import scala.collection.JavaConverters.seqAsJavaList
import org.apache.spark.{InterruptibleIterator, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.ColumnarProjection.dealPartitionData
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.util.SparkMemoryUtils.addLeakSafeTaskCompletionListener
import org.apache.spark.sql.execution.vectorized.OmniColumnVector
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class ColumnarProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
  extends UnaryExecNode
    with AliasAwareOutputPartitioning
    with AliasAwareOutputOrdering {

  override def supportsColumnar: Boolean = true

  override def nodeName: String = "OmniColumnarProject"

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override lazy val metrics = Map(
    "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
    "omniCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni codegen"),
    "getOutputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni getOutput"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatchs"))

  def buildCheck(): Unit = {
    val omniAttrExpsIdMap = getExprIdMap(child.output)
    child.output.map(
      exp => sparkTypeToOmniType(exp.dataType, exp.metadata)).toArray
    val omniExpressions: Array[AnyRef] = projectList.map(
      exp => rewriteToOmniJsonExpressionLiteral(exp, omniAttrExpsIdMap)).toArray
    checkOmniJsonWhiteList("", omniExpressions)
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val numOutputVecBatchs = longMetric("numOutputVecBatchs")
    val addInputTime = longMetric("addInputTime")
    val omniCodegenTime = longMetric("omniCodegenTime")
    val getOutputTime = longMetric("getOutputTime")

    val omniAttrExpsIdMap = getExprIdMap(child.output)
    val omniInputTypes = child.output.map(
      exp => sparkTypeToOmniType(exp.dataType, exp.metadata)).toArray
    val omniExpressions = projectList.map(
      exp => rewriteToOmniJsonExpressionLiteral(exp, omniAttrExpsIdMap)).toArray

    child.executeColumnar().mapPartitionsWithIndexInternal { (index, iter) =>
      dealPartitionData(numOutputRows, numOutputVecBatchs, addInputTime, omniCodegenTime,
        getOutputTime, omniInputTypes, omniExpressions, iter, this.schema)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override protected def outputExpressions: Seq[NamedExpression] = projectList

  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", projectList)}
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |""".stripMargin
  }
}

case class ColumnarFilterExec(condition: Expression, child: SparkPlan)
  extends UnaryExecNode with PredicateHelper {

  override def supportsColumnar: Boolean = true
  override def nodeName: String = "OmniColumnarFilter"

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
    case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }

  // If one expression and its children are null intolerant, it is null intolerant.
  private def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  override def output: Seq[Attribute] = {
    child.output.map { a =>
      if (a.nullable && notNullAttributes.contains(a.exprId)) {
        a.withNullability(false)
      } else {
        a
      }
    }
  }

  override lazy val metrics = Map(
    "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
    "numInputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of input vecBatchs"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "omniCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni codegen"),
    "getOutputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni getOutput"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatchs"))


  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      val predicate = Predicate.create(condition, child.output)
      predicate.initialize(0)
      iter.filter { row =>
        val r = predicate.eval(row)
        if (r) numOutputRows += 1
        r
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |Condition : ${condition}
       |""".stripMargin
  }

  def buildCheck(): Unit = {
    val omniAttrExpsIdMap = getExprIdMap(child.output)
    child.output.map(
      exp => sparkTypeToOmniType(exp.dataType, exp.metadata)).toArray
    val filterExpression = rewriteToOmniJsonExpressionLiteral(condition, omniAttrExpsIdMap)
    checkOmniJsonWhiteList(filterExpression, new Array[AnyRef](0))
  }

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric("numInputRows")
    val numInputVecBatchs = longMetric("numInputVecBatchs")
    val numOutputRows = longMetric("numOutputRows")
    val numOutputVecBatchs = longMetric("numOutputVecBatchs")
    val addInputTime = longMetric("addInputTime")
    val omniCodegenTime = longMetric("omniCodegenTime")
    val getOutputTime = longMetric("getOutputTime")

    val omniAttrExpsIdMap = getExprIdMap(child.output)
    val omniInputTypes = child.output.map(
      exp => sparkTypeToOmniType(exp.dataType, exp.metadata)).toArray
    val omniProjectIndices = child.output.map(
      exp => sparkProjectionToOmniJsonProjection(exp, omniAttrExpsIdMap(exp.exprId))).toArray
    val omniExpression = rewriteToOmniJsonExpressionLiteral(condition, omniAttrExpsIdMap)

    child.executeColumnar().mapPartitionsWithIndexInternal { (index, iter) =>
      val startCodegen = System.nanoTime()
      val filterOperatorFactory = new OmniFilterAndProjectOperatorFactory(
        omniExpression, omniInputTypes, seqAsJavaList(omniProjectIndices), 1, new OperatorConfig(SpillConfig.NONE, IS_SKIP_VERIFY_EXP))
      val filterOperator = filterOperatorFactory.createOperator
      omniCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startCodegen)

      // close operator
      addLeakSafeTaskCompletionListener[Unit](_ => {
        filterOperator.close()
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
            filterOperator.addInput(vecBatch)
            addInputTime += NANOSECONDS.toMillis(System.nanoTime() - startInput)
            numInputVecBatchs += 1
            numInputRows += batch.numRows()

            val startGetOp = System.nanoTime()
            results = filterOperator.getOutput
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
          numOutputVecBatchs += 1
          vecBatch.close()
          new ColumnarBatch(vectors.toArray, vecBatch.getRowCount)
        }
      }
    }
  }
}

case class ColumnarConditionProjectExec(projectList: Seq[NamedExpression],
                                        condition: Expression,
                                        child: SparkPlan)
  extends UnaryExecNode
    with AliasAwareOutputPartitioning
    with AliasAwareOutputOrdering {

  override def supportsColumnar: Boolean = true

  override def nodeName: String = "OmniColumnarConditionProject"

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override lazy val metrics = Map(
    "addInputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni addInput"),
    "numInputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of input vecBatchs"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "omniCodegenTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni codegen"),
    "getOutputTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in omni getOutput"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputVecBatchs" -> SQLMetrics.createMetric(sparkContext, "number of output vecBatchs"))

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric("numInputRows")
    val numInputVecBatchs = longMetric("numInputVecBatchs")
    val numOutputRows = longMetric("numOutputRows")
    val numOutputVecBatchs = longMetric("numOutputVecBatchs")
    val addInputTime = longMetric("addInputTime")
    val omniCodegenTime = longMetric("omniCodegenTime")
    val getOutputTime = longMetric("getOutputTime")

    val omniAttrExpsIdMap = getExprIdMap(child.output)
    val omniInputTypes = child.output.map(
      exp => sparkTypeToOmniType(exp.dataType, exp.metadata)).toArray
    val omniExpressions = projectList.map(
      exp => rewriteToOmniJsonExpressionLiteral(exp, omniAttrExpsIdMap)).toArray
    val conditionExpression = rewriteToOmniJsonExpressionLiteral(condition, omniAttrExpsIdMap)

    child.executeColumnar().mapPartitionsWithIndexInternal { (index, iter) =>
      val startCodegen = System.nanoTime()
      val operatorFactory = new OmniFilterAndProjectOperatorFactory(
        conditionExpression, omniInputTypes, seqAsJavaList(omniExpressions), 1, new OperatorConfig(SpillConfig.NONE, IS_SKIP_VERIFY_EXP))
      val operator = operatorFactory.createOperator
      omniCodegenTime += NANOSECONDS.toMillis(System.nanoTime() - startCodegen)
      // close operator
      addLeakSafeTaskCompletionListener[Unit](_ => {
        operator.close()
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
            operator.addInput(vecBatch)
            addInputTime += NANOSECONDS.toMillis(System.nanoTime() - startInput)
            numInputVecBatchs += 1
            numInputRows += batch.numRows()

            val startGetOp = System.nanoTime()
            results = operator.getOutput
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
          numOutputVecBatchs += 1
          vecBatch.close()
          new ColumnarBatch(vectors.toArray, vecBatch.getRowCount)
        }
      }
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override protected def outputExpressions: Seq[NamedExpression] = projectList

  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", projectList)}
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |""".stripMargin
  }
}

/**
 * Physical plan for unioning two plans, without a distinct. This is UNION ALL in SQL.
 *
 * If we change how this is implemented physically, we'd need to update
 * [[org.apache.spark.sql.catalyst.plans.logical.Union.maxRowsPerPartition]].
 */
case class ColumnarUnionExec(children: Seq[SparkPlan]) extends SparkPlan {

  override def nodeName: String = "OmniColumnarUnion"

  // updating nullability to make all the children consistent
  override def output: Seq[Attribute] = {
    children.map(_.output).transpose.map { attrs =>
      val firstAttr = attrs.head
      val nullable = attrs.exists(_.nullable)
      val newDt = attrs.map(_.dataType).reduce(StructType.merge)
      if (firstAttr.dataType == newDt) {
        firstAttr.withNullability(nullable)
      } else {
        AttributeReference(firstAttr.name, newDt, nullable, firstAttr.metadata)(
          firstAttr.exprId, firstAttr.qualifier)
      }
    }
  }

  def buildCheck(): Unit = {
    val inputTypes = new Array[DataType](output.size)
    output.zipWithIndex.foreach {
      case (attr, i) =>
        inputTypes(i) = sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
  }

  protected override def doExecute(): RDD[InternalRow] =
    sparkContext.union(children.map(_.execute()))

  override def supportsColumnar: Boolean = true

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] =
    sparkContext.union(children.map(_.executeColumnar()))
}

class ColumnarRangeExec(range: org.apache.spark.sql.catalyst.plans.logical.Range)
  extends RangeExec(range: org.apache.spark.sql.catalyst.plans.logical.Range) {

  private val maxRowCountPerBatch = 10000
  override def supportsColumnar: Boolean = true
  override def supportCodegen: Boolean = false

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")

    sqlContext
      .sparkContext
      .parallelize(0 until numSlices, numSlices)
      .mapPartitionsWithIndex { (i, _) =>
        val partitionStart = (i * numElements) / numSlices * step + start
        val partitionEnd = (((i + 1) * numElements) / numSlices) * step + start

        def getSafeMargin(bi: BigInt): Long =
          if (bi.isValidLong) {
            bi.toLong
          } else if (bi > 0) {
            Long.MaxValue
          } else {
            Long.MinValue
          }
        val safePartitionStart = getSafeMargin(partitionStart) // inclusive
        val safePartitionEnd = getSafeMargin(partitionEnd) // exclusive, unless start == this
        val taskContext = TaskContext.get()

        val iter: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {
          private[this] var number: Long = safePartitionStart
          private[this] var overflow: Boolean = false

          override def hasNext: Boolean =
            if (!overflow) {
              if (step > 0) {
                number < safePartitionEnd
              } else {
                number > safePartitionEnd
              }
            } else false

          override def next(): ColumnarBatch = {
            val start = number
            val remainingSteps = (safePartitionEnd - start) / step
            // Start is inclusive so we need to produce at least one row
            val rowsThisBatch = Math.max(1, Math.min(remainingSteps, maxRowCountPerBatch))
            val endInclusive = start + ((rowsThisBatch - 1) * step)
            number = endInclusive + step
            if (number < endInclusive ^ step < 0) {
              // we have Long.MaxValue + Long.MaxValue < Long.MaxValue
              // and Long.MinValue + Long.MinValue > Long.MinValue, so iff the step causes a
              // step back, we are pretty sure that we have an overflow.
              overflow = true
            }
            val vec = new OmniColumnVector(rowsThisBatch.toInt, LongType, true)
            var s = start
            for (i <- 0 until rowsThisBatch.toInt) {
              vec.putLong(i, s)
              s += step
            }
            numOutputRows += rowsThisBatch.toInt
            new ColumnarBatch(Array(vec), rowsThisBatch.toInt)
          }
        }
        new InterruptibleIterator(taskContext, iter)
      }
  }

  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

  override def doCanonicalize(): SparkPlan = {
    new ColumnarRangeExec(
      range.canonicalized.asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Range])
  }
}
