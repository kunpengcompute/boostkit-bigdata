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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.TaskContext
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.KVIterator

import java.util.concurrent.TimeUnit._

/**
 * Hash-based aggregate operator that can also fallback to sorting when data exceeds memory size.
 */
case class SimpleCountAggregateExec(
                              requiredChildDistributionExpressions: Option[Seq[Expression]],
                              groupingExpressions: Seq[NamedExpression],
                              aggregateExpressions: Seq[AggregateExpression],
                              aggregateAttributes: Seq[Attribute],
                              initialInputBufferOffset: Int,
                              resultExpressions: Seq[NamedExpression],
                              child: SparkPlan,
                              isDistinctCount: Boolean = false,
                              var columnStat: BigInt = -1)
  extends BaseAggregateExec
    with BlockingOperatorWithCodegen {

  require(HashAggregateExec.supportsAggregate(aggregateBufferAttributes))

  override def nodeName: String = "SimpleCountAggregate"

  override lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
    "aggTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in aggregation build"),
    "avgHashProbe" ->
      SQLMetrics.createAverageMetric(sparkContext, "avg hash probe bucket list iters"))

  // This is for testing. We force TungstenAggregationIterator to fall back to the unsafe row hash
  // map and/or the sort-based aggregation once it has processed a given number of input rows.
  private val testFallbackStartsAt: Option[(Int, Int)] = {
    Option(session).map { s =>
      s.conf.get("spark.sql.TungstenAggregate.testFallbackStartsAt", null)
    }.orNull match {
      case null | "" => None
      case fallbackStartsAt =>
        val splits = fallbackStartsAt.split(",").map(_.trim)
        Some((splits.head.toInt, splits.last.toInt))
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val peakMemory = longMetric("peakMemory")
    val spillSize = longMetric("spillSize")
    val avgHashProbe = longMetric("avgHashProbe")
    val aggTime = longMetric("aggTime")

    child.execute().mapPartitionsWithIndex { (partIndex, iter) =>

      val beforeAgg = System.nanoTime()
      val hasInput = iter.hasNext
      val res = if (!hasInput && groupingExpressions.nonEmpty) {
        // This is a grouped aggregate and the input iterator is empty,
        // so return an empty iterator.
        Iterator.empty
      } else {
        val aggregationIterator =
          new SimpleCountTungstenAggIter(
            partIndex,
            groupingExpressions,
            aggregateExpressions,
            aggregateAttributes,
            initialInputBufferOffset,
            resultExpressions,
            (expressions, inputSchema) =>
              MutableProjection.create(expressions, inputSchema),
            inputAttributes,
            iter,
            testFallbackStartsAt,
            numOutputRows,
            peakMemory,
            spillSize,
            avgHashProbe,
            isDistinctCount,
            columnStat)
        if (!hasInput && groupingExpressions.isEmpty) {
          numOutputRows += 1
          Iterator.single[UnsafeRow](aggregationIterator.outputForEmptyGroupingKeyWithoutInput())
        } else {
          aggregationIterator
        }
      }
      aggTime += NANOSECONDS.toMillis(System.nanoTime() - beforeAgg)
      res
    }
  }

  private val modes = aggregateExpressions.map(_.mode).distinct

  override def usedInputs: AttributeSet = inputSet

  override def supportCodegen: Boolean = false

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  private val groupingAttributes = groupingExpressions.map(_.toAttribute)
  private val groupingKeySchema = StructType.fromAttributes(groupingAttributes)
  private val declFunctions = aggregateExpressions.map(_.aggregateFunction)
    .filter(_.isInstanceOf[DeclarativeAggregate])
    .map(_.asInstanceOf[DeclarativeAggregate])
  private val bufferSchema = StructType.fromAttributes(aggregateBufferAttributes)

  /**
   * This is called by generated Java class, should be public.
   */
  def createHashMap(): UnsafeFixedWidthAggregationMap = {
    // create initialized aggregate buffer
    val initExpr = declFunctions.flatMap(f => f.initialValues)
    val initialBuffer = UnsafeProjection.create(initExpr)(EmptyRow)

    // create hashMap
    new UnsafeFixedWidthAggregationMap(
      initialBuffer,
      bufferSchema,
      groupingKeySchema,
      TaskContext.get(),
      1024 * 16, // initial capacity
      TaskContext.get().taskMemoryManager().pageSizeBytes
    )
  }

  def getTaskMemoryManager(): TaskMemoryManager = {
    TaskContext.get().taskMemoryManager()
  }

  def getEmptyAggregationBuffer(): InternalRow = {
    val initExpr = declFunctions.flatMap(f => f.initialValues)
    val initialBuffer = UnsafeProjection.create(initExpr)(EmptyRow)
    initialBuffer
  }

  /**
   * This is called by generated Java class, should be public.
   */
  def createUnsafeJoiner(): UnsafeRowJoiner = {
    GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)
  }

  /**
   * Called by generated Java class to finish the aggregate and return a KVIterator.
   */
  def finishAggregate(
                       hashMap: UnsafeFixedWidthAggregationMap,
                       sorter: UnsafeKVExternalSorter,
                       peakMemory: SQLMetric,
                       spillSize: SQLMetric,
                       avgHashProbe: SQLMetric): KVIterator[UnsafeRow, UnsafeRow] = {

    // update peak execution memory
    val mapMemory = hashMap.getPeakMemoryUsedBytes
    val sorterMemory = Option(sorter).map(_.getPeakMemoryUsedBytes).getOrElse(0L)
    val maxMemory = Math.max(mapMemory, sorterMemory)
    val metrics = TaskContext.get().taskMetrics()
    peakMemory.add(maxMemory)
    metrics.incPeakExecutionMemory(maxMemory)

    // Update average hashmap probe
    avgHashProbe.set(hashMap.getAvgHashProbeBucketListIterations)

    if (sorter == null) {
      // not spilled
      return hashMap.iterator()
    }

    // merge the final hashMap into sorter
    sorter.merge(hashMap.destructAndCreateExternalSorter())
    hashMap.free()
    val sortedIter = sorter.sortedIterator()

    // Create a KVIterator based on the sorted iterator.
    new KVIterator[UnsafeRow, UnsafeRow] {

      // Create a MutableProjection to merge the rows of same key together
      val mergeExpr = declFunctions.flatMap(_.mergeExpressions)
      val mergeProjection = MutableProjection.create(
        mergeExpr,
        aggregateBufferAttributes ++ declFunctions.flatMap(_.inputAggBufferAttributes))
      val joinedRow = new JoinedRow()

      var currentKey: UnsafeRow = null
      var currentRow: UnsafeRow = null
      var nextKey: UnsafeRow = if (sortedIter.next()) {
        sortedIter.getKey
      } else {
        null
      }

      override def next(): Boolean = {
        if (nextKey != null) {
          currentKey = nextKey.copy()
          currentRow = sortedIter.getValue.copy()
          nextKey = null
          // use the first row as aggregate buffer
          mergeProjection.target(currentRow)

          // merge the following rows with same key together
          var findNextGroup = false
          while (!findNextGroup && sortedIter.next()) {
            val key = sortedIter.getKey
            if (currentKey.equals(key)) {
              mergeProjection(joinedRow(currentRow, sortedIter.getValue))
            } else {
              // We find a new group.
              findNextGroup = true
              nextKey = key
            }
          }

          true
        } else {
          spillSize.add(sorter.getSpillSize)
          false
        }
      }

      override def getKey: UnsafeRow = currentKey
      override def getValue: UnsafeRow = currentRow
      override def close(): Unit = {
        sortedIter.close()
      }
    }
  }

  override def verboseString(maxFields: Int): String = toString(verbose = true, maxFields)

  override def simpleString(maxFields: Int): String = toString(verbose = false, maxFields)

  private def toString(verbose: Boolean, maxFields: Int): String = {
    val allAggregateExpressions = aggregateExpressions

    testFallbackStartsAt match {
      case None =>
        val keyString = truncatedString(groupingExpressions, "[", ", ", "]", maxFields)
        val functionString = truncatedString(allAggregateExpressions, "[", ", ", "]", maxFields)
        val outputString = truncatedString(output, "[", ", ", "]", maxFields)
        if (verbose) {
          s"HashAggregate(keys=$keyString, functions=$functionString, output=$outputString)"
        } else {
          s"HashAggregate(keys=$keyString, functions=$functionString)"
        }
      case Some(fallbackStartsAt) =>
        s"HashAggregateWithControlledFallback $groupingExpressions " +
          s"$allAggregateExpressions $resultExpressions fallbackStartsAt=$fallbackStartsAt"
    }
  }

  override protected def doProduce(ctx: CodegenContext): String = ""

  override protected def withNewChildInternal(newChild: SparkPlan): SimpleCountAggregateExec =
    copy(child = newChild)
}