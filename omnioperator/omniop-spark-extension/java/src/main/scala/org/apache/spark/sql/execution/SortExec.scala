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

import java.util.concurrent.TimeUnit._
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_MILLIS
import org.apache.spark.sql.execution.UnsafeExternalRowSorter.PrefixComputer
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.collection.unsafe.sort.PrefixComparator


/**
 * Base class of [[SortExec]] and [[TopNSortExec]]. All subclasses of this class need to override
 * their own sorter which inherits from [[org.apache.spark.sql.execution.AbstractUnsafeRowSorter]]
 * to perform corresponding sorting.
 *
 * @param global when true performs a global sort of all partitions by shuffling the data first
 *               if necessary.
 * @param testSpillFrequency Method for configuring periodic spilling in unit tests.
 *                           If set, will spill every 'frequency' records.
 * */
abstract class SortExecBase(
                       sortOrder: Seq[SortOrder],
                       global: Boolean,
                       child: SparkPlan,
                       testSpillFrequency: Int = 0)
                       extends UnaryExecNode with BlockingOperatorWithCodegen {

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder

  // sort performed is local within a given partition so will retain
  // child operator's partitioning
  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder):: Nil else UnspecifiedDistribution :: Nil

  private val enableRadixSort = conf.enableRadixSort

  override lazy val metrics = Map(
    "sortTime" -> SQLMetrics.createTimingMetric(sparkContext, "sort time"),
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size")
  )

  protected val sorterClassName: String

  protected def newSorterInstance(
                                 ordering: Ordering[InternalRow],
                                 prefixComparator: PrefixComparator,
                                 prefixComputer: PrefixComputer,
                                 pageSize: Long,
                                 canSortFullyWIthPrefix: Boolean): AbstractUnsafeRowSorter

  private[sql] var rowSorter: AbstractUnsafeRowSorter = _

  /**
   * This method gets invoked only once for each SortExec instance to initialize
   * an AbstractUnsafeRowSorter, both 'plan.execute' and code generation are using it.
   * In the code generation code path, we need to call this function outside the class
   * so we should make it public
   * */
  def createSorter(): AbstractUnsafeRowSorter = {
    val ordering = RowOrdering.create(sortOrder, output)

    // THe comparator for comparing prefix
    val boundSortExpression = BindReferences.bindReference(sortOrder.head, output)
    val prefixComparator = SortPrefixUtils.getPrefixComparator(boundSortExpression)

    val canSortFullyWIthPrefix = sortOrder.length == 1 &&
      SortPrefixUtils.canSortFullyWithPrefix(boundSortExpression)

    // The generator for prefix
    val prefixExpr = SortPrefix(boundSortExpression)
    val prefixProjection = UnsafeProjection.create(Seq(prefixExpr))
    val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
      private val result = new UnsafeExternalRowSorter.PrefixComputer.Prefix
      override def computePrefix(row: InternalRow):
        UnsafeExternalRowSorter.PrefixComputer.Prefix = {
          val prefix = prefixProjection.apply(row)
          result.isNull = prefix.isNullAt(0)
          result.value = if (result.isNull) prefixExpr.nullValue else prefix.getLong(0)
        result
      }
    }

    val pageSize = SparkEnv.get.memoryManager.pageSizeBytes
    rowSorter = newSorterInstance(ordering, prefixComparator, prefixComputer,
      pageSize, canSortFullyWIthPrefix)

    if (testSpillFrequency > 0) {
      rowSorter.setTestSpillFrequency(testSpillFrequency)
    }
    rowSorter
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val peakMemory = longMetric("peakMemory")
    val spillSize = longMetric("spillSize")
    val sortTime = longMetric("sortTime")

    child.execute().mapPartitionsInternal { iter =>
      val sorter = createSorter()
      val metrics = TaskContext.get().taskMetrics()

      // Remember spill data size of this task before execute this operator,
      // so that we can figure out how many bytes we spilled for this operator.
      val spillSizeBefore = metrics.memoryBytesSpilled
      val sortedIterator = sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])
      sortTime += NANOSECONDS.toMillis(sorter.getSortTimeNanos)
      peakMemory += sorter.getPeakMemoryUsage
      spillSize += metrics.memoryBytesSpilled - spillSizeBefore
      metrics.incPeakExecutionMemory(sorter.getPeakMemoryUsage)

      sortedIterator
    }
  }

  override def usedInputs: AttributeSet = AttributeSet(Seq.empty)

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs
  }

  // Name of sorter variable used in codegen
  private var sorterVariable: String = _

  override protected def doProduce(ctx: CodegenContext): String = {
    val needToSort = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN,
      "needToSort", v => s"$v = true;")

    // Initalize the class member variables. This includes the instance of the Sorter
    // and the iterator to return sorted rows.
    val thisPlan = ctx.addReferenceObj("plan", this)
    // Inline mutable state since not many Sort operations in a task
    sorterVariable = ctx.addMutableState(sorterClassName, "sorter",
      v => s"$v = $thisPlan.createSorter();", forceInline = true)
    val metrics = ctx.addMutableState(classOf[TaskMetrics].getName, "metrics",
      v => s"$v = org.apache.spark.TaskContext.get().taskMetrics();", forceInline = true)
    val sortedIterator = ctx.addMutableState("scala.collection.Iterator<UnsafeRow>",
      "sortedIter", forceInline = true)

    val addToSorter = ctx.freshName("addToSorter")
    val addToSorterFuncName = ctx.addNewFunction(addToSorter,
      s"""
        | private void $addToSorter() throws java.io.IOException {
        |   ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
        | }
       """.stripMargin.trim)

    val outputRow = ctx.freshName("outputRow")
    val peakMemory = metricTerm(ctx, "peakMemory")
    val spillSize = metricTerm(ctx, "spillSize")
    val spillSizeBefore = ctx.freshName("spillSizeBefore")
    val sortTime = metricTerm(ctx, "sortTime")
    s"""
      | if ($needToSort) {
      |   long $spillSizeBefore = $metrics.memoryBytesSpilled();
      |   $addToSorterFuncName();
      |   $sortedIterator = $sorterVariable.sort();
      |   $sortTime.add($sorterVariable.getSortTimeNanos() / $NANOS_PER_MILLIS);
      |   $peakMemory.add($sorterVariable.getPeakMemoryUsage());
      |   $spillSize.add($metrics.memoryBytesSpilled() - $spillSizeBefore);
      |   $metrics.incPeakExecutionMemory($sorterVariable.getPeakMemoryUsage());
      |   $needToSort = false;
      | }
      |
      | while ($limitNotReachedCond $sortedIterator.hasNext()) {
      |  UnsafeRow $outputRow = (UnsafeRow)$sortedIterator.next();
      |  ${consume(ctx, null, outputRow)}
      |  if (shouldStop()) return;
      | }
     """.stripMargin.trim
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    s"""
      | ${row.code}
      | $sorterVariable.insertRow((UnsafeRow)${row.value});
     """.stripMargin
  }

  /**
   * In BaseSortExec, we overwrites cleanupResources to close AbstractUnsafeRowSorter.
   * */

  override protected[sql] def cleanupResources(): Unit = {
    if (rowSorter != null) {
      // There's possible for rowSorter is null here, for example, in the scenario of empty
      // iterator in the current task, the downstream physical node(like SortMergeJoinExec) will
      // trigger cleanupResources before rowSorter initialized in createSorter
      rowSorter.cleanupResources()
    }
    super.cleanupResources()
  }
}


/**
 * Performs (external) sorting
 * */
case class SortExec(
                   sortOrder: Seq[SortOrder],
                   global: Boolean,
                   child: SparkPlan,
                   testSpillFrequency: Int = 0)
                   extends SortExecBase(sortOrder, global, child, testSpillFrequency) {
  private val enableRadixSort = conf.enableRadixSort


  override val sorterClassName: String = classOf[UnsafeExternalRowSorter].getName

  override def newSorterInstance(
                                  ordering: Ordering[InternalRow],
                                  prefixComparator: PrefixComparator,
                                  prefixComputer: PrefixComputer,
                                  pageSize: Long,
                                  canSortFullyWIthPrefix: Boolean): UnsafeExternalRowSorter = {
    UnsafeExternalRowSorter.create(
      schema,
      ordering,
      prefixComparator,
      prefixComputer,
      pageSize,
      enableRadixSort && canSortFullyWIthPrefix)
  }

  override def createSorter(): UnsafeExternalRowSorter = {
    super.createSorter().asInstanceOf[UnsafeExternalRowSorter]
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SortExec = {
    copy(child = newChild)
  }
}

/**
 * Performs topN sort
 *
 * @param strictTopN when true it strictly returns n results. This param distinguishes
 *                   [[RowNumber]] from [[Rank]]. [[RowNumber]] corresponds to true
 *                   and [[Rank]] corresponds to false.
 * @param partitionSpec partitionSpec of [[org.apache.spark.sql.execution.window.WindowExec]]
 * @param sortOrder orderSpec of [[org.apache.spark.sql.execution.window.WindowExec]]
 * */
case class TopNSortExec(
                       n: Int,
                       strictTopN: Boolean,
                       partitionSpec: Seq[Expression],
                       sortOrder: Seq[SortOrder],
                       global: Boolean,
                       child: SparkPlan)
                       extends SortExecBase(sortOrder, global, child, 0) {

  override val sorterClassName: String = classOf[UnsafeTopNRowSorter].getName

  override def newSorterInstance(
                                   ordering: Ordering[InternalRow],
                                   prefixComparator: PrefixComparator,
                                   prefixComputer: PrefixComputer,
                                   pageSize: Long,
                                   canSortFullyWIthPrefix: Boolean): UnsafeTopNRowSorter = {
    val partitionSpecProjection = UnsafeProjection.create(partitionSpec, output)
    UnsafeTopNRowSorter.create(
      n,
      strictTopN,
      schema,
      partitionSpecProjection,
      ordering,
      prefixComparator,
      prefixComputer,
      pageSize,
      canSortFullyWIthPrefix)
  }

  override def createSorter(): UnsafeTopNRowSorter = {
    super.createSorter().asInstanceOf[UnsafeTopNRowSorter]
  }

  override protected def withNewChildInternal(newChild: SparkPlan): TopNSortExec = {
    copy(child = newChild)
  }
}
