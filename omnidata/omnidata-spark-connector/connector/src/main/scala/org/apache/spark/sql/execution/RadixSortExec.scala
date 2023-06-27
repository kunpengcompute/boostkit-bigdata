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
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.LongType

case class RadixSortExec(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan,
    testSpillFrequency: Int = 0)
    extends UnaryExecNode with BlockingOperatorWithCodegen {

  override def nodeName: String = "OmniRadixSort"

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder

  // sort performed is local within a given partition so will retain
  // child operator's partitioning
  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  private val enableRadixSort = session.sqlContext.conf.enableRadixSort

  override lazy val metrics = Map(
    "sortTime" -> SQLMetrics.createTimingMetric(sparkContext, "sort time"),
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

  private[sql] var rowSorter: UnsafeExternalRadixRowSorter = _

  /**
   * This method gets invoked only once for each SortExec instance to initialize an
   * UnsafeExternalRowSorter, both `plan.execute` and code generation are using it.
   * In the code generation code path, we need to call this function outside the class so we
   * should make it public.
   */
  def createSorter(): UnsafeExternalRadixRowSorter = {
    val ordering = RowOrdering.create(sortOrder, output)

    // The comparator for comparing prefix
    // 转换下排序字段的字段对象（AttributeReference->BoundReference，只包含字段序号，数据类型，是否可以是null）

    // TODO 修改1，这里取sortOrder所有的，限制两个
    val boundSortExpressions = sortOrder.map(x => BindReferences.bindReference(x, output))
    // 比较器按这几个维度分类：有符号/无符号；NULL排最前/NULL排最后；升序/降序
    val prefixComparators = boundSortExpressions.map(SortPrefixUtils.getPrefixComparator)

    // The generator for prefix
    // 为各种数据类型生成可long类型的值作为前缀比较
    val prefixExprs = boundSortExpressions.map(SortPrefix)
    val prefixProjections = prefixExprs.map(x => UnsafeProjection.create(Seq(x)))
    val prefixComputers = prefixProjections.zip(prefixExprs)
        .map { case (prefixProjection, prefixExpr) =>
          new UnsafeExternalRadixRowSorter.PrefixComputer {
            private val result = new UnsafeExternalRadixRowSorter.PrefixComputer.Prefix

            override def computePrefix(row: InternalRow):
            UnsafeExternalRadixRowSorter.PrefixComputer.Prefix = {
              val prefix = prefixProjection.apply(row)
              result.isNull = prefix.isNullAt(0)
              result.value = if (result.isNull) prefixExpr.nullValue else prefix.getLong(0)
              result
            }
          }
        }

    val pageSize = SparkEnv.get.memoryManager.pageSizeBytes
    rowSorter = UnsafeExternalRadixRowSorter.create(
      schema, ordering,
      scala.collection.JavaConverters.seqAsJavaList(prefixComparators),
      scala.collection.JavaConverters.seqAsJavaList(prefixComputers),
      pageSize, true)

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
      // Remember spill data size of this task before execute this operator so that we can
      // figure out how many bytes we spilled for this operator.
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
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  // Name of sorter variable used in codegen.
  private var sorterVariable: String = _

  override protected def doProduce(ctx: CodegenContext): String = {
    val needToSort =
      ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "needToSort", v => s"$v = true;")

    // Initialize the class member variables. This includes the instance of the Sorter and
    // the iterator to return sorted rows.
    val thisPlan = ctx.addReferenceObj("plan", this)
    // Inline mutable state since not many Sort operations in a task
    sorterVariable = ctx.addMutableState(classOf[UnsafeExternalRowSorter].getName, "sorter",
      v => s"$v = $thisPlan.createSorter();", forceInline = true)
    val metrics = ctx.addMutableState(classOf[TaskMetrics].getName, "metrics",
      v => s"$v = org.apache.spark.TaskContext.get().taskMetrics();", forceInline = true)
    val sortedIterator = ctx.addMutableState("scala.collection.Iterator<UnsafeRow>", "sortedIter",
      forceInline = true)

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
       |   UnsafeRow $outputRow = (UnsafeRow)$sortedIterator.next();
       |   ${consume(ctx, null, outputRow)}
       |   if (shouldStop()) return;
       | }
   """.stripMargin.trim
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    s"""
       |${row.code}
       |$sorterVariable.insertRow((UnsafeRow)${row.value});
   """.stripMargin
  }

  /**
   * In SortExec, we overwrites cleanupResources to close UnsafeExternalRowSorter.
   */
  override protected[sql] def cleanupResources(): Unit = {
    if (rowSorter != null) {
      // There's possible for rowSorter is null here, for example, in the scenario of empty
      // iterator in the current task, the downstream physical node(like SortMergeJoinExec) will
      // trigger cleanupResources before rowSorter initialized in createSorter.
      rowSorter.cleanupResources()
    }
    super.cleanupResources()
  }

  override protected def withNewChildInternal(newChild: SparkPlan): RadixSortExec =
    copy(child = newChild)

  override def supportCodegen: Boolean = false
}
