package scr.main.scala.com.huawei.booskit.spark

case class RowGuard(child: SparkPlan) extends SparkPlan {
  def output: Seq[Attribute] = child.output
  protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException
  }
  def children: Seq[SparkPlan] = Seq(child)
}

case class ColumnarGuardRule() extends Rule[SparkPlan]{
  val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf
  val preferColumnar: Boolean = columnarConf.enablePreferColumnar
  val enableColumnarShuffle: Boolean = columnarConf.enableColumnarShuffle
  val enableColumnarSort: Boolean = columnarConf.enableColumnarSort
  val enableTakeOrderedAndProject: Boolean = columnarConf.enableTakeOrderedAndProject &&
    columnarConf.enableColumnarShuffle
  val enableColumnarUnion: Boolean = columnarConf.enableColumnarUnion
  val enableColumnarWindow: Boolean = columnarConf.enableColumnarWindow
  val enableColumnarHashAgg: Boolean = columnarConf.enableColumnarHashAgg
  val enableColumnarProject: Boolean = columnarConf.enableColumnarProject
  val enableColumnarFilter: Boolean = columnarConf.enableColumnarFilter
  val enableColumnarBroadcastExchange: Boolean = columnarConf.enableColumnarBroadcastExchange &&
    columnarConf.enableColumnarBroadcastJoin
  val enableColumnarBroadcastJoin: Boolean = columnarConf.enableColumnarBroadcastJoin
  val enableColumnarSortMergeJoin: Boolean = columnarConf.enableColumnarSortMergeJoin
  val enableShuffledHashJoin: Boolean = columnarConf.enableShuffledHashJoin
  val enableColumnarFileScan: Boolean = columnarConf.enableColumnarFileScan
  var optimizeLevel: Int = columnarConf.joinOptimizationThrottle

  private def tryConvertToColumnar(plan: SparkPlan): Boolean = {
    try {
      val columnarPlan = plan match {
        case plan: FileSourceScanExec =>
          if (!checkColumnarBatchSupport(conf, plan)) {
            return false
          }
          if (!enableColumnarFileScan) return false
          ColumnarFileSourceScanExec(
            plan.relation,
            plan.output,
            plan.requiredSchema,
            plan.partitionFilters,
            plan.optionalBucketSet,
            plan.optionalNumCoalescedBucketSet,
            plan.dataFilters,
            plan.tableIdentifier,
            plan.disableBucketedScan
          ).buildCheck()
        case plan: ProjectExec =>
          if (!enableColumnarProject) return false
          ColumnarProjectExec(plan.projectList, plan.child).buildCheck()
        case plan: FilterExec =>
          if (!enableColumnarFilter) return false
          ColumnarFilterExec(plan.condition, plan.child).buildCheck()
        case plan: HashAggregateExec =>
          if (!enableColumnarHashAgg) return false
          new ColumnarHashAggregateExec(
            plan.requiredChildDistributionExpressions,
            plan.groupingExpressions,
            plan.aggregateExpressions,
            plan.aggregateAttributes,
            plan.initialInputBufferOffset,
            plan.resultExpressions,
            plan.child).buildCheck()
        case plan: SortExec =>
          if (!enableColumnarSort) return false
          ColumnarSortExec(plan.sortOrder, plan.global,
            plan.child, plan.testSpillFrequency).buildCheck()
        case plan: BroadcastExchangeExec =>
          if (!enableColumnarBroadcastExchange) return false
          new ColumnarBroadcastExchangeExec(plan.mode, plan.child)
        case plan: ColumnarTakeOrderedAndProjectExec
          if (!enableTakeOrderedAndProject) {
            ColumnarTakeOrderedAndProjectExec(
              plan.limit,
              plan.sortOrder,
              plan.projectList,
              plan.child).buildCheck()
          }
        case plan: Union =>
          if (!enableColumnarUnion) return false
          ColumnarUnionExec(plan.children).buildCheck()
        case plan: ShuffleExchangeExec =>
          if (!enableColumnarShuffle) return false
          new ColumnarShuffleExchangeExec(plan.outputPartitioning, plan.child).buildCheck()
        case plan: BroadcastHashJoinExec =>
          // We need to check if BroadcastExchangeExec can be converted to columnar-based.
          // If not, BHJ should also be row-based.
          if (!enableColumnarBroadcastJoin) return false
          val left = plan.left
          left match {
            case exec: BroadcastExchangeExec =>
              new ColumnarBroadcastExchangeExec(exec.mode, exec.child)
            case BroadcastQueryStageExec(_, plan: BroadcastExchangeExec) =>
              new ColumnarBroadcastExchangeExec(plan.mode, plan.child)
            case BroadcastQueryStageExec(_, plan: ReusedExchangeExec) =>
              plan match {
                case ReusedExchangeExec(_, b: BroadcastExchangeExec) =>
                  new ColumnarBroadcastExchangeExec(b.mode, b.child)
                case _ =>
              }
            case _ =>
          }
          val right = plan.right
          right match {
            case exec: BroadcastExchangeExec =>
              new ColumnarBroadcastExchangeExec(exec.mode, exec.child)
            case BroadcastQueryStageExec(_, plan: BroadcastExchangeExec) =>
              new ColumnarBroadcastExchangeExec(plan.mode, plan.child)
            case BroadcastQueryStageExec(_, plan: ReusedExchangeExec) =>
              plan match {
                case ReusedExchangeExec(_, b: BroadcastExchangeExec) =>
                  new ColumnarBroadcastExchangeExec(b.mode, b.child)
                case _ =>
              }
            case _ =>
          }
          ColumnarBroadcastHashJoinExec(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            plan.buildSide,
            plan.condition,
            plan.left,
            plan.right).buildCheck()
        case plan: SortMergeJoinExec =>
          if (!enableColumnarSortMergeJoin) return false
          new ColumnarSortMergeJoinExec(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            plan.condition,
            plan.left,
            plan.right,
            plan.isSkewJoin).buildCheck()
        case plan: WindowExec =>
          if (!enableColumnarWindow) return false
          ColumnarWindowExec(plan.windowExpression, plan.partitionSpec,
            plan.orderSpec, plan.child).buildCheck()
        case plan: ShuffledHashJoinExec =>
          if (!enableShuffledHashJoin) return false
          ColumnarShuffledHashJoinExec(
            plan.leftKeys,
            plan.rightKeys,
            plan.joinType,
            plan.buildSide,
            plan.condition,
            plan.left,
            plan.right).buildCheck()
        case plan: BroadcastNestedLoopJoinExec => return false
        case p =>
          p
      }
    } catch {
      case e: UnsupportedOperationException =>
        logDebug(s"[OPERATOR FALLBACK] ${e} ${plan.getClass} falls back to Spark operator")
        return false
      case _: RuntimeException =>
        logDebug(s"[OPERATOR FALLBACK] ${plan.getClass} falls back to Spark operator")
        return false
      case _: Throwable =>
        logDebug(s"[OPERATOR FALLBACK] ${plan.getClass} falls back to Spark operator")
        return false
    }
    true
  }

  private def existsMultiCodegens(plan: SparkPlan, count: Int = 0): Boolean = {
    plan match {
      case plan: CodegenSupport if plan.supportCodegen =>
        if ((count + 1) >= optimizeLevel) return true
        plan.children.map(existsMultiCodegens(_, count + 1)).exists(_ == true)
      case plan: ShuffledHashJoinExec =>
        if ((count + 1) >= optimizeLevel) return true
        plan.children.map(existsMultiCodegens(_, count + 1)).exists(_ == true)
      case other => false
    }
  }

  /*
   * Insert an InputAdapter on top of those that do not support codegen.
   */
  private def insertRowGuardRecursive(plan: SparkPlan): SparkPlan = {
    plan match {
      case p: ShuffleExchangeExec =>
        RowGuard(p.withNewChildren(p.children.map(insertRowGuardOrNot)))
      case p: BroadcastExchangeExec =>
        RowGuard(p.withNewChildren(p.children.map(insertRowGuardOrNot)))
      case p: ShuffledHashJoinExec =>
        RowGuard(p.withNewChildren(p.children.map(insertRowGuardRecursive)))
      case p: if !supportCodegen(p) =>
        // insert row guard them recursively
        p.withNewChildren(p.children.map(insertRowGuardOrNot))
      case p: CustomShuffleReaderExec =>
        p.withNewChildren(p.children.map(insertRowGuardOrNot))
      case p: BroadcastQueryStageExec =>
        p
      case p => RowGuard(p.withNewChildren(p.children.map(insertRowGuardRecursive)))
    }
  }

  private def insertRowGuard(plan: SparkPlan): SparkPlan = {
    RowGuard(plan.withNewChildren(plan.children.map(insertRowGuardOrNot)))
  }

  private def insertRowGuardOrNot(plan: SparkPlan): SparkPlan = {
    plan match {
      case plan if !preferColumnar && existsMultiCodegens(plan) =>
        insertRowGuardRecursive(plan)
      case plan if !tryConvertToColumnar(plan) =>
        insertRowGuard(plan)
      case p: BroadcastQueryStageExec =>
        p
      case other =>
        other.withNewChildren(p.children.map(insertRowGuardOrNot))
    }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    insertRowGuardOrNot(plan)
  }
}
