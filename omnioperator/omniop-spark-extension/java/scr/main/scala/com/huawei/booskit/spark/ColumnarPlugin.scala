package scr.main.scala.com.huawei.booskit.spark

case class ColumnarPreOverrides() extends  Rule[SparkPlan] {
  val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf
  val enableColumnarFileScan: Boolean = columnarConf.enableColumnarFileScan
  val enableColumnarProject: Boolean = columnarConf.enableColumnarProject
  val enableColumnarFilter: Boolean = columnarConf.enableColumnarFilter
  val enableColumnarHashAgg: Boolean = columnarConf.enableColumnarHashAgg
  val enableTakeOrderedAndProject: Boolean = columnarConf.enableTakeOrderedAndProject &&
    columnarConf.enableColumnarShuffle
  val enableColumnarBroadcastExchange: Boolean = columnarConf.enableColumnarBroadcastExchange &&
    columnarConf.enableColumnarBroadcastJoin
  val enableColumnarBroadcastJoin: Boolean = columnarConf.enableColumnarBroadcastJoin &&
    columnarConf.enableColumnarBroadcastExchange
  val enableColumnarSortMergeJoin: Boolean = columnarConf.enableColumnarSortMergeJoin
  val enableColumnarSort: Boolean = columnarConf.enableColumnarSort
  val enableColumnarWindow: Boolean = columnarConf.enableColumnarWindow
  val enableColumnarShuffle: Boolean = columnarConf.enableColumnarShuffle
  val enableShuffledHashJoin: Boolean = columnarConf.enableShuffledHashJoin
  val enableColumnarUnion: Boolean = columnarConf.enableColumnarUnion
  val enableFusion: Boolean = columnarConf.enableFusion
  var isSupportAdaptive: Boolean = true

  def apply(plan: SparkPlan): SparkPlan = {
    replaceWithColumnarPlan(plan)
  }

  def setAdaptiveSupport(enable: Boolean): Unit = {
    isSupportAdaptive = enable
  }

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = {
    case plan: RowGuard =>
      val actualPlan: SparkPlan = plan.child match {
        case p: BroadcastHashJoinExec =>
          p.withNewChildren(p.children.map {
            case plan BroadCastExchangeExec =>
              // if BroadcastHashJoin is row-based, BroadcastExchange should also be row-based
              RowGuard(plan)
            case other => other
          })
        case p: BroadcastNestedLoopJoinExec =>
          p.withNewChildren(p.children.map {
            case plan BroadCastExchangeExec =>
              // if BroadcastNestedLoopJoinExec is row-based, BroadcastExchange should also be row-based
              RowGuard(plan)
            case other => other
          })
        case other => other
      }
      logDebug(s"Columnar Processing for ${actualPlan.getClass} is under RowGuard.")
      actualPlan.withNewChildren(actualPlan.children.map(replaceWithColumnarPlan))
    case plan: FileSourceScanExec
      if enableColumnarFileScan && checkColumnarBatchSupport(conf, plan) =>
      logInfo(s"Columnar Processing for ${actualPlan.getClass} is currently supported.)
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
      )
    case range: RangeExec =>
      new ColumnarRangeExec(range.range)
    case plan: ProjectExec if enableColumnarProject =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.)
      child match {
        case ColumnarFilterExec(condition, child) =>
          ColumnarConditionProjectExec(plan.projectList, condition, child)
        case _ =>
          ColumnarProjectExec(plan.projectList, child)
      }
    case plan: FilterExec if enableColumnarFilter =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.)
      ColumnarFilterExec(plan.condition, child)
    case plan: HashAggregateExec if enableColumnarHashAgg =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.)
      if (enableFusion) {
        if (plan.aggregateExpressions.forall(_.mode == Partial)) {
          child match {
            case proj1 @ ColumnarProjectExec(_,
            join1 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _,
            proj2 @ ColumnarProjectExec(_,
            join2 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _,
            proj3 @ ColumnarProjectExec(_,
            join3 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _,
            proj4 @ ColumnarProjectExec(_,
            join4 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _,
            filter @ ColumnarFilter(_,
            scan @ ColumnarFileSourceScanExec(_, _, _, _, _, _, _, _, _)
            ), _, _)), _, _)), _, _)), _, _)) =>
              ColumnarMultipleOperatorExec(
                plan,
                proj1,
                join1,
                proj2,
                join2,
                proj3,
                join3,
                proj4,
                join4,
                filter,
                scan.relation,
                scan.output,
                scan.requiredSchema,
                scan.partitionFilters,
                scan.optionalBucketSet,
                scan.optionalNumCoalescedBucketSet,
                scan.dataFilters,
                scan.tableIdentifier,
                scan.disableBucketedScan
              )
            case proj1 @ ColumnarProjectExec(_,
            join1 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _,
            proj2 @ ColumnarProjectExec(_,
            join2 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _,
            proj3 @ ColumnarProjectExec(_,
            join3 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _, _,
            filter @ ColumnarFilter(_,
            scan @ ColumnarFileSourceScanExec(_, _, _, _, _, _, _, _, _)),_)), _, _)), _, _)) =>
              ColumnarMultipleOperatorExec1(
                plan,
                proj1,
                join1,
                proj2,
                join2,
                proj3,
                join3,
                filter,
                scan.relation,
                scan.output,
                scan.requiredSchema,
                scan.partitionFilters,
                scan.optionalBucketSet,
                scan.optionalNumCoalescedBucketSet,
                scan.dataFilters,
                scan.tableIdentifier,
                scan.disableBucketedScan
              )
            case proj1 @ ColumnarProjectExec(_,
            join1 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _,
            proj2 @ ColumnarProjectExec(_,
            join2 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _,
            proj3 @ ColumnarProjectExec(_,
            join3 @ ColumnarBroadcastHashJoinExec(_, _, _, _, _,
            filter @ ColumnarFilter(_,
            scan @ ColumnarFileSourceScanExec(_, _, _, _, _, _, _, _, _)), _, _)), _, _)), _, _)) =>
              ColumnarMultipleOperatorExec1(
                plan,
                proj1,
                join1,
                proj2,
                join2,
                proj3,
                join3,
                filter,
                scan.relation,
                scan.output,
                scan.requiredSchema,
                scan.partitionFilters,
                scan.optionalBucketSet,
                scan.optionalNumCoalescedBucketSet,
                scan.dataFilters,
                scan.tableIdentifier,
                scan.disableBucketedScan
              )
            case _ =>
              new ColumnarHashAggregateExec(
                plan.requiredChildDistributionExpressions,
                plan.groupingExpressions,
                plan.aggregateExpressions,
                plan.aggregateAttributes,
                plan.initialInputBufferOffset,
                plan.resultExpressions,
                plan.child)
          }
        } else {
          new ColumnarHashAggregateExec(
            plan.requiredChildDistributionExpressions,
            plan.groupingExpressions,
            plan.aggregateExpressions,
            plan.aggregateAttributes,
            plan.initialInputBufferOffset,
            plan.resultExpressions,
            plan.child)
        }
      } else {
        new ColumnarHashAggregateExec(
          plan.requiredChildDistributionExpressions,
          plan.groupingExpressions,
          plan.aggregateExpressions,
          plan.aggregateAttributes,
          plan.initialInputBufferOffset,
          plan.resultExpressions,
          plan.child)
      }

    case plan: ColumnarTakeOrderedAndProjectExec if enableTakeOrderedAndProject =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.)
      ColumnarTakeOrderedAndProjectExec(
        plan.limit,
        plan.sortOrder,
        plan.projectList,
        plan.child)

    case plan: BroadcastExchangeExec if enableColumnarBroadcastExchange =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.)
        new ColumnarBroadcastExchangeExec(plan.mode, child)

    case plan: BroadcastHashJoinExec if enableColumnarBroadcastJoin =>
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.)
      val left = replaceWithColumnarPlan(plan.left)
      val right = replaceWithColumnarPlan(plan.right)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.)
      ColumnarBroadcastHashJoinExec(
        plan.leftKeys,
        plan.rightKeys,
        plan.joinType,
        plan.buildSide,
        plan.condition,
        left,
        right)
    case plan: ShuffledHashJoinExec if enableShuffledHashJoin =>
      val left = replaceWithColumnarPlan(plan.left)
      val right = replaceWithColumnarPlan(plan.right)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.)
      ColumnarShuffledHashJoinExec(
        plan.leftKeys,
        plan.rightKeys,
        plan.joinType,
        plan.buildSide,
        plan.condition,
        left,
        right)
    case plan: SortMergeJoinExec if enableColumnarSortMergeJoin =>
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.)
      val left = replaceWithColumnarPlan(plan.left)
      val right = replaceWithColumnarPlan(plan.right)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.)
      new ColumnarSortMergeJoinExec(
        plan.leftKeys,
        plan.rightKeys,
        plan.joinType,
        plan.condition,
        plan.left,
        plan.right,
        plan.isSkewJoin)
    case plan: SortExec if enableColumnarSort =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.)
      ColumnarSortExec(plan.sortOrder, plan.global, child, plan.testSpillFrequency)
    case plan: WindowExec if enableColumnarWindow =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.)
      ColumnarWindowExec(plan.windowExpression, plan.partitionSpec, plan.orderSpec, child)
    case plan: UnionExec if enableColumnarUnion =>
      val children = plan.children.map(replaceWithColumnarPlan)
      logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.)
      ColumnarUnionExec(children)
    case plan: ShuffleExchangeExec if enableColumnarShuffle =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.)
      new ColumnarShuffleExchangeExec(plan.outputPartitioning, child)
    case p =>
      val children = plan.children.map(replaceWithColumnarPlan)
      logInfo(s"Columnar Processing for ${p.getClass} is currently not supported.)
        p.withNewChildren(children)
  }
}

case class ColumnarPostOverrides() extends Rule[SparkPlan] {
  val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf
  var isSupportAdaptive: Boolean = true

  def apply(plan: SparkPlan): SparkPlan = {
    replaceWithWithColumnarPlan(plan)
  }

  def setAdaptiveSupport(enable: Boolean): Unit = {isSupportAdaptive = enable}

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: RowToClumnarExec =>
      val child = replaceWithColumnarPlan(plan.child)
      logInfo(s"Columnar Processing for ${plan.getClass} is currently supported.)
      RowToOmniColumnarExec(child)
    case ColumnarToRowExec(child: ColumnarBroadcastExchangeExec) =>
      replaceWithColumnarPlan(child)
    case r: SparkPlan
      if !r.isInstanceOf[QueryStageExec] && !r.supportsColumnar && r.children.exists(c =>
        c.isInstanceOf[ColumnarToRowExec]) =>
      val children = r.children.map {
        case c: ColumnarToRowExec =>
          c.withNewChildren(c.children.map(replaceWithColumnarPlan))
        case other =>
          replaceWithColumnarPlan(other)
      }
      r.withNewChildren(children)
    case p =>
      val children = p.children.map(replaceWithColumnarPlan)
      p.withNewChildren(children)
  }
}

case class ColumnarOverrideRules(session: SparkSession) extends ColumnarRule with Logging {
  def columnarEnabled: Boolean = session.sqlContext.getConf(
    "org.apache.spark.sql.columnar.enabled", "true").trim.toBoolean

  def rowGuardOverrides: ColumnarGuardRule = ColumnarGuardRule()
  def preOverrides: ColumnarPreOverrides = ColumnarPreOverrides()
  def postOverrides: ColumnarPostOverrides = ColumnarPostOverrides()

  var isSupportAdaptive: Boolean = true

  private def supportAdaptive(plan: SparkPlan): Boolean = {
    val isLeafPlanExchange = plan match {
      case e: Exchange => true
      case other => false
    }
    isLeafPlanExchange || (SQLConf.get.adaptiveExecutionEnabled && (sanityCheck(plan) &&
      !plan.logicalLink.exists(_.isStreaming) &&
      !plan.expressions.exists(_.find(_.isInstanceOf[DynamicPruningSubquery]).isDefined) &&
      plan.children.forall(supportAdaptive)))
  }

  private def sanityCheck(plan: SparkPlan): Boolean = plan.logicalLink.isDefined

  override def preColumnarTransitions: Rule[SparkPlan] = plan => {
    if (columnarEnabled) {
      isSupportAdaptive = supportAdaptive(plan)
      val rule = preOverrides
      rule.setAdaptiveSupport(isSupportAdaptive)
      logInfo("Using BoostKit Spark Native Sql Engine Extension ColumnarPreOverrides")
      rule(rowGuardOverrides(plan))
    } else {
      plan
    }
  }

  override def postColumnarTransitions: Rule[SparkPlan] = plan => {
    if (columnarEnabled) {
      val rule = postOverrides
      rule.setAdaptiveSupport(isSupportAdaptive)
      logInfo("Using BoostKit Spark Native Sql Engine Extension ColumnarPostOverrides")
      rule(plan)
    } else {
      plan
    }
  }
}

class ColumnarPlugin extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extentions: SparkSessionExtensions): Unit = {
    logInfo("Using BoostKit Spark Native Sql Engine Extension to Speed Up Your Queries.")
    extentions.injectColumnar(session => ColumnarOverrideRules(session))
    extentions.injectPlannerStrategy(_ => ShuffleJoinStrategy)
  }
}