package com.huawei.boostkit.omnioffload.spark

import com.huawei.boostkit.omnidata.spark.NdpConnectorUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Count, Sum}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, LogicalPlan, Project, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommandExec}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.ndp.NdpConf.getOptimizerPushDownThreshold
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

import java.net.URI
import scala.collection.JavaConverters

case class NdpOverrides() extends Rule[SparkPlan] {

  var numPartitions: Int = -1
  var pushDownTaskCount: Int = -1
  var isSMJ = false
  var isSort = false
  var hasCoalesce = false
  var hasShuffle = false

  def apply(plan: SparkPlan): SparkPlan = {
    preRuleApply(plan)
    val ruleList = Seq(CountReplaceRule)
    val afterPlan = ruleList.foldLeft(plan) { case (sp, rule) =>
      val result = rule.apply(sp)
      result
    }
    val optimizedPlan = replaceWithOptimizedPlan(afterPlan)
    val finalPlan = replaceWithScanPlan(optimizedPlan)
    postRuleApply(finalPlan)
    finalPlan
  }

  def preRuleApply(plan: SparkPlan): Unit = {
    numPartitions = SQLConf.get.getConfString("spark.omni.sql.ndpPlugin.coalesce.numPartitions",
      NdpConnectorUtils.getNdpNumPartitionsStr("10000")).toInt
    pushDownTaskCount = NdpConnectorUtils.getPushDownTaskTotal(getOptimizerPushDownThreshold(plan.session))
    if (CountReplaceRule.shouldReplaceCountOne(plan)) {
      pushDownTaskCount = NdpConnectorUtils.getCountTaskTotal(200)
      SQLConf.get.setConfString(SQLConf.FILES_MAX_PARTITION_BYTES.key,
        NdpConnectorUtils.getCountMaxPartSize("512MB"))
    }
    if (CountReplaceRule.shouldReplaceDistinctCount(plan)) {
      pushDownTaskCount = NdpConnectorUtils.getCountDistinctTaskTotal(2000)
    }
  }

  def postRuleApply(plan: SparkPlan): Unit = {
    if (isSMJ) {
      SQLConf.get.setConfString(SQLConf.FILES_MAX_PARTITION_BYTES.key,
        NdpConnectorUtils.getSMJMaxPartSize("536870912"))
    }
  }

  //now set task total number, we can use this number pushDown task in thread
  def replaceWithScanPlan(plan: SparkPlan): SparkPlan = {
    val p = plan.transformUp {
      case scan: FileSourceScanExec =>
        scan.setRuntimePushDownSum(pushDownTaskCount)
        if (hasCoalesce && !hasShuffle) {
          // without shuffle , coalesce num is task num
          scan.setRuntimePartSum(numPartitions)
        }
        scan
      case p => p
    }
    p
  }

  def replaceWithOptimizedPlan(plan: SparkPlan): SparkPlan = {
    val p = plan.transformUp {
      case shuffle: ShuffleExchangeExec =>
        hasShuffle = true
        shuffle
      case p@ColumnarSortExec(sortOrder, global, child, testSpillFrequency) if isRadixSortExecEnable(sortOrder) =>
        isSort = true
        RadixSortExec(sortOrder, global, child, testSpillFrequency)
      case p@SortExec(sortOrder, global, child, testSpillFrequency) if isRadixSortExecEnable(sortOrder) =>
        isSort = true
        RadixSortExec(sortOrder, global, child, testSpillFrequency)
      case p@DataWritingCommandExec(cmd, child) =>
        if (isSort || isVagueAndAccurateHd(child)) {
          p
        } else {
          hasCoalesce = true
          DataWritingCommandExec(cmd, CoalesceExec(numPartitions, child))
        }
      case p@ColumnarSortMergeJoinExec(_, _, joinType, _, _, _, _, projectList)
        if joinType.equals(LeftOuter) =>
        isSMJ = true
        numPartitions = NdpConnectorUtils.getSMJNumPartitions(5000)
        ColumnarSortMergeJoinExec(leftKeys = p.leftKeys, rightKeys = p.rightKeys, joinType = LeftAnti,
          condition = p.condition, left = p.left, right = p.right, isSkewJoin = p.isSkewJoin, projectList)
      case p@SortMergeJoinExec(_, _, joinType, _, _, _, _)
        if joinType.equals(LeftOuter) =>
        isSMJ = true
        numPartitions = NdpConnectorUtils.getSMJNumPartitions(5000)
        SortMergeJoinExec(leftKeys = p.leftKeys, rightKeys = p.rightKeys, joinType = LeftAnti, condition = p.condition,
          left = p.left, right = p.right, isSkewJoin = p.isSkewJoin)
      case p@ColumnarBroadcastHashJoinExec(_, _, joinType, _, _, _, _, _, projectList) if joinType.equals(LeftOuter) =>
        ColumnarBroadcastHashJoinExec(leftKeys = p.leftKeys, rightKeys = p.rightKeys,
          joinType = LeftAnti, buildSide = p.buildSide, condition = p.condition, left = p.left,
          right = p.right, isNullAwareAntiJoin = p.isNullAwareAntiJoin, projectList)
      case p@BroadcastHashJoinExec(_, _, joinType, _, _, _, _, _) if joinType.equals(LeftOuter) =>
        BroadcastHashJoinExec(leftKeys = p.leftKeys, rightKeys = p.rightKeys, joinType = LeftAnti,
          buildSide = p.buildSide, condition = p.condition, left = p.left, right = p.right,
          isNullAwareAntiJoin = p.isNullAwareAntiJoin)
      case p@ColumnarShuffledHashJoinExec(_, _, joinType, _, _, _, _, projectList)
        if joinType.equals(LeftOuter) =>
        ColumnarShuffledHashJoinExec(p.leftKeys, p.rightKeys, LeftAnti, p.buildSide, p.condition,
          p.left, p.right, projectList)
      case p@ShuffledHashJoinExec(_, _, joinType, _, _, _, _, isSkewJoin) if joinType.equals(LeftOuter) =>
        ShuffledHashJoinExec(p.leftKeys, p.rightKeys, LeftAnti, p.buildSide, p.condition, p.left, p.right, isSkewJoin)
      case p@FilterExec(condition, child: OmniColumnarToRowExec, selectivity) =>
        val childPlan = child.transform {
          case p@OmniColumnarToRowExec(child: NdpFileSourceScanExec) =>
            ColumnarToRowExec(FileSourceScanExec(child.relation,
              child.output,
              child.requiredSchema,
              child.partitionFilters,
              child.optionalBucketSet,
              child.optionalNumCoalescedBuckets,
              child.dataFilters,
              child.tableIdentifier,
              child.partitionColumn,
              child.disableBucketedScan))
          case p@OmniColumnarToRowExec(child: FileSourceScanExec) =>
            ColumnarToRowExec(child)
          case p => p
        }
        FilterExec(condition, childPlan, selectivity)
      case c1@OmniColumnarToRowExec(c2@ColumnarFilterExec(condition, c3: FileSourceScanExec)) =>
        numPartitions = NdpConnectorUtils.getOmniColumnarNumPartitions(1000)
        if (isAccurate(condition)) {
          pushDownTaskCount = NdpConnectorUtils.getOmniColumnarTaskCount(400)
        }
        FilterExec(condition, ColumnarToRowExec(c3))
      case p@FilterExec(condition, _, _) if isAccurate(condition) =>
        numPartitions = NdpConnectorUtils.getFilterPartitions(1000)
        pushDownTaskCount = NdpConnectorUtils.getFilterTaskCount(400)
        p
      case p@ColumnarConditionProjectExec(projectList, condition, child)
        if condition.toString().startsWith("isnull") && (child.isInstanceOf[ColumnarSortMergeJoinExec]
          || child.isInstanceOf[ColumnarBroadcastHashJoinExec] || child.isInstanceOf[ColumnarShuffledHashJoinExec]) =>
        ColumnarProjectExec(changeProjectList(projectList), child)
      case p@ProjectExec(projectList, filter: FilterExec)
        if filter.condition.toString().startsWith("isnull") && (filter.child.isInstanceOf[SortMergeJoinExec]
          || filter.child.isInstanceOf[BroadcastHashJoinExec] || filter.child.isInstanceOf[ShuffledHashJoinExec]) =>
        ProjectExec(changeProjectList(projectList), filter.child)
      case p: SortAggregateExec if p.child.isInstanceOf[OmniColumnarToRowExec]
        && p.child.asInstanceOf[OmniColumnarToRowExec].child.isInstanceOf[ColumnarSortExec]
        && isAggPartial(p.aggregateAttributes) =>
        val omniColumnarToRow = p.child.asInstanceOf[OmniColumnarToRowExec]
        val omniColumnarSort = omniColumnarToRow.child.asInstanceOf[ColumnarSortExec]
        SortAggregateExec(p.requiredChildDistributionExpressions,
          p.groupingExpressions,
          p.aggregateExpressions,
          p.aggregateAttributes,
          p.initialInputBufferOffset,
          p.resultExpressions,
          SortExec(omniColumnarSort.sortOrder,
            omniColumnarSort.global,
            ColumnarToRowExec(omniColumnarSort.child),
            omniColumnarSort.testSpillFrequency))
      case p: SortAggregateExec if p.child.isInstanceOf[OmniColumnarToRowExec]
        && p.child.asInstanceOf[OmniColumnarToRowExec].child.isInstanceOf[ColumnarSortExec]
        && isAggFinal(p.aggregateAttributes) =>
        val omniColumnarToRow = p.child.asInstanceOf[OmniColumnarToRowExec]
        val omniColumnarSort = omniColumnarToRow.child.asInstanceOf[ColumnarSortExec]
        val omniShuffleExchange = omniColumnarSort.child.asInstanceOf[ColumnarShuffleExchangeExec]
        val rowToOmniColumnar = omniShuffleExchange.child.asInstanceOf[RowToOmniColumnarExec]
        SortAggregateExec(p.requiredChildDistributionExpressions,
          p.groupingExpressions,
          p.aggregateExpressions,
          p.aggregateAttributes,
          p.initialInputBufferOffset,
          p.resultExpressions,
          SortExec(omniColumnarSort.sortOrder,
            omniColumnarSort.global,
            ShuffleExchangeExec(omniShuffleExchange.outputPartitioning, rowToOmniColumnar.child,
              omniShuffleExchange.shuffleOrigin),
            omniColumnarSort.testSpillFrequency))
      case p@OmniColumnarToRowExec(agg: ColumnarHashAggregateExec)
        if agg.groupingExpressions.nonEmpty && agg.child.isInstanceOf[ColumnarShuffleExchangeExec] =>
        val omniExchange = agg.child.asInstanceOf[ColumnarShuffleExchangeExec]
        val omniHashAgg = omniExchange.child.asInstanceOf[ColumnarHashAggregateExec]
        HashAggregateExec(agg.requiredChildDistributionExpressions,
          agg.groupingExpressions,
          agg.aggregateExpressions,
          agg.aggregateAttributes,
          agg.initialInputBufferOffset,
          agg.resultExpressions,
          ShuffleExchangeExec(omniExchange.outputPartitioning,
            HashAggregateExec(omniHashAgg.requiredChildDistributionExpressions,
              omniHashAgg.groupingExpressions,
              omniHashAgg.aggregateExpressions,
              omniHashAgg.aggregateAttributes,
              omniHashAgg.initialInputBufferOffset,
              omniHashAgg.resultExpressions,
              ColumnarToRowExec(omniHashAgg.child)),
            omniExchange.shuffleOrigin))
      case p => p
    }
    p
  }

  def isAggPartial(aggAttributes: Seq[Attribute]): Boolean = {
    aggAttributes.exists(x => x.name.equals("max") || x.name.equals("maxxx"))
  }

  def isAggFinal(aggAttributes: Seq[Attribute]): Boolean = {
    aggAttributes.exists(x => x.name.contains("avg(cast"))
  }

  def isVagueAndAccurateHd(child: SparkPlan): Boolean = {
    var result = false
    child match {
      case filter: FilterExec =>
        filter.child match {
          case columnarToRow: ColumnarToRowExec =>
            if (columnarToRow.child.isInstanceOf[FileSourceScanExec]) {
              filter.condition.foreach { x =>
                if (x.isInstanceOf[StartsWith] || x.isInstanceOf[EndsWith] || x.isInstanceOf[Contains]) {
                  result = true
                }
                x match {
                  case literal: Literal if literal.value.toString.startsWith("153") =>
                    result = true
                  case _ =>
                }
              }
            }
          case _ =>
        }
      case _ =>
    }
    result
  }

  def isAccurate(condition: Expression): Boolean = {
    var result = false
    condition.foreach {
      case literal: Literal if literal.value.toString.startsWith("000") =>
        result = true
      case _ =>
    }
    result
  }

  def changeProjectList(projectList: Seq[NamedExpression]): Seq[NamedExpression] = {
    val p = projectList.map {
      case exp: Alias =>
        Alias(Literal(null, exp.dataType), exp.name)(
          exprId = exp.exprId,
          qualifier = exp.qualifier,
          explicitMetadata = exp.explicitMetadata,
          nonInheritableMetadataKeys = exp.nonInheritableMetadataKeys
        )
      case exp => exp
    }
    p
  }

  def hasComplementOperator(p: SparkPlan): Boolean = {
    var result = false
    var hasFilterAtFormerOp = false
    p.transformDown {
      case f: FilterExec =>
        if (f.condition.toString().startsWith("isnull")) {
          hasFilterAtFormerOp = true
        } else {
          hasFilterAtFormerOp = false
        }
        f
      case j: BaseJoinExec =>
        if (hasFilterAtFormerOp) {
          result = true
        }
        j
      case o =>
        hasFilterAtFormerOp = false
        o
    }
    result
  }

  def isRadixSortExecEnable(sortOrder: Seq[SortOrder]): Boolean = {
    sortOrder.length == 2 &&
      sortOrder.head.dataType == LongType &&
      sortOrder(1).dataType == LongType &&
      SQLConf.get.getConfString("spark.omni.sql.ndpPlugin.radixSort.enabled", "true").toBoolean
  }
}

case class NdpRules(session: SparkSession) extends ColumnarRule with Logging {

  def ndpOverrides: NdpOverrides = NdpOverrides()

  override def preColumnarTransitions: Rule[SparkPlan] = plan => {
    plan
  }

  override def postColumnarTransitions: Rule[SparkPlan] = plan => {
    if (NdpPluginEnableFlag.isEnable(plan.session)) {
      val rule = ndpOverrides
      rule(plan)
    } else {
      plan
    }
  }
}

case class NdpOptimizerRules(session: SparkSession) extends Rule[LogicalPlan] {

  val SORT_REPARTITION_PLANS: Seq[String] = Seq(
    "Sort,HiveTableRelation",
    "Sort,LogicalRelation",
    "Sort,RepartitionByExpression,HiveTableRelation",
    "Sort,RepartitionByExpression,LogicalRelation",
    "Sort,Project,HiveTableRelation",
    "Sort,Project,LogicalRelation",
    "Sort,RepartitionByExpression,Project,HiveTableRelation",
    "Sort,RepartitionByExpression,Project,LogicalRelation"
  )

  val SORT_REPARTITION_SIZE: Int = SQLConf.get.getConfString(
    "spark.omni.sql.ndpPlugin.sort.repartition.size",
    NdpConnectorUtils.getSortRepartitionSizeStr("104857600")).toInt
  val DECIMAL_PRECISION: Int = SQLConf.get.getConfString(
    "spark.omni.sql.ndpPlugin.cast.decimal.precision",
    NdpConnectorUtils.getCastDecimalPrecisionStr("15")).toInt
  val MAX_PARTITION_BYTES_ENABLE_FACTOR: Int = SQLConf.get.getConfString(
    "spark.omni.sql.ndpPlugin.max.partitionBytesEnable.factor",
    NdpConnectorUtils.getNdpMaxPtFactorStr("2")).toInt


  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (NdpPluginEnableFlag.isEnable(session)) {
      repartition(FileSystem.get(session.sparkContext.hadoopConfiguration), plan)
      replaceWithOptimizedPlan(plan)
    } else {
      plan
    }
  }

  def replaceWithOptimizedPlan(plan: LogicalPlan): LogicalPlan = {
    plan.transformUp {
      case CreateHiveTableAsSelectCommand(tableDesc, query, outputColumnNames, mode)
        if isParquetEnable(tableDesc)
          && SQLConf.get.getConfString("spark.omni.sql.ndpPlugin.parquetOutput.enabled", "true")
          .toBoolean =>
        CreateDataSourceTableAsSelectCommand(
          tableDesc.copy(provider = Option("parquet")), mode, query, outputColumnNames)
      case a@Aggregate(groupingExpressions, aggregateExpressions, _)
        if SQLConf.get.getConfString("spark.omni.sql.ndpPlugin.castDecimal.enabled", "true")
          .toBoolean =>
        var ifCast = false
        if (groupingExpressions.nonEmpty && hasCount(aggregateExpressions)) {
          SQLConf.get.setConfString(SQLConf.FILES_MAX_PARTITION_BYTES.key,
            NdpConnectorUtils.getCountAggMaxFilePtBytesStr("1024MB"))
        } else if (groupingExpressions.nonEmpty && hasAvg(aggregateExpressions)) {
          SQLConf.get.setConfString(SQLConf.FILES_MAX_PARTITION_BYTES.key,
            NdpConnectorUtils.getAvgAggMaxFilePtBytesStr("256MB"))
          ifCast = true
        }
        if (ifCast) {
          a.copy(aggregateExpressions = aggregateExpressions
            .map(castSumAvgToBigInt)
            .map(_.asInstanceOf[NamedExpression]))
        }
        else {
          a
        }
      case j@Join(_, _, Inner, condition, _) =>
        // turnOffOperator()
        // 6-x-bhj
        SQLConf.get.setConfString(SQLConf.FILES_MAX_PARTITION_BYTES.key,
          NdpConnectorUtils.getBhjMaxFilePtBytesStr("512MB"))
        if (condition.isDefined) {
          condition.get match {
            case e@EqualTo(attr1: AttributeReference, attr2: AttributeReference) =>
              SQLConf.get.setConfString(SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key, "false")
              j.copy(condition = Some(And(EqualTo(Substring(attr1, Literal(8), Literal(11))
                , Substring(attr2, Literal(8), Literal(11))), e)))
            case _ => j
          }
        } else {
          j
        }
      case s@Sort(order, _, _) =>
        s.copy(order = order.map(e => e.copy(child = castStringExpressionToBigint(e.child))))
      case p => p
    }
  }

  def hasCount(aggregateExpressions: Seq[Expression]): Boolean = {
    aggregateExpressions.exists {
      case exp: Alias if (exp.child.isInstanceOf[AggregateExpression]
        && exp.child.asInstanceOf[AggregateExpression].aggregateFunction.isInstanceOf[Count]) => true
      case _ => false
    }
  }

  def hasAvg(aggregateExpressions: Seq[Expression]): Boolean = {
    aggregateExpressions.exists {
      case exp: Alias if (exp.child.isInstanceOf[AggregateExpression]
        && exp.child.asInstanceOf[AggregateExpression].aggregateFunction.isInstanceOf[Average]) => true
      case _ => false
    }
  }

  def isParquetEnable(tableDesc: CatalogTable): Boolean = {
    if (tableDesc.provider.isEmpty || tableDesc.provider.get.equals("hive")) {
      if (tableDesc.storage.outputFormat.isEmpty
        || tableDesc.storage.serde.get.equals("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")) {
        return true
      }
    }
    false
  }

  def repartition(fs: FileSystem, plan: LogicalPlan): Unit = {
    var tables = Seq[URI]()
    var planContents = Seq[String]()
    var maxPartitionBytesEnable = true
    var existsProject = false
    var existsTable = false
    var existsAgg = false

    plan.foreach {
      case p@HiveTableRelation(tableMeta, _, _, _, _) =>
        if (tableMeta.storage.locationUri.isDefined) {
          tables :+= tableMeta.storage.locationUri.get
        }
        existsTable = true
        planContents :+= p.nodeName
      case p@LogicalRelation(_, _, catalogTable, _) =>
        if (catalogTable.isDefined && catalogTable.get.storage.locationUri.isDefined) {
          tables :+= catalogTable.get.storage.locationUri.get
        }
        existsTable = true
        planContents :+= p.nodeName
      case p: Project =>
        maxPartitionBytesEnable &= (p.output.length * MAX_PARTITION_BYTES_ENABLE_FACTOR < p.inputSet.size)
        existsProject = true
        planContents :+= p.nodeName
      case p: Aggregate =>
        maxPartitionBytesEnable = true
        existsProject = true
        existsAgg = true
        planContents :+= p.nodeName
      case p =>
        planContents :+= p.nodeName
    }

    // agg shuffle partition 200 ,other 5000
    if (existsTable && existsAgg) {
      // SQLConf.get.setConfString(SQLConf.FILES_MAX_PARTITION_BYTES.key, "536870912")
      SQLConf.get.setConfString(SQLConf.SHUFFLE_PARTITIONS.key,
        NdpConnectorUtils.getAggShufflePartitionsStr("200"))
    } else {
      SQLConf.get.setConfString(SQLConf.SHUFFLE_PARTITIONS.key,
        NdpConnectorUtils.getShufflePartitionsStr("5000"))
    }
    repartitionShuffleForSort(fs, tables, planContents)
    repartitionHdfsReadForDistinct(fs, tables, plan)
  }

  def repartitionShuffleForSort(fs: FileSystem, tables: Seq[URI], planContents: Seq[String]): Unit = {
    if (!SQLConf.get.getConfString("spark.omni.sql.ndpPlugin.radixSort.enabled", "true").toBoolean) {
      return
    }

    val planContent = planContents.mkString(",")
    if (tables.length == 1
      && SORT_REPARTITION_PLANS.exists(planContent.contains(_))) {
      val partitions = Math.max(1, fs.getContentSummary(new Path(tables.head)).getLength / SORT_REPARTITION_SIZE)
      SQLConf.get.setConfString(SQLConf.SHUFFLE_PARTITIONS.key,
        NdpConnectorUtils.getSortShufflePartitionsStr("1000"))
      // SQLConf.get.setConfString("spark.shuffle.sort.bypassMergeThreshold", "1000")
      turnOffOperator()
    }
  }

  def repartitionHdfsReadForDistinct(fs: FileSystem, tables: Seq[URI], plan: LogicalPlan): Unit = {
    if (!SQLConf.get.getConfString("spark.omni.sql.ndpPlugin.distinct.enabled", "true").toBoolean) {
      return
    }
    if (tables.length != 1) {
      return
    }

    plan.foreach {
      case Aggregate(groupingExpressions, aggregateExpressions, _) if groupingExpressions == aggregateExpressions =>
        SQLConf.get.setConfString(SQLConf.FILES_MAX_PARTITION_BYTES.key,
          NdpConnectorUtils.getGroupMaxFilePtBytesStr("1024MB"))
        return
      case _ =>
    }
  }

  def castSumAvgToBigInt(expression: Expression): Expression = {
    val exp = expression.transform {
      case agg@Average(cast: Cast, _) if cast.dataType.isInstanceOf[DoubleType] =>
        Average(Cast(cast.child, DataTypes.LongType), agg.failOnError)
      case agg@Sum(cast: Cast, _) if cast.dataType.isInstanceOf[DoubleType] =>
        Sum(Cast(cast.child, DataTypes.LongType), agg.failOnError)
      case e =>
        e
    }
    var finalExp = exp
    exp match {
      case agg: Alias if agg.child.isInstanceOf[AggregateExpression]
        && agg.child.asInstanceOf[AggregateExpression].aggregateFunction.isInstanceOf[Sum] =>
        finalExp = Alias(Cast(agg.child, DataTypes.DoubleType), agg.name)(
          exprId = agg.exprId,
          qualifier = agg.qualifier,
          explicitMetadata = agg.explicitMetadata,
          nonInheritableMetadataKeys = agg.nonInheritableMetadataKeys
        )
      case _ =>
    }
    finalExp
  }

  def castStringExpressionToBigint(expression: Expression): Expression = {
    expression match {
      case a@AttributeReference(_, DataTypes.StringType, _, _) =>
        Cast(a, DataTypes.LongType)
      case e => e
    }
  }


  def turnOffOperator(): Unit = {
    session.sqlContext.setConf("org.apache.spark.sql.columnar.enabled", "false")
    session.sqlContext.setConf("spark.sql.join.columnar.preferShuffledHashJoin", "false")
  }
}

class ColumnarPlugin extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(session => NdpRules(session))
    extensions.injectOptimizerRule(session => NdpOptimizerRules(session))
  }
}

object NdpPluginEnableFlag {

  def isMatchedIpAddress: Boolean = {
    val ipSet = Set("90.90.57.114", "90.90.59.122")
    val hostAddrSet = JavaConverters.asScalaSetConverter(NdpConnectorUtils.getIpAddress).asScala
    val res = ipSet & hostAddrSet
    res.nonEmpty
  }

  def isEnable(session: SparkSession): Boolean = {
    def ndpEnabled: Boolean = session.sqlContext.getConf(
      "spark.omni.sql.ndpPlugin.enabled", "true").trim.toBoolean

    ndpEnabled && (isMatchedIpAddress || NdpConnectorUtils.getNdpEnable.equals("true"))
  }

  def isEnable(): Boolean = {
    def ndpEnabled: Boolean = sys.props.getOrElse(
      "spark.omni.sql.ndpPlugin.enabled", "true").trim.toBoolean

    ndpEnabled && (isMatchedIpAddress || NdpConnectorUtils.getNdpEnable.equals("true"))
  }
}
