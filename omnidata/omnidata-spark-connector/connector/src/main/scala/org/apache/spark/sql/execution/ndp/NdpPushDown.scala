/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
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

package org.apache.spark.sql.execution.ndp

import org.apache.derby.vti.Restriction.AND

import java.util.{Locale, Properties}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{PushDownManager, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BinaryExpression, Expression, NamedExpression, PredicateHelper, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Partial, PartialMerge}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, GlobalLimitExec, LeafExecNode, LocalLimitExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.hive.HiveSimpleUDF
import org.apache.hadoop.hive.ql.exec.DefaultUDFMethodResolver
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.FilterEstimation

import scala.collection.{JavaConverters, mutable}
import scala.reflect.runtime.universe

case class NdpPushDown(sparkSession: SparkSession)
  extends Rule[SparkPlan] with PredicateHelper {
  private val pushDownEnabled = NdpConf.getNdpEnabled(sparkSession)
  private var fpuHosts: scala.collection.Map[String, String] = _
  // filter performance blackList: like, startswith, endswith, contains
  private val filterWhiteList = Set("or", "and", "not", "equalto", "isnotnull", "lessthan",
    "greaterthan", "greaterthanorequal", "lessthanorequal", "in", "literal", "isnull",
    "attributereference")
  private val attrWhiteList = Set("long", "integer", "byte", "short", "float", "double",
    "boolean", "date", "decimal", "timestamp", "string")
  private val sparkUdfWhiteList = Set("substr", "substring", "length", "upper", "lower", "cast",
    "replace", "getarrayitem")
  private val udfPathWhiteList = Set("")
  private val udfWhitelistConf = NdpConf.getNdpUdfWhitelist(sparkSession)
  private val udfWhitelistProp = NdpConf.getNdpUdfWhitelist("udfname.txt")
  private val customUdfWhiteList = if (udfWhitelistConf.nonEmpty) {
    udfWhitelistConf.map(_.split(",")).get.toSet
  } else {
    Set.empty
  }
  private val udfWhiteList = sparkUdfWhiteList ++ customUdfWhiteList ++ udfWhitelistProp
  private val aggFuncWhiteList = Set("max", "min", "count", "avg", "sum")
  private val aggExpressionWhiteList =
    Set("multiply", "add", "subtract", "divide", "remainder", "literal", "attributereference")
  private val selectivityThreshold = NdpConf.getNdpFilterSelectivity(sparkSession)
  private val tableSizeThreshold = NdpConf.getNdpTableSizeThreshold(sparkSession)
  private val maxNdpTableSize = NdpConf.getMaxNdpTableSize(sparkSession)
  private val filterSelectivityEnable = NdpConf.getNdpFilterSelectivityEnable(sparkSession)
  private val tableFileFormatWhiteList = Set("orc", "parquet")
  private val parquetSchemaMergingEnabled = NdpConf.getParquetMergeSchema(sparkSession)
  private val timeOut = NdpConf.getNdpZookeeperTimeout(sparkSession)
  private val sizeofString = NdpConf.getSizeofString(sparkSession)
  private val parentPath = NdpConf.getNdpZookeeperPath(sparkSession)
  private val zkAddress = NdpConf.getNdpZookeeperAddress(sparkSession)

  override def apply(plan: SparkPlan): SparkPlan = {
    if (pushDownEnabled && shouldPushDown(plan) && shouldPushDown()) {
      pushDownOperator(plan)
    } else {
      plan
    }
  }

  def shouldPushDown(plan: SparkPlan): Boolean = {
    var isPush = false
    val p = plan.transformUp {
      case a: AdaptiveSparkPlanExec =>
        if (shouldPushDown(a.inputPlan)) {
          isPush = true
        }
        plan
      case s: FileSourceScanExec =>
        if (s.metadata.get("Location").toString.contains("[hdfs") ||
          s.metadata.get("Location").toString.contains("[cephrgw") ||
          s.metadata.get("Location").toString.contains("[s3a") ) {
          isPush = true
        }
        plan
    }
    if (parquetSchemaMergingEnabled) {
      isPush = false
    }
    isPush
  }

  def shouldPushDown(): Boolean = {
    val pushDownManagerClass = new PushDownManager()
    fpuHosts = pushDownManagerClass.getZookeeperData(timeOut, parentPath, zkAddress)
    fpuHosts.nonEmpty
  }

  def shouldPushDown(relation: HadoopFsRelation, output: Seq[Attribute]): Boolean = {
    val isSupportFormat = relation.fileFormat match {
      case source: DataSourceRegister =>
        tableFileFormatWhiteList.contains(source.shortName().toLowerCase(Locale.ROOT))
      case _ => false
    }
    var outputColumnTypeSize = 0
    var tableColumnTpyeSize = 0
    relation.dataSchema.map( r => {
      r.dataType.typeName match {
        case "string" => tableColumnTpyeSize += sizeofString
          logDebug(s"string  table column type size is : ${sizeofString}")
        case _ =>  tableColumnTpyeSize += r.dataType.defaultSize
          logDebug(s"table column type name is : ${r.dataType.typeName}")
          logDebug(s"other table column type size is : ${r.dataType.defaultSize}")
      }
    })
    output.map( r => {
      logDebug(s"first table column type name is : ${r.dataType.typeName}")
      r.dataType.typeName match {
        case "string" => outputColumnTypeSize += sizeofString
        case _ =>  outputColumnTypeSize += r.dataType.defaultSize
      }
    })
    val selectRatio = outputColumnTypeSize * 1.0 / tableColumnTpyeSize
    logDebug(s"Selected table size is : ${selectRatio * relation.sizeInBytes / 1024 / 1024}MB")
    logDebug(s"Max table size is : ${maxNdpTableSize / 1024 / 1024}MB")
    isSupportFormat && relation.sizeInBytes > tableSizeThreshold.toLong && selectRatio * relation.sizeInBytes < maxNdpTableSize
  }

  def shouldPushDown(f: FilterExec, scan: NdpSupport): Boolean = {
    scan.filterExeInfos.isEmpty &&
      f.subqueries.isEmpty &&
      f.output.forall(x => attrWhiteList.contains(x.dataType.typeName.split("\\(")(0))
        || supportedHiveStringType(x))
  }

  private def supportedHiveStringType(attr: Attribute): Boolean = {
    if (attr.dataType.typeName.equals("string")) {
      !attr.metadata.contains("HIVE_TYPE_STRING") ||
        attr.metadata.getString("HIVE_TYPE_STRING").startsWith("varchar") ||
        attr.metadata.getString("HIVE_TYPE_STRING").startsWith("char")
    } else {
      false
    }
  }

  def shouldPushDown(projectList: Seq[NamedExpression], s: NdpScanWrapper): Boolean = {
    s.scan.isPushDown && projectList.forall(_.isInstanceOf[AttributeReference])
  }

  def shouldPushDown(agg: BaseAggregateExec, scan: NdpSupport): Boolean = {
    scan.aggExeInfos.isEmpty &&
      agg.output.forall(x => attrWhiteList.contains(x.dataType.typeName)) &&
      agg.aggregateExpressions.forall{ e =>
      aggFuncWhiteList.contains(e.aggregateFunction.prettyName) &&
        (e.mode.equals(PartialMerge) || e.mode.equals(Partial)) &&
        !e.isDistinct &&
        e.aggregateFunction.children.forall { g =>
          aggExpressionWhiteList.contains(g.prettyName)
        }
    }
  }

  def shouldPushDown(scan: NdpSupport): Boolean = {
    scan.limitExeInfo.isEmpty
  }

  def filterSelectivityEnabled: Boolean = {
    filterSelectivityEnable &&
      sparkSession.conf.get(SQLConf.CBO_ENABLED) &&
      sparkSession.conf.get(SQLConf.PLAN_STATS_ENABLED)
  }

  def replaceWrapper(plan: SparkPlan): SparkPlan = {
    plan.transform {
      case s: NdpScanWrapper =>
        if (s.scan.isPushDown) {
          s.scan match {
            case f: FileSourceScanExec =>
              val scan = f.copy(output = s.scanOutput)
              scan.pushDown(s.scan)
              scan.fpuHosts(fpuHosts)
              logInfo(s"Push down with [${scan.ndpOperators}]")
              scan
            case _ => throw new UnsupportedOperationException()
          }
        } else {
          s.scan
        }
    }
  }

  def pushDownOperator(plan: SparkPlan): SparkPlan = {
    val p = pushDownOperatorInternal(plan)
    replaceWrapper(p)
  }

  def isDynamiCpruning(f: FilterExec): Boolean = {
    if(f.child.isInstanceOf[NdpScanWrapper] &&
      f.child.asInstanceOf[NdpScanWrapper].scan.isInstanceOf[FileSourceScanExec] ) {
      f.child.asInstanceOf[NdpScanWrapper].scan.asInstanceOf[FileSourceScanExec].partitionFilters
        .toString().contains("dynamicpruningexpression")
    } else {
      false
    }
  }

  def isUDFInWhiteList(expr: Expression): Boolean = {
    expr match {
      case be: BinaryExpression =>
        if (!isUDFInWhiteList(be.left)) false
        else isUDFInWhiteList(be.right)
      case ue: UnaryExpression =>
        isUDFInWhiteList(ue.child)
      case h: HiveSimpleUDF =>
        val defaultUDFMethodResolver = h.function.getResolver
        val runtimeMirror = universe.runtimeMirror(defaultUDFMethodResolver.getClass.getClassLoader)
        val udfTerm = universe.typeOf[DefaultUDFMethodResolver].decl(universe.TermName("udfClass")).asTerm
        val udfFiledMirror = runtimeMirror.reflect(defaultUDFMethodResolver).reflectField(udfTerm)
        val fullPathClassName = udfFiledMirror.get.toString.split(" ")(1)
        val packagePath = fullPathClassName.splitAt(fullPathClassName.lastIndexOf("."))._1
        udfPathWhiteList.contains(packagePath)
      case _ => false
    }
  }

  def pushDownOperatorInternal(plan: SparkPlan): SparkPlan = {
    val b = plan.logicalLink.get.stats

    val p = plan.transformUp {
      case a: AdaptiveSparkPlanExec =>
        pushDownOperatorInternal(a.inputPlan)
      case s: FileSourceScanExec if shouldPushDown(s.relation, s.output) =>
        val filters = s.partitionFilters.filter { x =>
          //TODO maybe need to adapt to the UDF whitelist.
          filterWhiteList.contains(x.prettyName) || udfWhiteList.contains(x.prettyName)
        }
        NdpScanWrapper(s, s.output, filters)
      case f @ FilterExec(condition, s: NdpScanWrapper) if shouldPushDown(f, s.scan) =>
        var selectivity2 : Option[Double] = Some(1.0)
        val bb = s.logicalLink.get.children.map{ x =>
          if (x.isInstanceOf[Filter]) {
            val cc = FilterEstimation(x.asInstanceOf[Filter]).calculateFilterSelectivity(x.asInstanceOf[Filter].condition)
            selectivity2 = cc
          }
        }
        // val selectivity2 = FilterEstimation(f.child.asInstanceOf[Filter]).calculateFilterSelectivity(condition)
        // val selectivity2 = FilterEstimation(f.child.logicalLink.get.children.asInstanceOf[Filter]).calculateFilterSelectivity(condition)
        if (filterSelectivityEnabled &&
          selectivity2.nonEmpty &&
          selectivity2.get > selectivityThreshold.toDouble) {
          logError(s"selectivity2 is : ${selectivity2.get}")
          logInfo(s"Fail to push down filter, since " +
            s"selectivity2[${selectivity2.get}] > threshold[${selectivityThreshold}] " +
            s"for condition[${condition}]")
          f
        } else if(isDynamiCpruning(f)){
          logInfo(s"Fail to push down filter, since ${s.scan.nodeName} contains dynamic pruning")
          f
        } else {
          // TODO: move selectivity info to pushdown-info
          if (filterSelectivityEnabled && selectivity2.nonEmpty) {
            logError(s"Selectivity: ${selectivity2.get}")
          }
          // partial pushDown
          val (otherFilters, pushDownFilters) =
            (splitConjunctivePredicates(condition) ++ s.partitionFilters).partition { x =>
              val containsUDFPath = isUDFInWhiteList(x)
              x.find { y =>
              !filterWhiteList.contains(y.prettyName) &&
                !udfWhiteList.contains(y.prettyName.toLowerCase) && !containsUDFPath
            }.isDefined
          }
          if (pushDownFilters.nonEmpty) {
            s.scan.pushDownFilter(FilterExeInfo(pushDownFilters.reduce(And), f.output))
          }
          s.update(f.output)
          if (otherFilters.nonEmpty) {
            FilterExec(otherFilters.reduce(And), s)
          } else {
            s
          }
        }
      case p @ ProjectExec(projectList, s: NdpScanWrapper) if shouldPushDown(projectList, s) =>
        s.update(p.output)
      case agg @ HashAggregateExec(_, _, _, _, _, _, s: NdpScanWrapper)
        if shouldPushDown(agg, s.scan) =>
        val execution = NdpSupport.toAggExecution(agg)
        s.scan.pushDownAgg(execution)
        s.update(agg.output)
      case agg @ HashAggregateExec(_, _, _, _, _, _, ProjectExec(projectList, s: NdpScanWrapper))
        if shouldPushDown(agg, s.scan) && shouldPushDown(projectList, s) =>
        val execution = NdpSupport.toAggExecution(agg)
        s.scan.pushDownAgg(execution)
        s.update(agg.output)
      case agg @ SortAggregateExec(_, _, _, _, _, _, s: NdpScanWrapper)
        if shouldPushDown(agg, s.scan) =>
        val execution = NdpSupport.toAggExecution(agg)
        s.scan.pushDownAgg(execution)
        s.update(agg.output)
      case agg @ SortAggregateExec(_, _, _, _, _, _, ProjectExec(projectList, s: NdpScanWrapper))
        if shouldPushDown(agg, s.scan) && shouldPushDown(projectList, s) =>
        val execution = NdpSupport.toAggExecution(agg)
        s.scan.pushDownAgg(execution)
        s.update(agg.output)
      case agg @ ObjectHashAggregateExec(_, _, _, _, _, _, s: NdpScanWrapper)
        if shouldPushDown(agg, s.scan) =>
        val execution = NdpSupport.toAggExecution(agg)
        s.scan.pushDownAgg(execution)
        s.update(agg.output)
      case agg @ ObjectHashAggregateExec(_, _, _, _, _, _, ProjectExec(pl, s: NdpScanWrapper))
        if shouldPushDown(agg, s.scan) && shouldPushDown(pl, s) =>
        val execution = NdpSupport.toAggExecution(agg)
        s.scan.pushDownAgg(execution)
        s.update(agg.output)
      case l @ GlobalLimitExec(limit, s: NdpScanWrapper) if shouldPushDown(s.scan) =>
        s.scan.pushDownLimit(LimitExeInfo(limit))
        s.update(l.output)
      case l @ LocalLimitExec(limit, s: NdpScanWrapper) if shouldPushDown(s.scan) =>
        s.scan.pushDownLimit(LimitExeInfo(limit))
        s.update(l.output)
    }
    replaceWrapper(p)
  }

}

case class NdpScanWrapper(
                           scan: NdpSupport,
                           var scanOutput: Seq[Attribute],
                           partitionFilters: Seq[Expression]) extends LeafExecNode {

  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

  override def output: Seq[Attribute] = scan.output

  def update(scanOutput: Seq[Attribute]): NdpScanWrapper = {
    this.scanOutput = scanOutput
    this
  }
}

object NdpConf {
  val NDP_ENABLED = "spark.sql.ndp.enabled"
  val PARQUET_MERGESCHEMA = "spark.sql.parquet.mergeSchema"
  val NDP_FILTER_SELECTIVITY_ENABLE = "spark.sql.ndp.filter.selectivity.enable"
  val NDP_TABLE_SIZE_THRESHOLD = "spark.sql.ndp.table.size.threshold"
  val MAX_NDP_TABLE_SIZE = "spark.sql.max.ndp.table.size"
  val NDP_ZOOKEEPER_TIMEOUT = "spark.sql.ndp.zookeeper.timeout"
  val NDP_SIZEOF_STRING = "spark.sql.ndp.sizeof.string"
  val NDP_ALIVE_OMNIDATA = "spark.sql.ndp.alive.omnidata"
  val NDP_FILTER_SELECTIVITY = "spark.sql.ndp.filter.selectivity"
  val NDP_UDF_WHITELIST = "spark.sql.ndp.udf.whitelist"
  val NDP_ZOOKEEPER_PATH = "spark.sql.ndp.zookeeper.path"
  val NDP_ZOOKEEPER_ADDRESS = "spark.sql.ndp.zookeeper.address"
  val NDP_SDI_PORT = "spark.sql.ndp.sdi.port"
  val NDP_GRPC_SSL_ENABLED = "spark.sql.ndp.grpc.ssl.enabled"
  val NDP_GRPC_CLIENT_CERT_FILE_PATH = "spark.sql.ndp.grpc.client.cert.file.path"
  val NDP_GRPC_CLIENT_PRIVATE_KEY_FILE_PATH = "spark.sql.ndp.grpc.client.private.key.file.path"
  val NDP_GRPC_TRUST_CA_FILE_PATH = "spark.sql.ndp.grpc.trust.ca.file.path"
  val NDP_PKI_DIR = "spark.sql.ndp.pki.dir"
  val NDP_MAX_FAILED_TIMES = "spark.sql.ndp.max.failed.times"
  val NDP_CLIENT_TASK_TIMEOUT = "spark.sql.ndp.task.timeout"

  def toBoolean(key: String, value: String, sparkSession: SparkSession): Boolean = {
    try {
      value.trim.toBoolean
    } catch {
      case _: IllegalArgumentException =>
        sparkSession.conf.unset(key)
        throw new IllegalArgumentException(s"NdpPushDown: $key should be boolean, but was $value")
    }
  }

  def toBoolean(key: String, value: String, conf: SQLConf): Boolean = {
    try {
      value.trim.toBoolean
    } catch {
      case _: IllegalArgumentException =>
        conf.unsetConf(key)
        throw new IllegalArgumentException(s"NdpPushDown: $key should be boolean, but was $value")
    }
  }

  def toNumber[T](key: String, value: String, converter: String => T,
                  configType: String, sparkSession: SparkSession): T = {
    try {
      converter(value.trim)
    } catch {
      case _: NumberFormatException =>
        sparkSession.conf.unset(key)
        throw new IllegalArgumentException(
          s"NdpPushDown: $key should be $configType, but was $value")
    }
  }

  def checkLongValue(key: String, value: Long, validator: Long => Boolean,
                     errorMsg: String, sparkSession: SparkSession) {
    if (!validator(value)) {
      sparkSession.conf.unset(key)
      throw new IllegalArgumentException(errorMsg)
    }
  }

  def checkDoubleValue(key: String, value: Double, validator: Double => Boolean,
                       errorMsg: String, sparkSession: SparkSession) {
    if (!validator(value)) {
      sparkSession.conf.unset(key)
      throw new IllegalArgumentException(errorMsg)
    }
  }

  def getNdpEnabled(sparkSession: SparkSession): Boolean = {
    toBoolean(NDP_ENABLED,
      sparkSession.conf.getOption(NDP_ENABLED).getOrElse("true"), sparkSession)
  }

  def getParquetMergeSchema(sparkSession: SparkSession): Boolean = {
    toBoolean(PARQUET_MERGESCHEMA,
      sparkSession.conf.getOption(PARQUET_MERGESCHEMA).getOrElse("false"), sparkSession)
  }

  def getNdpFilterSelectivityEnable(sparkSession: SparkSession): Boolean = {
    toBoolean(NDP_FILTER_SELECTIVITY_ENABLE,
      sparkSession.conf.getOption(NDP_FILTER_SELECTIVITY_ENABLE).getOrElse("true"), sparkSession)
  }

  def getNdpTableSizeThreshold(sparkSession: SparkSession): Long = {
    val result = toNumber(NDP_TABLE_SIZE_THRESHOLD,
      sparkSession.conf.getOption(NDP_TABLE_SIZE_THRESHOLD).getOrElse("10240"),
      _.toLong, "long", sparkSession)
    checkLongValue(NDP_TABLE_SIZE_THRESHOLD, result, _ > 0,
      s"The $NDP_TABLE_SIZE_THRESHOLD value must be positive", sparkSession)
    result
  }

  def getMaxNdpTableSize(sparkSession: SparkSession): Long = {
    val result = toNumber(MAX_NDP_TABLE_SIZE,
      sparkSession.conf.getOption(MAX_NDP_TABLE_SIZE).getOrElse("10240"),
      _.toLong, "long", sparkSession)
    checkLongValue(MAX_NDP_TABLE_SIZE, result, _ > 0,
      s"The $MAX_NDP_TABLE_SIZE value must be positive", sparkSession)
    result
  }

  def getNdpZookeeperTimeout(sparkSession: SparkSession): Int = {
    val result = toNumber(NDP_ZOOKEEPER_TIMEOUT,
      sparkSession.conf.getOption(NDP_ZOOKEEPER_TIMEOUT).getOrElse("5000"),
      _.toInt, "int", sparkSession)
    checkLongValue(NDP_ZOOKEEPER_TIMEOUT, result, _ > 0,
      s"The $NDP_ZOOKEEPER_TIMEOUT value must be positive", sparkSession)
    result
  }

  def getSizeofString(sparkSession: SparkSession): Int = {
    val result = toNumber(NDP_SIZEOF_STRING,
      sparkSession.conf.getOption(NDP_SIZEOF_STRING).getOrElse("20"),
      _.toInt, "int", sparkSession)
    checkLongValue(NDP_SIZEOF_STRING, result, _ > 0,
      s"The $NDP_SIZEOF_STRING value must be positive", sparkSession)
    result
  }

  def getNdpFilterSelectivity(sparkSession: SparkSession): Double = {
    val result = toNumber(NDP_FILTER_SELECTIVITY,
      sparkSession.conf.getOption(NDP_FILTER_SELECTIVITY).getOrElse("0.5"),
      _.toDouble, "double", sparkSession)
    checkDoubleValue(NDP_FILTER_SELECTIVITY, result,
      selectivity => selectivity >= 0.0 && selectivity <= 1.0,
      s"The $NDP_FILTER_SELECTIVITY value must be in [0.0, 1.0].", sparkSession)
    result
  }

  def getNdpUdfWhitelist(sparkSession: SparkSession): Option[String] = {
    sparkSession.conf.getOption(NDP_UDF_WHITELIST)
  }

  def getNdpZookeeperPath(sparkSession: SparkSession): String = {
    sparkSession.conf.getOption(NDP_ZOOKEEPER_PATH).getOrElse("/sdi/status")
  }

  def getNdpZookeeperAddress(sparkSession: SparkSession): String = {
    sparkSession.conf.getOption(NDP_ZOOKEEPER_ADDRESS).getOrElse("")
  }

  def getMaxFailedTimes(sparkSession: SparkSession): String = {
    sparkSession.conf.getOption(NDP_MAX_FAILED_TIMES).getOrElse("3")
  }

  def getTaskTimeout(sparkSession: SparkSession): Int = {
    val result = toNumber(NDP_CLIENT_TASK_TIMEOUT,
      sparkSession.conf.getOption(NDP_CLIENT_TASK_TIMEOUT).getOrElse("300"),
      _.toInt, "int", sparkSession)
    checkLongValue(NDP_CLIENT_TASK_TIMEOUT, result, _ > 0,
      s"The $NDP_CLIENT_TASK_TIMEOUT value must be positive", sparkSession)
    result
  }

  def getNdpUdfWhitelist(sourceName: String): mutable.Set[AnyRef] = {
    val prop = new Properties()
    val inputStream = this.getClass.getResourceAsStream("/"+sourceName)
    if (inputStream == null){
      inputStream.close()
      mutable.Set("")
    } else {
      prop.load(inputStream)
      inputStream.close()
      JavaConverters.asScalaSetConverter(prop.keySet()).asScala
    }
  }
}
