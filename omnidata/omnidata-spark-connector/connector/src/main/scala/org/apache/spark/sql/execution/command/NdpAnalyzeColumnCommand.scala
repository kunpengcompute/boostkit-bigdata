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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogColumnStat, CatalogStatistics, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan}
import org.apache.spark.sql.catalyst.util.{DateFormatter, TimestampFormatter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.time.ZoneOffset


/**
 * Analyzes the given columns of the given table to generate statistics, which will be used in
 * query optimizations. Parameter `allColumns` may be specified to generate statistics of all the
 * columns of a given table.
 */
case class NdpAnalyzeColumnCommand(
                                 tableIdent: TableIdentifier,
                                 columnNames: Option[Seq[String]],
                                 allColumns: Boolean) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    require(columnNames.isDefined ^ allColumns, "Parameter `columnNames` or `allColumns` are " +
      "mutually exclusive. Only one of them should be specified.")
    val sessionState = sparkSession.sessionState

    tableIdent.database match {
      case Some(db) if db == sparkSession.sharedState.globalTempViewManager.database =>
        val plan = sessionState.catalog.getGlobalTempView(tableIdent.identifier).getOrElse {
          throw new NoSuchTableException(db = db, table = tableIdent.identifier)
        }
        analyzeColumnInTempView(plan, sparkSession)
      case Some(_) =>
        analyzeColumnInCatalog(sparkSession)
      case None =>
        sessionState.catalog.getTempView(tableIdent.identifier) match {
          case Some(tempView) => analyzeColumnInTempView(tempView, sparkSession)
          case _ => analyzeColumnInCatalog(sparkSession)
        }
    }

    Seq.empty[Row]
  }

  private def analyzeColumnInCachedData(plan: LogicalPlan, sparkSession: SparkSession): Boolean = {
    val cacheManager = sparkSession.sharedState.cacheManager
    val planToLookup = sparkSession.sessionState.executePlan(plan).analyzed
    cacheManager.lookupCachedData(planToLookup).map { cachedData =>
      val columnsToAnalyze = getColumnsToAnalyze(
        tableIdent, cachedData.cachedRepresentation, columnNames, allColumns)
      cacheManager.analyzeColumnCacheQuery(sparkSession, cachedData, columnsToAnalyze)
      cachedData
    }.isDefined
  }

  private def analyzeColumnInTempView(plan: LogicalPlan, sparkSession: SparkSession): Unit = {
    if (!analyzeColumnInCachedData(plan, sparkSession)) {
      throw new AnalysisException(
        s"Temporary view $tableIdent is not cached for analyzing columns.")
    }
  }

  private def getColumnsToAnalyze(
                                   tableIdent: TableIdentifier,
                                   relation: LogicalPlan,
                                   columnNames: Option[Seq[String]],
                                   allColumns: Boolean = false): Seq[Attribute] = {
    val columnsToAnalyze = if (allColumns) {
      relation.output
    } else {
      columnNames.get.map { col =>
        val exprOption = relation.output.find(attr => conf.resolver(attr.name, col))
        exprOption.getOrElse(throw new AnalysisException(s"Column $col does not exist."))
      }
    }
    // Make sure the column types are supported for stats gathering.
    columnsToAnalyze.foreach { attr =>
      if (!supportsType(attr.dataType)) {
        throw new AnalysisException(
          s"Column ${attr.name} in table $tableIdent is of type ${attr.dataType}, " +
            "and Spark does not support statistics collection on this column type.")
      }
    }
    columnsToAnalyze
  }

  private def analyzeColumnInCatalog(sparkSession: SparkSession): Unit = {
    val sessionState = sparkSession.sessionState
    val tableMeta = sessionState.catalog.getTableMetadata(tableIdent)
    if (tableMeta.tableType == CatalogTableType.VIEW) {
      // Analyzes a catalog view if the view is cached
      val plan = sparkSession.table(tableIdent.quotedString).logicalPlan
      if (!analyzeColumnInCachedData(plan, sparkSession)) {
        throw new AnalysisException("ANALYZE TABLE is not supported on views.")
      }
    } else {
      val sizeInBytes = CommandUtils.calculateTotalSize(sparkSession, tableMeta)
      val relation = sparkSession.table(tableIdent).logicalPlan
      val columnsToAnalyze = getColumnsToAnalyze(tableIdent, relation, columnNames, allColumns)

      SQLConf.get.setConfString("spark.omni.sql.ndpPlugin.castDecimal.enabled", "false")
      // Compute stats for the computed list of columns.
      val (rowCount, newColStats) =
        NdpCommandUtils.computeColumnStats(sparkSession, relation, columnsToAnalyze)
      SQLConf.get.setConfString("spark.omni.sql.ndpPlugin.castDecimal.enabled", "true")
      val newColCatalogStats = newColStats.map {
        case (attr, columnStat) =>
          attr.name -> toCatalogColumnStat(columnStat, attr.name, attr.dataType)
      }

      // We also update table-level stats in order to keep them consistent with column-level stats.
      val statistics = CatalogStatistics(
        sizeInBytes = sizeInBytes,
        rowCount = Some(rowCount),
        // Newly computed column stats should override the existing ones.
        colStats = tableMeta.stats.map(_.colStats).getOrElse(Map.empty) ++ newColCatalogStats)

      sessionState.catalog.alterTableStats(tableIdent, Some(statistics))
    }
  }

  private def toCatalogColumnStat(columnStat: ColumnStat, colName: String, dataType: DataType): CatalogColumnStat =
    CatalogColumnStat(
      distinctCount = columnStat.distinctCount,
      min = columnStat.min.map(toExternalString(_, colName, dataType)),
      max = columnStat.max.map(toExternalString(_, colName, dataType)),
      nullCount = columnStat.nullCount,
      avgLen = columnStat.avgLen,
      maxLen = columnStat.maxLen,
      histogram = columnStat.histogram,
      version = columnStat.version)

  private def toExternalString(v: Any, colName: String, dataType: DataType): String = {
    val externalValue = dataType match {
      case DateType => DateFormatter().format(v.asInstanceOf[Int])
      case TimestampType => getTimestampFormatter(isParsing = false).format(v.asInstanceOf[Long])
      case BooleanType | _: IntegralType | FloatType | DoubleType | StringType => v
      case _: DecimalType => v.asInstanceOf[Decimal].toJavaBigDecimal
      case _ =>
        throw new AnalysisException("Column statistics serialization is not supported for " +
          s"column $colName of data type: $dataType.")
    }
    externalValue.toString
  }

  private def getTimestampFormatter(isParsing: Boolean): TimestampFormatter = {
    TimestampFormatter(
      format = "yyyy-MM-dd HH:mm:ss.SSSSSS",
      zoneId = ZoneOffset.UTC,
      isParsing = isParsing)
  }

  /** Returns true iff the we support gathering column statistics on column of the given type. */
  private def supportsType(dataType: DataType): Boolean = dataType match {
    case _: IntegralType => true
    case _: DecimalType => true
    case DoubleType | FloatType => true
    case BooleanType => true
    case DateType => true
    case TimestampType => true
    case BinaryType | StringType => true
    case _ => false
  }
}