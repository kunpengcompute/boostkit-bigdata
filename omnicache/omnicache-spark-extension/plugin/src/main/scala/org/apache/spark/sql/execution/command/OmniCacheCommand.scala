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

import com.huawei.boostkit.spark.conf.OmniCachePluginConfig
import com.huawei.boostkit.spark.conf.OmniCachePluginConfig._
import com.huawei.boostkit.spark.util.ViewMetadata
import java.net.URI
import scala.util.control.NonFatal

import org.apache.spark.sql.{AnalysisException, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.optimizer.OmniCacheToSparkAdapter._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{DataSource, HadoopFsRelation}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{StringType, StructType}

case class OmniCacheCreateMvCommand(
    databaseNameOption: Option[String],
    name: String,
    providerStr: String,
    comment: Option[String],
    properties: Map[String, String],
    ifNotExistsSet: Boolean = false,
    partitioning: Seq[String],
    query: LogicalPlan,
    outputColumnNames: Seq[String]) extends DataWritingCommand {
  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    ViewMetadata.init(sparkSession)
    val sessionState = sparkSession.sessionState
    val databaseName = databaseNameOption.getOrElse(sessionState.catalog.getCurrentDatabase)
    val identifier = TableIdentifier(name, Option(databaseName))

    val (storageFormat, provider) = getStorageFormatAndProvider(
      providerStr, properties, None
    )

    val table = buildCatalogTable(
      identifier, new StructType,
      partitioning, None, properties, provider, None,
      comment, storageFormat, external = false
    )
    val tableIdentWithDB = identifier.copy(database = Some(databaseName))

    if (ViewMetadata.isViewExists(identifier.toString())) {
      if (!ifNotExistsSet) {
        throw new Exception(
          s"Materialized view with name $databaseName.$name already exists"
        )
      } else {
        return Seq.empty
      }
    }

    if (sessionState.catalog.tableExists(tableIdentWithDB)) {
      if (!ifNotExistsSet) {
        throw new AnalysisException(
          s"Materialized View $tableIdentWithDB already exists. You need to drop it first")
      } else {
        // Since the table already exists and the save mode is Ignore,we will just return.
        return Seq.empty
      }
    } else {
      assert(table.schema.isEmpty)
      sparkSession.sessionState.catalog.validateTableLocation(table)
      val tableLocation = if (table.tableType == CatalogTableType.MANAGED) {
        Some(sessionState.catalog.defaultTablePath(table.identifier))
      } else {
        table.storage.locationUri
      }
      val result = saveDataIntoTable(
        sparkSession, table, tableLocation, child, SaveMode.Overwrite)
      val tableSchema = CharVarcharUtils.getRawSchema(result.schema)
      val newTable = table.copy(
        storage = table.storage.copy(locationUri = tableLocation),
        // We will use the schema of resolved.relation as the schema of the table (instead of
        // the schema of df). It is important since the nullability may be changed by the relation
        // provider (for example,see org.apache.spark.sql.parquet.DefaultSource).
        schema = tableSchema)
      // Table location is already validated. No need to check it again during table creation.
      sessionState.catalog.createTable(newTable, ignoreIfExists = false, validateLocation = false)
      result match {
        case _: HadoopFsRelation if table.partitionColumnNames.nonEmpty &&
            sparkSession.sqlContext.conf.manageFilesourcePartitions =>
          // Need to recover partitions into the metastore so our saved data is visible
          sessionState.executePlan(AlterTableRecoverPartitionsCommand(table.identifier)).toRdd
        case _ =>
      }
    }

    CommandUtils.updateTableStats(sparkSession, table)
    ViewMetadata.addCatalogTableToCache(table)

    Seq.empty[Row]
  }

  private def saveDataIntoTable(
      session: SparkSession,
      table: CatalogTable,
      tableLocation: Option[URI],
      physicalPlan: SparkPlan,
      mode: SaveMode,
      tableExists: Boolean = false): BaseRelation = {
    // Create the relation based on the input logical plan: `query`.
    val pathOption = tableLocation.map("path" -> CatalogUtils.URIToString(_))
    val dataSource = DataSource(
      session,
      className = table.provider.get,
      partitionColumns = table.partitionColumnNames,
      bucketSpec = table.bucketSpec,
      options = table.storage.properties ++ pathOption,
      catalogTable = if (tableExists) Some(table) else None)

    try {
      dataSource.writeAndRead(mode, query, outputColumnNames, physicalPlan)
    } catch {
      case ex: AnalysisException =>
        logError(s"Failed to write to table ${table.identifier.unquotedString}", ex)
        throw ex
    }
  }
}

case class DropMaterializedViewCommand(
    tableName: TableIdentifier,
    ifExists: Boolean,
    purge: Boolean) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    ViewMetadata.init(sparkSession)
    val catalog = sparkSession.sessionState.catalog
    val isTempView = catalog.isTemporaryTable(tableName)

    if (!isTempView && catalog.tableExists(tableName)) {
      // If the command DROP VIEW is to drop a table or DROP TABLE is to drop a view
      // issue an exception.
      catalog.getTableMetadata(tableName).tableType match {
        case CatalogTableType.VIEW =>
          throw new AnalysisException(
            "Cannot drop a view with DROP TABLE. Please use DROP VIEW instead"
          )
        case _ =>
      }
    }

    if (isTempView || catalog.tableExists(tableName)) {
      ViewMetadata.init(sparkSession)
      if (catalog.tableExists(tableName) &&
          !isMV(catalog.getTableMetadata(tableName))) {
        throw new AnalysisException(
          "Cannot drop a table with DROP MV. Please use DROP TABLE instead"
        )
      }
      try {
        val hasViewText = isTempView &&
            catalog.getTempViewOrPermanentTableMetadata(tableName).viewText.isDefined
        sparkSession.sharedState.cacheManager.uncacheQuery(
          sparkSession.table(tableName), cascade = !isTempView || hasViewText)
      } catch {
        case NonFatal(e) => log.warn(e.toString, e)
      }
      catalog.refreshTable(tableName)
      catalog.dropTable(tableName, ifExists, purge)
      // remove mv from cache
      ViewMetadata.removeMVCache(tableName)
    } else if (ifExists) {
      // no-op
    } else {
      throw new AnalysisException(s"Table or view not found: ${tableName.identifier}")
    }
    Seq.empty[Row]
  }
}

case class ShowMaterializedViewCommand(
    databaseName: Option[String],
    tableIdentifierPattern: Option[String]
) extends RunnableCommand {

  override val output: Seq[Attribute] = {
    val tableExtendedInfo = Nil

    AttributeReference("database", StringType, nullable = false)() ::
        AttributeReference("mvName", StringType, nullable = false)() ::
        AttributeReference("rewriteEnable", StringType, nullable = false)() ::
        AttributeReference("latestUpdateTime", StringType, nullable = false)() ::
        AttributeReference("originalSql", StringType, nullable = false)() ::
        AttributeReference("isCached", StringType, nullable = false)() ::
        tableExtendedInfo
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    ViewMetadata.init(sparkSession)
    val catalog = sparkSession.sessionState.catalog
    val db = databaseName.getOrElse(catalog.getCurrentDatabase)

    val omniCacheFilter: TableIdentifier => Boolean = {
      tableIdentifier =>
        isMV(catalog.getTableMetadata(tableIdentifier))
    }
    val tables =
      tableIdentifierPattern.map(catalog.listTables(db, _)).getOrElse(catalog.listTables(db))
          .filter(omniCacheFilter)
    if (tableIdentifierPattern.isDefined && tables.isEmpty) {
      throw new AnalysisException(s"Table or view not found: ${tableIdentifierPattern.get}")
    }

    val showLength = tableIdentifierPattern match {
      case Some(_) =>
        Integer.MAX_VALUE
      case None =>
        OmniCachePluginConfig.getConf.showMVQuerySqlLen
    }
    tables.map { tableIdent =>
      val properties = catalog.getTableMetadata(tableIdent).properties
      val database = tableIdent.database.getOrElse("")
      val tableName = tableIdent.table
      var original = properties.getOrElse(MV_QUERY_ORIGINAL_SQL, "")
      original = original.substring(0, Math.min(original.length, showLength))
      val rewriteEnable = properties.getOrElse(MV_REWRITE_ENABLED, "")
      val latestUpdateTime = properties.getOrElse(MV_LATEST_UPDATE_TIME, "")
      val isCached = ViewMetadata.isViewExists(tableIdent.quotedString).toString
      Row(database, tableName, rewriteEnable, latestUpdateTime, original, isCached)
    }
  }
}

case class AlterRewriteMaterializedViewCommand(
    tableName: TableIdentifier,
    enableRewrite: Boolean) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    ViewMetadata.init(sparkSession)
    val catalog = sparkSession.sessionState.catalog
    if (catalog.tableExists(tableName)) {
      ViewMetadata.init(sparkSession)
      if (!isMV(catalog.getTableMetadata(tableName))) {
        throw new AnalysisException(
          "Cannot alter a table with ALTER MV. Please use ALTER TABLE instead")
      }
      val table = catalog.getTableMetadata(tableName)
      val newTable = table.copy(
        properties = table.properties ++ Map(MV_REWRITE_ENABLED -> enableRewrite.toString)
      )
      catalog.alterTable(newTable)

      if (enableRewrite) {
        ViewMetadata.saveViewMetadataToMap(newTable)
      } else {
        ViewMetadata.removeMVCache(tableName)
      }
    } else {
      throw new AnalysisException(s"Table or view not found: ${tableName.identifier}")
    }
    Seq.empty
  }
}
