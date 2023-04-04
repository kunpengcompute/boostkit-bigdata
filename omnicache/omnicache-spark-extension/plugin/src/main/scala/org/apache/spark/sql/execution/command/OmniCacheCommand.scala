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
import com.huawei.boostkit.spark.util.{RewriteHelper, ViewMetadata}
import com.huawei.boostkit.spark.util.ViewMetadata._
import com.huawei.boostkit.spark.util.lock.{FileLock, OmniCacheAtomic}
import java.io.{FileNotFoundException, IOException}
import java.net.URI
import java.rmi.UnexpectedException
import java.util.Locale
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.{mutable, JavaConverters}
import scala.util.control.NonFatal

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.{AnalysisException, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.getPartitionPathString
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.optimizer.OmniCacheToSparkAdapter._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CharVarcharUtils}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.WashOutStrategy._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.util.SchemaUtils


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
    try {
      ViewMetadata.init(sparkSession)
      loadViewCount()
      val sessionState = sparkSession.sessionState
      val databaseName = databaseNameOption.getOrElse(sessionState.catalog.getCurrentDatabase)
      val identifier = TableIdentifier(name, Option(databaseName))

      val (storageFormat, provider) = getStorageFormatAndProvider(
        providerStr, Map.empty, None)

      val table = buildCatalogTable(
        identifier, new StructType,
        partitioning, None,
        properties ++ Map(MV_LATEST_UPDATE_TIME ->
            ViewMetadata.getViewDependsTableTimeStr(query)),
        provider, None,
        comment, storageFormat, external = false)
      val tableIdentWithDB = identifier.copy(database = Some(databaseName))

      if (ViewMetadata.isViewExists(identifier.toString())) {
        if (!ifNotExistsSet) {
          throw new Exception(
            s"Materialized view with name $databaseName.$name already exists")
        } else {
          return Seq.empty
        }
      }

      if (sessionState.catalog.tableExists(tableIdentWithDB)) {
        if (!ifNotExistsSet) {
          throw new AnalysisException(
            s"Materialized View $tableIdentWithDB already exists. You need to drop it first")
        } else {
          // Since the table already exists and the save mode is Ignore, we will just return.
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
          // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
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

      // atomic save ViewMetadata.viewCnt
      val dbName = table.identifier.database.getOrElse(DEFAULT_DATABASE)
      val dbPath = new Path(metadataPath, dbName)
      val dbViewCnt = new Path(dbPath, VIEW_CNT_FILE)
      val fileLock = FileLock(fs, new Path(dbPath, VIEW_CNT_FILE_LOCK))
      OmniCacheAtomic.funcWithSpinLock(fileLock) {
        () =>
          val viewName = formatViewName(table.identifier)
          if (fs.exists(dbViewCnt)) {
            val curModifyTime = fs.getFileStatus(dbViewCnt).getModificationTime
            if (ViewMetadata.getViewCntModifyTime(viewCnt).getOrElse(0L) != curModifyTime) {
              loadViewCount(dbName)
            }
          }
          ViewMetadata.viewCnt.put(
            viewName, Array(0, System.currentTimeMillis(), UNLOAD))
          saveViewCountToFile(dbName)
          loadViewCount(dbName)
      }

      ViewMetadata.addCatalogTableToCache(table)
    } catch {
      case e: Throwable =>
        throw e
    } finally {
      RewriteHelper.enableCachePlugin()
    }
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

/**
 * Drops a materialized view from the metastore and removes it if it is cached.
 *
 * The syntax of this command is:
 * {{{
 *   DROP MATERIALIZED VIEW [IF EXISTS] view_name;
 * }}}
 */
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
            "Cannot drop a view with DROP TABLE. Please use DROP VIEW instead")
        case _ =>
      }
    }

    if (isTempView || catalog.tableExists(tableName)) {
      ViewMetadata.init(sparkSession)
      if (catalog.tableExists(tableName) &&
          !isMV(catalog.getTableMetadata(tableName))) {
        throw new AnalysisException(
          "Cannot drop a table with DROP MV. Please use DROP TABLE instead")
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
      ViewMetadata.deleteViewMetadata(tableName)

      // atomic del ViewMetadata.viewCnt
      val dbName = tableName.database.getOrElse(DEFAULT_DATABASE)
      val dbPath = new Path(metadataPath, dbName)
      val dbViewCnt = new Path(dbPath, VIEW_CNT_FILE)
      val filelock = FileLock(fs, new Path(dbPath, VIEW_CNT_FILE_LOCK))
      OmniCacheAtomic.funcWithSpinLock(filelock) {
        () =>
          val viewName = formatViewName(tableName)
          if (fs.exists(dbViewCnt)) {
            val curModifyTime = fs.getFileStatus(dbViewCnt).getModificationTime
            if (ViewMetadata.getViewCntModifyTime(viewCnt).getOrElse(0L) != curModifyTime) {
              loadViewCount(dbName)
            }
          }
          ViewMetadata.viewCnt.remove(viewName)
          saveViewCountToFile(dbName)
          loadViewCount(dbName)
      }
    } else if (ifExists) {
      // no-op
    } else {
      throw new AnalysisException(s"Table or view not found: ${tableName.identifier}")
    }
    Seq.empty[Row]
  }
}

/**
 * ShowMaterializedViewCommand RunnableCommand
 *
 */
case class ShowMaterializedViewCommand(
    databaseName: Option[String],
    tableIdentifierPattern: Option[String]) extends RunnableCommand {
  // The result of SHOW MaterializedView has three basic columns:
  // database, tableName and originalSql.

  override val output: Seq[Attribute] = {
    val tableExtendedInfo = Nil
    AttributeReference("database", StringType, nullable = false)() ::
        AttributeReference("mvName", StringType, nullable = false)() ::
        AttributeReference("rewriteEnable", StringType, nullable = false)() ::
        AttributeReference("latestUpdateTime", StringType, nullable = false)() ::
        AttributeReference("originalSql", StringType, nullable = false)() ::
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
      Row(database, tableName, rewriteEnable, latestUpdateTime, original)
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
        ViewMetadata.addCatalogTableToCache(newTable)
      } else {
        ViewMetadata.addCatalogTableToCache(newTable)
        ViewMetadata.removeMVCache(tableName)
      }
    } else {
      throw new AnalysisException(s"Table or view not found: ${tableName.identifier}")
    }
    Seq.empty
  }
}

case class RefreshMaterializedViewCommand(
    outputPath: Path,
    staticPartitions: TablePartitionSpec,
    ifPartitionNotExists: Boolean,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    fileFormat: FileFormat,
    options: Map[String, String],
    query: LogicalPlan,
    mode: SaveMode,
    catalogTable: Option[CatalogTable],
    fileIndex: Option[FileIndex],
    outputColumnNames: Seq[String])
    extends DataWritingCommand {

  private lazy val parameters = CaseInsensitiveMap(options)

  private[sql] lazy val dynamicPartitionOverwrite: Boolean = {
    val partitionOverwriteMode = parameters.get("partitionOverwriteMode")
        .map(mode => PartitionOverwriteMode.withName(mode.toUpperCase(Locale.ROOT)))
        .getOrElse(SQLConf.get.partitionOverwriteMode)
    val enableDynamicOverwrite = partitionOverwriteMode == PartitionOverwriteMode.DYNAMIC
    // This config only makes sense when we are overwriting a partitioned dataset with dynamic
    // partition columns.
    enableDynamicOverwrite && mode == SaveMode.Overwrite &&
        staticPartitions.size < partitionColumns.length
  }

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    try {
      // disable mv rewrite
      var tableMeta = catalogTable.get
      tableMeta = tableMeta.copy(properties =
        tableMeta.properties + (MV_UPDATE_REWRITE_ENABLED -> "false"))
      sparkSession.sessionState.catalog.alterTable(tableMeta)

      // Most formats don't do well with duplicate columns, so lets not allow that
      SchemaUtils.checkColumnNameDuplication(
        outputColumnNames,
        s"when inserting into $outputPath",
        sparkSession.sessionState.conf.caseSensitiveAnalysis)

      val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options)
      val fs = outputPath.getFileSystem(hadoopConf)
      val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

      val partitionsTrackedByCatalog = sparkSession.sessionState.conf.manageFilesourcePartitions &&
          catalogTable.isDefined &&
          catalogTable.get.partitionColumnNames.nonEmpty &&
          catalogTable.get.tracksPartitionsInCatalog

      var initialMatchingPartitions: Seq[TablePartitionSpec] = Nil
      var customPartitionLocations: Map[TablePartitionSpec, String] = Map.empty
      var matchingPartitions: Seq[CatalogTablePartition] = Seq.empty

      // When partitions are tracked by the catalog, compute all custom partition locations that
      // may be relevant to the insertion job.
      if (partitionsTrackedByCatalog) {
        matchingPartitions = sparkSession.sessionState.catalog.listPartitions(
          catalogTable.get.identifier, Some(staticPartitions))
        initialMatchingPartitions = matchingPartitions.map(_.spec)
        customPartitionLocations = getCustomPartitionLocations(
          fs, catalogTable.get, qualifiedOutputPath, matchingPartitions)
      }

      val jobId = java.util.UUID.randomUUID().toString
      val committer = FileCommitProtocol.instantiate(
        sparkSession.sessionState.conf.fileCommitProtocolClass,
        jobId = jobId,
        outputPath = outputPath.toString,
        dynamicPartitionOverwrite = dynamicPartitionOverwrite)

      val doInsertion = if (mode == SaveMode.Append) {
        true
      } else {
        val pathExists = fs.exists(qualifiedOutputPath)
        (mode, pathExists) match {
          case (SaveMode.ErrorIfExists, true) =>
            throw new AnalysisException(s"path $qualifiedOutputPath already exists.")
          case (SaveMode.Overwrite, true) =>
            if (ifPartitionNotExists && matchingPartitions.nonEmpty) {
              false
            } else if (dynamicPartitionOverwrite) {
              // For dynamic partition overwrite, do not delete partition directories ahead.
              true
            } else {
              deleteMatchingPartitions(fs, qualifiedOutputPath, customPartitionLocations, committer)
              true
            }
          case (SaveMode.Overwrite, _) | (SaveMode.ErrorIfExists, false) =>
            true
          case (SaveMode.Ignore, exists) =>
            !exists
          case (s, exists) =>
            throw new IllegalStateException(s"unsupported save mode $s ($exists)")
        }
      }

      if (doInsertion) {
        def refreshUpdatedPartitions(updatedPartitionPaths: Set[String]): Unit = {
          val updatedPartitions = updatedPartitionPaths.map(PartitioningUtils.parsePathFragment)
          if (partitionsTrackedByCatalog) {
            val newPartitions = updatedPartitions -- initialMatchingPartitions
            if (newPartitions.nonEmpty) {
              AlterTableAddPartitionCommand(
                catalogTable.get.identifier, newPartitions.toSeq.map(p => (p, None)),
                ifNotExists = true).run(sparkSession)
            }
            // For dynamic partition overwrite, we never remove partitions but only update existing
            // ones.
            if (mode == SaveMode.Overwrite && !dynamicPartitionOverwrite) {
              val deletedPartitions = initialMatchingPartitions.toSet -- updatedPartitions
              if (deletedPartitions.nonEmpty) {
                AlterTableDropPartitionCommand(
                  catalogTable.get.identifier, deletedPartitions.toSeq,
                  ifExists = true, purge = false,
                  retainData = true /* already deleted */).run(sparkSession)
              }
            }
          }
        }

        // For dynamic partition overwrite, FileOutputCommitter's output path is staging path, files
        // will be renamed from staging path to final output path during commit job
        val committerOutputPath = if (dynamicPartitionOverwrite) {
          FileCommitProtocol.getStagingDir(outputPath.toString, jobId)
              .makeQualified(fs.getUri, fs.getWorkingDirectory)
        } else {
          qualifiedOutputPath
        }

        val updatedPartitionPaths =
          FileFormatWriter.write(
            sparkSession = sparkSession,
            plan = child,
            fileFormat = fileFormat,
            committer = committer,
            outputSpec = FileFormatWriter.OutputSpec(
              committerOutputPath.toString, customPartitionLocations, outputColumns),
            hadoopConf = hadoopConf,
            partitionColumns = partitionColumns,
            bucketSpec = bucketSpec,
            statsTrackers = Seq(basicWriteJobStatsTracker(hadoopConf)),
            options = options)


        // update metastore partition metadata
        if (updatedPartitionPaths.isEmpty && staticPartitions.nonEmpty
            && partitionColumns.length == staticPartitions.size) {
          // Avoid empty static partition can't loaded to datasource table.
          val staticPathFragment =
            PartitioningUtils.getPathFragment(staticPartitions, partitionColumns)
          refreshUpdatedPartitions(Set(staticPathFragment))
        } else {
          refreshUpdatedPartitions(updatedPartitionPaths)
        }

        // refresh cached files in FileIndex
        fileIndex.foreach(_.refresh())
        // refresh data cache if table is cached
        sparkSession.sharedState.cacheManager.recacheByPath(sparkSession, outputPath, fs)

        if (catalogTable.nonEmpty) {
          CommandUtils.updateTableStats(sparkSession, catalogTable.get)
        }
      } else {
        logInfo("Skipping insertion into a relation that already exists.")
      }
    } catch {
      case e: Throwable =>
        throw e
    } finally {
      // enable plugin and mv rewrite
      var tableMeta = catalogTable.get
      tableMeta = tableMeta.copy(properties =
        tableMeta.properties + (MV_UPDATE_REWRITE_ENABLED -> "true"))
      sparkSession.sessionState.catalog.alterTable(tableMeta)
      RewriteHelper.enableCachePlugin()
    }
    Seq.empty[Row]
  }

  /**
   * Deletes all partition files that match the specified static prefix. Partitions with custom
   * locations are also cleared based on the custom locations map given to this class.
   */
  private def deleteMatchingPartitions(
      fs: FileSystem,
      qualifiedOutputPath: Path,
      customPartitionLocations: Map[TablePartitionSpec, String],
      committer: FileCommitProtocol): Unit = {
    val staticPartitionPrefix = if (staticPartitions.nonEmpty) {
      "/" + partitionColumns.flatMap { p =>
        staticPartitions.get(p.name).map(getPartitionPathString(p.name, _))
      }.mkString("/")
    } else {
      ""
    }
    // first clear the path determined by the static partition keys (e.g. /table/foo=1)
    val staticPrefixPath = qualifiedOutputPath.suffix(staticPartitionPrefix)
    if (fs.exists(staticPrefixPath) && !committer
        .deleteWithJob(fs, staticPrefixPath, recursive = true)) {
      throw new IOException(s"Unable to clear output " +
          s"directory $staticPrefixPath prior to writing to it")
    }
    // now clear all custom partition locations (e.g. /custom/dir/where/foo=2/bar=4)
    for ((spec, customLoc) <- customPartitionLocations) {
      assert(
        (staticPartitions.toSet -- spec).isEmpty,
        "Custom partition location did not match static partitioning keys")
      val path = new Path(customLoc)
      if (fs.exists(path) && !committer.deleteWithJob(fs, path, recursive = true)) {
        throw new IOException(s"Unable to clear partition " +
            s"directory $path prior to writing to it")
      }
    }
  }

  /**
   * Given a set of input partitions, returns those that have locations that differ from the
   * Hive default (e.g. /k1=v1/k2=v2). These partitions were manually assigned locations by
   * the user.
   *
   * @return a mapping from partition specs to their custom locations
   */
  private def getCustomPartitionLocations(
      fs: FileSystem,
      table: CatalogTable,
      qualifiedOutputPath: Path,
      partitions: Seq[CatalogTablePartition]): Map[TablePartitionSpec, String] = {
    partitions.flatMap { p =>
      val defaultLocation = qualifiedOutputPath.suffix(
        "/" + PartitioningUtils.getPathFragment(p.spec, table.partitionSchema)).toString
      val catalogLocation = new Path(p.location).makeQualified(
        fs.getUri, fs.getWorkingDirectory).toString
      if (catalogLocation != defaultLocation) {
        Some(p.spec -> catalogLocation)
      } else {
        None
      }
    }.toMap
  }
}

/**
 * Eliminate the least used materialized view.
 *
 * The syntax of this command is:
 * {{{
 *   WASH OUT MATERIALIZED VIEW;
 * }}}
 */
case class WashOutMaterializedViewCommand(
    dropAll: Boolean,
    strategy: Option[List[(String, Option[Int])]]) extends RunnableCommand {

  private val logFlag = "[OmniCache]"

  override def run(sparkSession: SparkSession): Seq[Row] = {
    ViewMetadata.init(sparkSession)
    loadViewCount()
    if (dropAll) {
      washOutAllMV()
      return Seq.empty[Row]
    }
    if (strategy.isDefined) {
      strategy.get.foreach {
        infos: (String, Option[Int]) =>
          infos._1 match {
            case UNUSED_DAYS =>
              washOutByUnUsedDays(infos._2)
            case RESERVE_QUANTITY_BY_VIEW_COUNT =>
              washOutByReserveQuantity(infos._2)
            case DROP_QUANTITY_BY_SPACE_CONSUMED =>
              washOutViewsBySpace(infos._2)
            case _ =>
          }
      }
    } else {
      // default wash out strategy.
      washOutByUnUsedDays(Option.empty)
    }

    // save wash out timestamp
    ViewMetadata.washOutTimestamp = Some(System.currentTimeMillis())
    ViewMetadata.saveWashOutTimestamp()

    Seq.empty[Row]
  }

  private def washOutAllMV(): Unit = {
    ViewMetadata.viewCnt.forEach {
      (viewName, _) =>
        ViewMetadata.spark.sql("DROP MATERIALIZED VIEW IF EXISTS " + viewName)
    }
    logInfo(f"$logFlag WASH OUT ALL MATERIALIZED VIEW.")
  }

  private def washOutByUnUsedDays(para: Option[Int]): Unit = {
    val unUsedDays = para.getOrElse(
      OmniCachePluginConfig.getConf.minimumUnusedDaysForWashOut)
    val curTime = System.currentTimeMillis()
    val threshold = curTime - RewriteHelper.daysToMillisecond(unUsedDays.toLong)
    ViewMetadata.viewCnt.forEach {
      (viewName, viewInfo) =>
        if (viewInfo(1) <= threshold) {
          ViewMetadata.spark.sql("DROP MATERIALIZED VIEW IF EXISTS " + viewName)
        }
    }
    logInfo(f"$logFlag WASH OUT MATERIALIZED VIEW " +
        f"USING $UNUSED_DAYS $unUsedDays.")
  }

  private def washOutByReserveQuantity(para: Option[Int]): Unit = {
    val reserveQuantity = para.getOrElse(
      OmniCachePluginConfig.getConf.reserveViewQuantityByViewCount)
    var viewCntList = JavaConverters.mapAsScalaMap(ViewMetadata.viewCnt).toList
    if (viewCntList.size <= reserveQuantity) {
      return
    }
    viewCntList = viewCntList.sorted {
      (x: (String, Array[Long]), y: (String, Array[Long])) => {
        if (y._2(0) != x._2(0)) {
          y._2(0).compare(x._2(0))
        } else {
          y._2(1).compare(x._2(1))
        }
      }
    }
    for (i <- reserveQuantity until viewCntList.size) {
      ViewMetadata.spark.sql("DROP MATERIALIZED VIEW IF EXISTS " + viewCntList(i)._1)
    }
    logInfo(f"$logFlag WASH OUT MATERIALIZED VIEW " +
        f"USING $RESERVE_QUANTITY_BY_VIEW_COUNT $reserveQuantity.")
  }

  private def washOutViewsBySpace(para: Option[Int]): Unit = {
    val dropQuantity = para.getOrElse(
      OmniCachePluginConfig.getConf.dropViewQuantityBySpaceConsumed)
    val views = JavaConverters.mapAsScalaMap(ViewMetadata.viewCnt).toList.map(_._1)
    val viewInfos = mutable.Map[String, Long]()
    views.foreach {
      view =>
        val dbName = view.split("\\.")(0)
        val tableName = view.split("\\.")(1)
        val tableLocation = ViewMetadata.spark.sessionState.catalog.defaultTablePath(
          TableIdentifier(tableName, Some(dbName)))
        var spaceConsumed = Long.MaxValue
        try {
          spaceConsumed = ViewMetadata.fs.getContentSummary(
            new Path(tableLocation)).getSpaceConsumed
        } catch {
          case _: FileNotFoundException =>
            log.info(f"Can not find table: $tableName. It may have been deleted.")
          case _ =>
            throw new UnexpectedException(
              "Something unknown happens when wash out views by space")
        } finally {
          viewInfos.put(view, spaceConsumed)
        }
    }
    val topN = viewInfos.toList.sorted {
      (x: (String, Long), y: (String, Long)) => {
        y._2.compare(x._2)
      }
    }.slice(0, dropQuantity)
    topN.foreach {
      view =>
        ViewMetadata.spark.sql("DROP MATERIALIZED VIEW IF EXISTS " + view._1)
    }
    logInfo(f"$logFlag WASH OUT MATERIALIZED VIEW " +
        f"USING $DROP_QUANTITY_BY_SPACE_CONSUMED $dropQuantity.")
  }

}

object WashOutStrategy {
  val UNUSED_DAYS = "UNUSED_DAYS"
  val RESERVE_QUANTITY_BY_VIEW_COUNT = "RESERVE_QUANTITY_BY_VIEW_COUNT"
  val DROP_QUANTITY_BY_SPACE_CONSUMED = "DROP_QUANTITY_BY_SPACE_CONSUMED"
}
