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

package com.huawei.boostkit.spark.util

import com.huawei.boostkit.spark.conf.OmniCachePluginConfig
import com.huawei.boostkit.spark.conf.OmniCachePluginConfig._
import com.huawei.boostkit.spark.util.serde.KryoSerDeUtil
import java.io.IOException
import java.net.URI
import java.util.Locale
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, LocalFileSystem, Path}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import scala.collection.{mutable, JavaConverters}

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.rules.RewriteTime
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation


object ViewMetadata extends RewriteHelper {

  val viewToViewQueryPlan = new ConcurrentHashMap[String, LogicalPlan]()

  val viewToTablePlan = new ConcurrentHashMap[String, LogicalPlan]()

  val viewToContainsTables = new ConcurrentHashMap[String, Set[TableEqual]]()

  val tableToViews = new ConcurrentHashMap[String, mutable.Set[String]]()

  val viewProperties = new ConcurrentHashMap[String, Map[String, String]]()

  val viewPriority = new ConcurrentHashMap[String, Long]()

  // Map (viewName <- Array(viewCounts, lastUsedMillisecond, fileModifyTime))
  val viewCnt = new ConcurrentHashMap[String, Array[Long]]()

  var spark: SparkSession = _

  var fs: FileSystem = _

  var metadataPath: Path = _
  var metadataPriorityPath: Path = _

  var initQueryPlan: Option[LogicalPlan] = None

  var washOutTimestamp: Option[Long] = Option.empty
  var autoWashOutTimestamp: Option[Long] = Option.empty

  val STATUS_UN_LOAD = "UN_LOAD"
  val STATUS_LOADING = "LOADING"
  val STATUS_LOADED = "LOADED"

  var status: String = STATUS_UN_LOAD

  val VIEW_CNT_FILE = "viewCount"
  val VIEW_CNT_FILE_LOCK = "viewCount.lock"
  val DEFAULT_DATABASE = "default"
  val VIEW_CONTAINS_TABLES_FILE = "viewContainsTables"
  val WASH_OUT_TIMESTAMP = "washOutTimestamp"

  private var kryoSerializer: KryoSerializer = _

  private val SEPARATOR: Char = 0xA

  val UNLOAD: Int = -1

  private var REFRESH_STAT: String = _
  private val BUSY = "BUSY"
  private val IDLE = "IDLE"

  /**
   * set sparkSession
   */
  def setSpark(sparkSession: SparkSession): Unit = {
    spark = sparkSession
    REFRESH_STAT = IDLE
    status = STATUS_LOADING

    kryoSerializer = new KryoSerializer(spark.sparkContext.getConf)

    metadataPath = new Path(OmniCachePluginConfig.getConf.metadataPath)
    metadataPriorityPath = new Path(metadataPath, "priority")

    val conf = KerberosUtil.newConfiguration(spark)
    fs = metadataPath.getFileSystem(conf)

    val paths = Seq(metadataPath, metadataPriorityPath)
    paths.foreach { path =>
      if (!fs.exists(path)) {
        fs.mkdirs(path)
      }
    }
  }

  /**
   * save mv metadata to cache
   */
  def saveViewMetadataToMap(catalogTable: CatalogTable): Unit = this.synchronized {
    val viewQuerySql = catalogTable.properties.getOrElse(MV_QUERY_ORIGINAL_SQL, "")
    if (viewQuerySql.isEmpty) {
      logError(s"mvTable: ${catalogTable.identifier.quotedString}'s viewQuerySql is empty!")
      return
    }

    // preserve preDatabase and set curDatabase
    val preDatabase = spark.catalog.currentDatabase
    val curDatabase = catalogTable.properties.getOrElse(MV_QUERY_ORIGINAL_SQL_CUR_DB, "")
    if (curDatabase.isEmpty) {
      logError(s"mvTable: ${catalogTable.identifier.quotedString}'s curDatabase is empty!")
      return
    }
    try {
      spark.sessionState.catalogManager.setCurrentNamespace(Array(curDatabase))

      // db.table
      val tableName = catalogTable.identifier.quotedString
      val viewTablePlan = RewriteTime
          .withTimeStat("viewTablePlan") {
            spark.table(tableName).queryExecution.analyzed match {
              case SubqueryAlias(_, child) => child
              case a@_ => a
            }
          }
      var viewQueryPlan = RewriteTime
          .withTimeStat("viewQueryPlan") {
            RewriteHelper.optimizePlan(
              spark.sql(viewQuerySql).queryExecution.analyzed)
          }
      viewQueryPlan = viewQueryPlan match {
        case RepartitionByExpression(_, child, _) =>
          child
        case _ =>
          viewQueryPlan
      }
      // reset preDatabase
      spark.sessionState.catalogManager.setCurrentNamespace(Array(preDatabase))

      // spark_catalog.db.table
      val viewName = formatViewName(catalogTable.identifier)

      // mappedViewQueryPlan and mappedViewContainsTables
      val (mappedViewQueryPlan, mappedViewContainsTables) = RewriteTime
          .withTimeStat("extractTables") {
            extractTables(sortProjectListForPartition(viewQueryPlan, catalogTable))
          }

      mappedViewContainsTables
          .foreach { mappedViewContainsTable =>
            val name = mappedViewContainsTable.tableName
            val views = tableToViews.getOrDefault(name, mutable.Set.empty)
            views += viewName
            tableToViews.put(name, views)
          }

      // extract view query project's Attr and replace view table's Attr by query project's Attr
      // match function is attributeReferenceEqualSimple, by name and data type
      // Attr of table cannot used, because same Attr in view query and view table,
      // it's table is different.
      val mappedViewTablePlan = RewriteTime
          .withTimeStat("mapTablePlanAttrToQuery") {
            mapTablePlanAttrToQuery(viewTablePlan, mappedViewQueryPlan)
          }

      viewToContainsTables.put(viewName, mappedViewContainsTables)
      viewToViewQueryPlan.putIfAbsent(viewName, mappedViewQueryPlan)
      viewToTablePlan.putIfAbsent(viewName, mappedViewTablePlan)
      viewProperties.put(viewName, catalogTable.properties)
      saveViewMetadataToFile(catalogTable.database, viewName)
    } catch {
      case e: Throwable =>
        logDebug(s"Failed to saveViewMetadataToMap,errmsg: ${e.getMessage}")
        throw new IOException(s"Failed to save ViewMetadata to file.")
    } finally {
      // reset preDatabase
      spark.sessionState.catalogManager.setCurrentNamespace(Array(preDatabase))
    }
  }

  /**
   * is metadata empty
   */
  def isEmpty: Boolean = {
    viewToTablePlan.isEmpty
  }

  /**
   * is mv exists
   */
  def isViewExists(viewIdentifier: String): Boolean = {
    viewToTablePlan.containsKey(viewIdentifier)
  }

  /**
   * add catalog table to cache
   */
  def addCatalogTableToCache(table: CatalogTable): Unit = this.synchronized {
    saveViewMetadataToMap(table)
    if (!isViewEnable(table.properties)) {
      removeMVCache(table.identifier)
    }
  }

  /**
   * remove mv metadata from cache
   */
  def removeMVCache(tableName: TableIdentifier): Unit = this.synchronized {
    val viewName = formatViewName(tableName)
    viewToContainsTables.remove(viewName)
    viewToViewQueryPlan.remove(viewName)
    viewToTablePlan.remove(viewName)
    viewProperties.remove(viewName)
    tableToViews.forEach { (key, value) =>
      if (value.contains(viewName)) {
        value -= viewName
        tableToViews.put(key, value)
      }
    }
  }

  /**
   * init mv metadata
   */
  def init(sparkSession: SparkSession): Unit = {
    init(sparkSession, None)
  }

  /**
   * init mv metadata with certain queryPlan
   */
  def init(sparkSession: SparkSession, queryPlan: Option[LogicalPlan]): Unit = {
    if (status == STATUS_LOADED) {
      return
    }

    initQueryPlan = queryPlan
    setSpark(sparkSession)
    forceLoad()
    status = STATUS_LOADED
  }

  // Called when ViewMetadata is initialized.
  def forceLoad(): Unit = this.synchronized {
    loadViewContainsTablesFromFile()
    loadViewMetadataFromFile()
    loadViewPriorityFromFile()
    loadViewCount()
    checkViewMetadataComplete()
  }

  /**
   * load mv metadata from metastore
   */
  def forceLoadFromMetastore(): Unit = this.synchronized {
    val catalog = spark.sessionState.catalog

    // load from all db
    val dbs = RewriteTime.withTimeStat("loadDbs") {
      if (getConf.omniCacheDB.nonEmpty) {
        getConf.omniCacheDB.split(",").toSeq
      } else {
        catalog.listDatabases()
      }
    }
    for (db <- dbs) {
      val tables = RewriteTime.withTimeStat(s"loadTable from $db") {
        omniCacheFilter(catalog, db)
      }
      RewriteTime.withTimeStat("saveViewMetadataToMap") {
        tables.foreach(tableData => saveViewMetadataToMap(tableData))
      }
    }
    logDetail(s"tableToViews:$tableToViews")
  }

  /**
   * filter mv metadata from database
   */
  def omniCacheFilter(catalog: SessionCatalog,
      mvDataBase: String): Seq[CatalogTable] = {
    var res: Seq[CatalogTable] = Seq.empty[CatalogTable]
    try {
      val allTables = catalog.listTables(mvDataBase)
      res = catalog.getTablesByName(allTables).filter { tableData =>
        tableData.properties.contains(MV_QUERY_ORIGINAL_SQL)
      }
    } catch {
      // if db exists a table hive materialized view, will throw analysis exception
      case e: Throwable =>
        logDebug(s"Failed to listTables in $mvDataBase, errmsg: ${e.getMessage}")
        throw new UnsupportedOperationException("hive materialized view is not supported.")
    }
    res
  }

  /**
   * offset expression's exprId
   * origin exprId + NamedExpression.newExprId.id
   */
  def offsetExprId(plan: LogicalPlan): LogicalPlan = {
    val offset = NamedExpression.newExprId.id
    var maxId = offset
    val res = plan.transformAllExpressions {
      case alias: Alias =>
        val id = offset + alias.exprId.id
        maxId = Math.max(maxId, id)
        alias.copy()(exprId = alias.exprId.copy(id = id), qualifier = alias.qualifier,
          explicitMetadata = alias.explicitMetadata,
          nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys)
      case attr: AttributeReference =>
        val id = offset + attr.exprId.id
        maxId = Math.max(maxId, id)
        attr.copy()(exprId = attr.exprId.copy(id = id), qualifier = attr.qualifier)
      case e => e
    }
    val idField = NamedExpression.getClass.getDeclaredField("curId")
    idField.setAccessible(true)
    val id = idField.get(NamedExpression).asInstanceOf[AtomicLong]
    id.set(maxId)
    while (NamedExpression.newExprId.id <= maxId) {}
    res
  }

  /**
   * reassign exprId from 0 before save to file
   */
  def reassignExprId(plan: LogicalPlan): LogicalPlan = {
    val idMappings = mutable.HashMap[Long, Long]()
    var start = 0

    def mappingId(exprId: ExprId): Long = {
      val id = if (idMappings.contains(exprId.id)) {
        idMappings(exprId.id)
      } else {
        start += 1
        idMappings += (exprId.id -> start)
        start
      }
      id
    }

    plan.transformAllExpressions {
      case alias: Alias =>
        val id = mappingId(alias.exprId)
        alias.copy()(exprId = alias.exprId.copy(id = id), qualifier = alias.qualifier,
          explicitMetadata = alias.explicitMetadata,
          nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys)
      case attr: AttributeReference =>
        val id = mappingId(attr.exprId)
        attr.copy()(exprId = attr.exprId.copy(id = id), qualifier = attr.qualifier)
      case e => e
    }
  }

  /**
   * save mv metadata to file
   */
  def saveViewMetadataToFile(kryoSerializer: KryoSerializer, dbName: String,
      viewName: String): Unit = {
    val tablePlan = reassignExprId(viewToTablePlan.get(viewName))
    val queryPlan = reassignExprId(viewToViewQueryPlan.get(viewName))
    val properties = viewProperties.get(viewName)

    val jsons = mutable.Map[String, String]()

    val tablePlanStr = KryoSerDeUtil.serializePlan(kryoSerializer, tablePlan)
    jsons += ("tablePlan" -> tablePlanStr)

    val queryPlanStr = KryoSerDeUtil.serializePlan(kryoSerializer, queryPlan)
    jsons += ("queryPlan" -> queryPlanStr)

    val propertiesStr = KryoSerDeUtil.serializeToStr(kryoSerializer, properties)
    jsons += ("properties" -> propertiesStr)

    jsons += (MV_REWRITE_ENABLED -> properties(MV_REWRITE_ENABLED))

    saveMapToDisk(dbName, viewName, jsons, isAppend = false, lineFeed = false)
  }

  /**
   * save mv metadata to file
   */
  def saveViewMetadataToFile(dbName: String, viewName: String): Unit = {
    saveViewMetadataToFile(kryoSerializer, dbName, viewName)
    saveViewContainsTablesToFile(dbName, viewName)
  }

  /**
   * save view contains tables to file
   */
  def saveViewContainsTablesToFile(dbName: String, viewName: String): Unit = {
    val data = loadViewContainsTablesFromFile(dbName)
    data.put(viewName, (viewToContainsTables.get(viewName).map(_.tableName),
        System.currentTimeMillis()))
    saveMapToDisk(dbName, VIEW_CONTAINS_TABLES_FILE, data, isAppend = true, lineFeed = true)
  }

  /**
   * load view contains tables to file
   */
  def loadViewContainsTablesFromFile(): mutable.Map[String, (Set[String], Long)] = {
    val dbs = getDBs

    val jsons = mutable.Map[String, (Set[String], Long)]().empty
    dbs.foreach { db =>
      val properties = loadViewContainsTablesFromFile(db)
      for ((view, (tables, time)) <- properties) {
        if (!jsons.contains(view) || jsons(view)._2 < time) {
          jsons += (view -> (tables, time))
        }
      }
    }
    jsons
  }

  /**
   * load view contains tables to file
   */
  def loadViewContainsTablesFromFile(dbName: String): mutable.Map[String, (Set[String], Long)] = {
    val jsons = mutable.Map[String, (Set[String], Long)]().empty
    loadDataFromDisk(dbName, VIEW_CONTAINS_TABLES_FILE, isTailLines = true, jsons) {
      (preData, curData, modifyTime) =>
        for ((view, (tables, time)) <- curData) {
          if (!preData.contains(view) || preData(view)._2 < time) {
            preData += (view -> (tables, time))
          }
        }
    }
  }

  /**
   * load view priority from file
   */
  def loadViewPriorityFromFile(): Unit = {
    fs.listStatus(metadataPriorityPath)
        .sortWith((f1, f2) => f1.getModificationTime < f2.getModificationTime)
        .foreach { file =>
          val is = fs.open(file.getPath)
          val lines = JavaConverters
              .asScalaIteratorConverter(
                IOUtils.readLines(is, "UTF-8").iterator()).asScala.toSeq
          is.close()
          lines.foreach { line =>
            val views = line.split(",")
            var len = views.length
            views.foreach { view =>
              viewPriority.put(view, len)
              len -= 1
            }
          }
        }
  }

  /**
   * load metadata file when mv's db=omniCacheDB and mv exists
   * and when enableMetadataInitByQuery only load relate with query
   */
  def filterValidMetadata(): Array[FileStatus] = {
    val files = fs.listStatus(metadataPath).flatMap(x => fs.listStatus(x.getPath))
    val dbs = getDBs
    val dbTables = mutable.Set.empty[String]
    dbs.foreach { db =>
      if (spark.sessionState.catalog.databaseExists(db)) {
        dbTables ++= spark.sessionState.catalog.listTables(db).map(formatViewName)
      }
    }
    var res = files.filter { file =>
      dbTables.contains(file.getPath.getName)
    }

    if (OmniCachePluginConfig.getConf.enableMetadataInitByQuery && initQueryPlan.isDefined) {
      RewriteTime.withTimeStat("loadViewContainsTablesFromFile") {
        val queryTables = extractTablesOnly(initQueryPlan.get)
        val viewContainsTables = loadViewContainsTablesFromFile()
        res = res.filter { file =>
          val view = file.getPath.getName
          viewContainsTables.contains(view) && viewContainsTables(view)._1.subsetOf(queryTables)
        }
      }
    }

    res
  }


  /**
   * load mv metadata from file
   */
  def loadViewMetadataFromFile(): Unit = {
    if (!fs.exists(metadataPath)) {
      return
    }

    val files = RewriteTime.withTimeStat("listStatus") {
      filterValidMetadata()
    }

    val threadPool = RewriteTime.withTimeStat("threadPool") {
      Executors.newFixedThreadPool(Math.max(50, files.length * 2))
    }

    files.foreach { file =>
      threadPool.submit {
        new Runnable {
          override def run(): Unit = {
            val viewName = file.getPath.getName
            val is = fs.open(file.getPath)
            val jsons: Map[String, String] = RewriteTime.withTimeStat("Json.read.C") {
              Json(DefaultFormats).read[Map[String, String]](is)
            }
            is.close()

            if (!isViewEnable(jsons)) {
              return
            }

            val tablePlanStr = jsons("tablePlan")
            val tablePlan = RewriteTime.withTimeStat("deSerTablePlan.C") {
              KryoSerDeUtil.deserializePlan(kryoSerializer, spark, tablePlanStr)
            }
            viewToTablePlan.put(viewName, tablePlan)

            val propertiesStr = jsons("properties")
            val properties = RewriteTime.withTimeStat("deSerProperties.C") {
              KryoSerDeUtil.deserializeFromStr[Map[String, String]](kryoSerializer, propertiesStr)
            }
            viewProperties.put(viewName, properties)
          }
        }
      }

      threadPool.submit {
        new Runnable {
          override def run(): Unit = {
            val viewName = file.getPath.getName
            val is = fs.open(file.getPath)
            val jsons: Map[String, String] = RewriteTime.withTimeStat("Json.read.C") {
              Json(DefaultFormats).read[Map[String, String]](is)
            }
            is.close()

            if (!isViewEnable(jsons)) {
              return
            }

            val queryPlanStr = jsons("queryPlan")
            val queryPlan = RewriteTime.withTimeStat("deSerQueryPlan.C") {
              KryoSerDeUtil.deserializePlan(kryoSerializer, spark, queryPlanStr)
            }
            viewToViewQueryPlan.put(viewName, queryPlan)
          }
        }
      }
    }

    threadPool.shutdown()
    threadPool.awaitTermination(20, TimeUnit.SECONDS)

    viewProperties.keySet().forEach { viewName =>
      val tablePlan = viewToTablePlan.get(viewName)
      val queryPlan = viewToViewQueryPlan.get(viewName)

      val resignTablePlan = RewriteTime.withTimeStat("reSignExprId") {
        offsetExprId(tablePlan)
      }
      viewToTablePlan.put(viewName, resignTablePlan)

      val resignQueryPlan = RewriteTime.withTimeStat("reSignExprId") {
        offsetExprId(queryPlan)
      }
      viewToViewQueryPlan.put(viewName, resignQueryPlan)

      val (_, tables) = RewriteTime.withTimeStat("extractTables") {
        extractTables(resignQueryPlan)
      }
      viewToContainsTables.put(viewName, tables)

      RewriteTime.withTimeStat("tableToViews") {
        tables.foreach { table =>
          val name = table.tableName
          val views = tableToViews.getOrDefault(name, mutable.Set.empty)
          views += viewName
          tableToViews.put(name, views)
        }
      }
    }
  }

  /**
   * delete mv metadata from file
   */
  def deleteViewMetadata(identifier: TableIdentifier): Unit = {
    removeMVCache(identifier)
    val viewName = formatViewName(identifier)
    fs.delete(new Path(new Path(metadataPath, identifier.database.get), viewName), true)
  }

  /**
   * formatted mv name
   */
  def formatViewName(identifier: TableIdentifier): String = {
    identifier.toString().replace("`", "").toLowerCase(Locale.ROOT)
  }

  /**
   * is mv enable rewrite
   */
  def isViewEnable(jsons: Map[String, String]): Boolean = {
    jsons.contains(MV_REWRITE_ENABLED) && jsons(MV_REWRITE_ENABLED).toBoolean
  }

  /**
   * check mv metadata load complete
   */
  def checkViewMetadataComplete(): Unit = {
    val loadSize = viewToViewQueryPlan.size()
    var checkRes = true
    checkRes &&= (loadSize == viewToTablePlan.size())
    checkRes &&= (loadSize == viewToContainsTables.size())
    checkRes &&= (loadSize == viewProperties.size())
    if (!checkRes) {
      viewToViewQueryPlan.clear()
      viewToTablePlan.clear()
      viewToContainsTables.clear()
      viewProperties.clear()
      tableToViews.clear()
      viewProperties.clear()
    }
  }

  // Called when apply a MV rewrite.
  def saveViewCountToFile(): Unit = {
    val dbs = mutable.Set[String]()
    ViewMetadata.viewCnt.forEach {
      (name, _) =>
        dbs.add(name.split("\\.")(0))
    }
    for (db <- dbs) {
      saveViewCountToFile(db)
    }
  }

  // Called when creating a new MV.
  def saveViewCountToFile(dbName: String): Unit = {
    val data: mutable.Map[String, Array[Long]] = mutable.Map[String, Array[Long]]()
    ViewMetadata.viewCnt.forEach {
      (name, info) =>
        val db = name.split("\\.")(0)
        if (db.equals(dbName)) {
          data.put(name, info)
        }
    }
    saveMapToDisk(dbName, VIEW_CNT_FILE, data, isAppend = false, lineFeed = false)
  }

  def loadViewCount(): Unit = {
    val dbs = getDBs
    dbs.foreach {
      db =>
        loadViewCount(db)
    }
  }

  def loadViewCount(dbName: String): Unit = {
    // clear viewCnt info in dbName
    val iterator = viewCnt.entrySet.iterator
    while (iterator.hasNext) {
      val entry = iterator.next
      if (entry.getKey.split("\\.")(0) equals dbName) iterator.remove
    }

    val viewCounts = mutable.Map[String, Array[Long]]().empty
    viewCounts ++= loadDataFromDisk(dbName, VIEW_CNT_FILE, isTailLines = true, viewCounts) {
      (preData, newData, modifyTime) =>
        for (data <- newData) {
          val dataWithModifyTime = (data._1, data._2.slice(0, 2) ++ Array(modifyTime))
          preData += dataWithModifyTime
        }
    }

    // set view count into ViewMetadata.viewCnt
    for (viewCount <- viewCounts) {
      viewCnt.put(viewCount._1, viewCount._2)
    }
  }

  /**
   * load data from disk.
   *
   * @param dbName   Which directory in the metadata stores this data.
   * @param fileName Which file in the metadata directory stores this data.
   * @param isTailLines
   * @param data     Data to be stored and data is of type Map.
   * @tparam K is the type of key for the Map
   * @tparam V V is the type of value for the Map
   * @return
   */
  private def loadDataFromDisk[K: Manifest, V: Manifest](
      dbName: String,
      fileName: String,
      isTailLines: Boolean,
      data: mutable.Map[K, V])
      (addNewDataToPreData: (
          mutable.Map[K, V],
              mutable.Map[K, V],
              Long) => Unit): mutable.Map[K, V] = {

    val dbPath = new Path(metadataPath, dbName)
    val filePath = new Path(dbPath, fileName)
    loadMapFromDisk(filePath, isTailLines, data)(addNewDataToPreData)
  }

  private def loadMapFromDisk[K: Manifest, V: Manifest](
      filePath: Path,
      isTailLines: Boolean,
      data: mutable.Map[K, V])
      (addNewDataToPreData: (
          mutable.Map[K, V],
              mutable.Map[K, V],
              Long) => Unit): mutable.Map[K, V] = {
    val newData = data.empty
    if (!fs.exists(filePath)) {
      return newData
    }
    var readLines = OmniCachePluginConfig.getConf.metadataIndexTailLines
    val is = fs.open(filePath)
    var pos = fs.getFileStatus(filePath).getLen - 1
    val modifyTime = fs.getFileStatus(filePath).getModificationTime
    var lineReady = false
    var bytes = mutable.Seq.empty[Char]
    // tail the file
    while (pos >= 0) {
      is.seek(pos)
      val readByte = is.readByte()
      readByte match {
        // \n
        case SEPARATOR =>
          if (bytes.size != 0) {
            lineReady = true
          }
        case _ =>
          bytes +:= readByte.toChar
      }
      pos -= 1

      // find \n or file start
      if (lineReady || pos < 0) {
        val line = bytes.mkString("")
        val properties = Json(DefaultFormats)
            .read[mutable.Map[K, V]](line)
        addNewDataToPreData(newData, properties, modifyTime)
        lineReady = false
        bytes = mutable.Seq.empty[Char]

        if (isTailLines) {
          readLines -= 1
          if (readLines <= 0) {
            return newData
          }
        }
      }
    }
    is.close()
    newData
  }

  private def loadStrFromDisk(filePath: Path): String = {
    if (!fs.exists(filePath)) {
      return ""
    }
    val in = fs.open(filePath)
    val ciphertext = IOUtils.toByteArray(in).map(_.toChar).mkString("")
    in.close()
    ciphertext
  }

  /**
   * save data to disk.
   * Metadata information is classified by DBNames.
   *
   * @param dbName   Which directory in the metadata stores this data.
   * @param fileName Which file in the metadata directory stores this data.
   * @param data     Data to be stored and data is of type Map.
   * @tparam K K is the type of key for the Map
   * @tparam V V is the type of value for the Map
   */
  def saveMapToDisk[K: Manifest, V: Manifest](
      dbName: String,
      fileName: String,
      data: mutable.Map[K, V],
      isAppend: Boolean,
      lineFeed: Boolean): Unit = {
    val dbPath = new Path(metadataPath, dbName)
    val file = new Path(dbPath, fileName)
    val os = if (!fs.exists(file) || !isAppend || fs.isInstanceOf[LocalFileSystem]) {
      fs.create(file, true)
    } else {
      fs.append(file)
    }
    // append
    val jsonFile = Json(DefaultFormats).write(data)
    os.write(jsonFile.getBytes())
    // line feed
    if (lineFeed) {
      os.write(SEPARATOR)
    }
    os.close()
  }

  private def saveStrToDisk(
      file: Path,
      data: String,
      isAppend: Boolean): Unit = {
    val os = if (!fs.exists(file) || !isAppend || fs.isInstanceOf[LocalFileSystem]) {
      fs.create(file, true)
    } else {
      fs.append(file)
    }
    IOUtils.write(data, os)
    os.close()
  }

  /**
   * If "spark.sql.omnicache.dbs" specifies databases,
   * the databases are used.
   * Otherwise, all databases in the metadata directory are obtained by default.
   *
   * @return
   */
  def getDBs: Set[String] = {
    if (OmniCachePluginConfig.getConf.omniCacheDB.nonEmpty) {
      OmniCachePluginConfig.getConf.omniCacheDB
          .split(",").map(_.toLowerCase(Locale.ROOT)).toSet
    } else {
      fs.listStatus(metadataPath).map(_.getPath.getName).toSet
    }
  }

  // just for test.
  def getViewCntPath: String = {
    VIEW_CNT_FILE
  }

  def getDefaultDatabase: String = {
    DEFAULT_DATABASE
  }

  def saveWashOutTimestamp(): Unit = {
    val map = mutable.Map[String, Long]()
    if (washOutTimestamp.isDefined) {
      map += ("washOutTimestamp" -> washOutTimestamp.get)
    }
    val str = KryoSerDeUtil.serializeToStr(kryoSerializer, map)
    saveStrToDisk(new Path(metadataPath, WASH_OUT_TIMESTAMP), str, isAppend = false)
  }

  def loadWashOutTimestamp(): Unit = {
    val ciphertext = loadStrFromDisk(new Path(metadataPath, WASH_OUT_TIMESTAMP))
    val timestamp = KryoSerDeUtil.deserializeFromStr[mutable.Map[String, Long]](
      kryoSerializer, ciphertext)
    if (timestamp != null) {
      washOutTimestamp = timestamp.get(WASH_OUT_TIMESTAMP)
    }
  }

  def getViewCntModifyTime(viewCnt: ConcurrentHashMap[String, Array[Long]]): Option[Long] = {
    viewCnt.forEach {
      (_, value) =>
        return Some(value(2))
    }
    Option.empty
  }

  def getViewDependsTableTime(viewName: String): Map[String, String] = {
    var catalogTables: Set[CatalogTable] = Set()
    viewToContainsTables.get(viewName).map(_.logicalPlan)
        .foreach(plan => catalogTables ++= extractCatalogTablesOnly(plan))
    getViewDependsTableTime(catalogTables)
  }

  def getViewDependsTableTime(catalogTables: Set[CatalogTable]): Map[String, String] = {
    var viewDependsTableTime = Map[String, String]()
    catalogTables.foreach { catalogTable =>
      viewDependsTableTime += (formatViewName(catalogTable.identifier) ->
          getPathTime(catalogTable.storage.locationUri.get).toString)
    }
    viewDependsTableTime
  }

  def getViewDependsTableTimeStr(viewQueryPlan: LogicalPlan): String = {
    val str: String = Json(DefaultFormats).write(
      getViewDependsTableTime(extractCatalogTablesOnly(viewQueryPlan)))
    str
  }

  def getLastViewDependsTableTime(viewName: String): Map[String, String] = {
    Json(DefaultFormats).read[Map[String, String]](
      viewProperties.get(viewName)(MV_LATEST_UPDATE_TIME))
  }

  def getPathTime(uri: URI): Long = {
    fs.getFileStatus(new Path(uri)).getModificationTime
  }

  def checkViewDataReady(viewName: String): Unit = {
    if (REFRESH_STAT equals BUSY) {
      return
    }
    val lastTime = getLastViewDependsTableTime(viewName)
    val nowTime = getViewDependsTableTime(viewName)
    if (lastTime != nowTime) {
      REFRESH_STAT = BUSY
      RewriteTime.withTimeStat("REFRESH MV") {
        val sqlText = spark.sparkContext.getLocalProperty(SPARK_JOB_DESCRIPTION)
        RewriteHelper.enableSqlLog()
        spark.sql(s"REFRESH MATERIALIZED VIEW $viewName;")
        RewriteHelper.disableSqlLog()
        spark.sparkContext.setJobDescription(sqlText)
        val newProperty = ViewMetadata.viewProperties.get(viewName) +
            (MV_LATEST_UPDATE_TIME -> Json(DefaultFormats).write(nowTime))
        ViewMetadata.viewProperties.put(viewName, newProperty)
        val viewDB = viewName.split("\\.")(0)
        saveViewMetadataToFile(viewDB, viewName)
      }
      val updateReason = nowTime.toSeq.filter { kv =>
        !lastTime.contains(kv._1) || lastTime(kv._1) != kv._2
      }.toString()
      logBasedOnLevel(s"REFRESH MATERIALIZED VIEW $viewName; " +
          s"for depends table has updated $updateReason")
      REFRESH_STAT = IDLE
    }
  }
}
