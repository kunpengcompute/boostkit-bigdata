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

import com.huawei.boostkit.spark.conf.OmniCachePluginConfig._
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.optimizer.rules.RewriteTime
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, RepartitionByExpression, SubqueryAlias}

object ViewMetadata extends RewriteHelper {

  val viewToViewQueryPlan = new ConcurrentHashMap[String, LogicalPlan]()

  val viewToTablePlan = new ConcurrentHashMap[String, LogicalPlan]()

  val viewToContainsTables = new ConcurrentHashMap[String, Set[TableEqual]]()

  val tableToViews = new ConcurrentHashMap[String, mutable.Set[String]]()

  var spark: SparkSession = _

  val STATUS_UN_LOAD = "UN_LOAD"
  val STATUS_LOADING = "LOADING"
  val STATUS_LOADED = "LOADED"

  var status: String = STATUS_UN_LOAD

  def setSpark(sparkSession: SparkSession): Unit = {
    spark = sparkSession
    status = STATUS_LOADING
  }

  def saveViewMetadataToMap(catalogTable: CatalogTable): Unit = this.synchronized {
    // if QUERY_REWRITE_ENABLED is false, doesn't load ViewMetadata
    if (!catalogTable.properties.getOrElse(MV_REWRITE_ENABLED, "false").toBoolean) {
      return
    }

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
            spark.sql(viewQuerySql).queryExecution.analyzed
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
      val viewName = catalogTable.identifier.toString()

      // mappedViewQueryPlan and mappedViewContainsTables
      val (mappedViewQueryPlan, mappedViewContainsTables) = RewriteTime
          .withTimeStat("extractTables") {
            extractTables(viewQueryPlan)
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
    } catch {
      case e: Throwable =>
        logDebug(s"Failed to saveViewMetadataToMap,errmsg: ${e.getMessage}")
        // reset preDatabase
        spark.sessionState.catalogManager.setCurrentNamespace(Array(preDatabase))
    }
  }

  def isEmpty: Boolean = {
    viewToTablePlan.isEmpty
  }

  def isViewExists(viewIdentifier: String): Boolean = {
    viewToTablePlan.containsKey(viewIdentifier)
  }

  def addCatalogTableToCache(table: CatalogTable): Unit = this.synchronized {
    saveViewMetadataToMap(table)
  }

  def removeMVCache(tableName: TableIdentifier): Unit = this.synchronized {
    val viewName = tableName.toString()
    viewToContainsTables.remove(viewName)
    viewToViewQueryPlan.remove(viewName)
    viewToTablePlan.remove(viewName)
    tableToViews.forEach { (key, value) =>
      if (value.contains(viewName)) {
        value -= viewName
        tableToViews.put(key, value)
      }
    }
  }

  def init(sparkSession: SparkSession): Unit = {
    if (status == STATUS_LOADED) {
      return
    }

    setSpark(sparkSession)
    forceLoad()
    status = STATUS_LOADED
  }

  def forceLoad(): Unit = this.synchronized {
    val catalog = spark.sessionState.catalog

    // load from all db
    for (db <- catalog.listDatabases()) {
      val tables = RewriteTime.withTimeStat("loadTable") {
        omniCacheFilter(catalog, db)
      }
      RewriteTime.withTimeStat("saveViewMetadataToMap") {
        tables.foreach(tableData => saveViewMetadataToMap(tableData))
      }
    }
  }

  def omniCacheFilter(catalog: SessionCatalog,
      mvDataBase: String): Seq[CatalogTable] = {
    try {
      val allTables = catalog.listTables(mvDataBase)
      catalog.getTablesByName(allTables).filter { tableData =>
        tableData.properties.contains(MV_QUERY_ORIGINAL_SQL)
      }
    } catch {
      // if db exists a table hive materialized view, will throw analysis exception
      case e: Throwable =>
        logDebug(s"Failed to listTables in $mvDataBase, errmsg: ${e.getMessage}")
        Seq.empty[CatalogTable]
    }
  }
}
