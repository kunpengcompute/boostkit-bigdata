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

package com.huawei.boostkit.spark.conf

import java.util.Locale

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf

class OmniCachePluginConfig(conf: SQLConf) {

  // enable or disable OmniCache
  def enableOmniCache: Boolean = conf
      .getConfString("spark.sql.omnicache.enable", "true").toBoolean

  // show mv querySql length
  def showMVQuerySqlLen: Int = conf
      .getConfString("spark.sql.omnicache.show.length", "50").toInt

  // database where create OmniCache, like omnicache,omnicache1
  def omniCacheDB: String = conf
      .getConfString("spark.sql.omnicache.dbs", "")

  // rewrite cur match mv
  def curMatchMV: String = conf
      .getConfString("spark.sql.omnicache.cur.match.mv", "")

  def setCurMatchMV(mv: String): Unit = {
    conf.setConfString("spark.sql.omnicache.cur.match.mv", mv)
  }

  // mv table datasource
  def defaultDataSource: String = conf
      .getConfString("spark.sql.omnicache.default.datasource", "orc")

  val dataSourceSet: Set[String] = Set("orc", "parquet")

  // omnicache loglevel
  def logLevel: String = conf
      .getConfString("spark.sql.omnicache.logLevel", "DEBUG")
      .toUpperCase(Locale.ROOT)

  // set parsed sql as JobDescription
  def enableSqlLog: Boolean = conf
      .getConfString("spark.sql.omnicache.log.enable", "false")
      .toBoolean

  // omnicache metadata path
  def metadataPath: String = conf
      .getConfString("spark.sql.omnicache.metadata.path", "/user/omnicache/metadata")

  // enable omnicache init by query
  def enableMetadataInitByQuery: Boolean = conf
      .getConfString("spark.sql.omnicache.metadata.initbyquery.enable", "false")
      .toBoolean

  // metadata index tail lines
  def metadataIndexTailLines: Long = conf
      .getConfString("spark.sql.omnicache.metadata.index.tail.lines", "5")
      .toLong

  // Minimum unused time required for wash out. The default unit is "day".
  def minimumUnusedDaysForWashOut: Int = conf
      .getConfString("spark.sql.omnicache.washout.unused.day", "30")
      .toInt

  // The number of materialized views to be reserved.
  def reserveViewQuantityByViewCount: Int = conf
      .getConfString("spark.sql.omnicache.washout.reserve.quantity.byViewCnt", "25")
      .toInt

  def dropViewQuantityBySpaceConsumed: Int = conf
      .getConfString("spark.sql.omnicache.washout.drop.quantity.bySpaceConsumed", "3")
      .toInt

  // The default unit is "day".
  def autoWashOutTimeInterval: Int = conf
      .getConfString("spark.sql.omnicache.washout.automatic.time.interval", "35")
      .toInt

  // Check "auto wash out" at intervals during the same session. The default unit is "second".
  def autoCheckWashOutTimeInterval: Int = conf
      .getConfString("spark.sql.omnicache.washout.automatic.checkTime.interval", "3600")
      .toInt

  // The minimum number of views that trigger automatic wash out.
  def automaticWashOutMinimumViewQuantity: Int = conf
      .getConfString("spark.sql.omnicache.washout.automatic.view.quantity", "20")
      .toInt

  def enableAutoWashOut: Boolean = conf
      .getConfString("spark.sql.omnicache.washout.automatic.enable", "false")
      .toBoolean

}

object OmniCachePluginConfig {
  // mv if enable for rewrite
  val MV_REWRITE_ENABLED = "spark.omnicache.rewrite.enable"

  // mv if enable for rewrite when update
  val MV_UPDATE_REWRITE_ENABLED = "spark.omnicache.update.rewrite.enable"

  // mv query original sql
  val MV_QUERY_ORIGINAL_SQL = "spark.omnicache.query.sql.original"

  // mv query original sql exec db
  val MV_QUERY_ORIGINAL_SQL_CUR_DB = "spark.omnicache.query.sql.cur.db"

  // mv latest update time
  val MV_LATEST_UPDATE_TIME = "spark.omnicache.latest.update.time"

  var ins: Option[OmniCachePluginConfig] = None

  def getConf: OmniCachePluginConfig = synchronized {
    if (ins.isEmpty) {
      ins = Some(getSessionConf)
    }
    ins.get
  }

  def getSessionConf: OmniCachePluginConfig = {
    new OmniCachePluginConfig(SQLConf.get)
  }

  /**
   *
   * check if table is mv
   *
   * @param catalogTable catalogTable
   * @return true:is mv; false:is not mv
   */
  def isMV(catalogTable: CatalogTable): Boolean = {
    catalogTable.properties.contains(MV_QUERY_ORIGINAL_SQL)
  }

  /**
   * check if mv is in update
   *
   * @param spark        spark
   * @param quotedMvName quotedMvName
   * @return true:is in update; false:is not in update
   */
  def isMVInUpdate(spark: SparkSession, quotedMvName: String): Boolean = {
    val names = quotedMvName.replaceAll("`", "")
        .split("\\.").toSeq
    val mv = TableIdentifier(names(1), Some(names.head))
    val catalogTable = spark.sessionState.catalog.getTableMetadata(mv)
    !catalogTable.properties.getOrElse(MV_UPDATE_REWRITE_ENABLED, "true").toBoolean
  }

  /**
   * check if mv is in update
   *
   * @param viewTablePlan viewTablePlan
   * @return true:is in update; false:is not in update
   */
  def isMVInUpdate(viewTablePlan: LogicalPlan): Boolean = {
    val logicalRelation = viewTablePlan.asInstanceOf[LogicalRelation]
    !logicalRelation.catalogTable.get
        .properties.getOrElse(MV_UPDATE_REWRITE_ENABLED, "true").toBoolean
  }
}
