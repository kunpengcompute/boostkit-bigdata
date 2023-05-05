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

class OmniMVPluginConfig(conf: SQLConf) {

  // enable or disable OmniMV
  def enableOmniMV: Boolean = conf
      .getConfString("spark.sql.omnimv.enable", "true").toBoolean

  // show mv querySql length
  def showMVQuerySqlLen: Int = conf
      .getConfString("spark.sql.omnimv.show.length", "50").toInt

  // database where create OmniMV, like omnimv,omnimv1
  def omniMVDB: String = conf
      .getConfString("spark.sql.omnimv.dbs", "")

  // rewrite cur match mv
  def curMatchMV: String = conf
      .getConfString("spark.sql.omnimv.cur.match.mv", "")

  def setCurMatchMV(mv: String): Unit = {
    conf.setConfString("spark.sql.omnimv.cur.match.mv", mv)
  }

  // mv table datasource
  def defaultDataSource: String = conf
      .getConfString("spark.sql.omnimv.default.datasource", "orc")

  val dataSourceSet: Set[String] = Set("orc", "parquet")

  // omnimv loglevel
  def logLevel: String = conf
      .getConfString("spark.sql.omnimv.logLevel", "DEBUG")
      .toUpperCase(Locale.ROOT)

  // set parsed sql as JobDescription
  def enableSqlLog: Boolean = conf
      .getConfString("spark.sql.omnimv.log.enable", "true")
      .toBoolean

  // omnimv metadata path
  def metadataPath: String = conf
      .getConfString("spark.sql.omnimv.metadata.path", "/user/omnimv/metadata")

  // enable omnimv init by query
  def enableMetadataInitByQuery: Boolean = conf
      .getConfString("spark.sql.omnimv.metadata.initbyquery.enable", "false")
      .toBoolean

  // metadata index tail lines
  def metadataIndexTailLines: Long = conf
      .getConfString("spark.sql.omnimv.metadata.index.tail.lines", "5")
      .toLong

  // Minimum unused time required for wash out. The default unit is "day".
  def minimumUnusedDaysForWashOut: Int = conf
      .getConfString("spark.sql.omnimv.washout.unused.day", "30")
      .toInt

  // The number of materialized views to be reserved.
  def reserveViewQuantityByViewCount: Int = conf
      .getConfString("spark.sql.omnimv.washout.reserve.quantity.byViewCnt", "25")
      .toInt

  def dropViewQuantityBySpaceConsumed: Int = conf
      .getConfString("spark.sql.omnimv.washout.drop.quantity.bySpaceConsumed", "3")
      .toInt

  // The default unit is "day".
  def autoWashOutTimeInterval: Int = conf
      .getConfString("spark.sql.omnimv.washout.automatic.time.interval", "35")
      .toInt

  // Check "auto wash out" at intervals during the same session. The default unit is "second".
  def autoCheckWashOutTimeInterval: Int = conf
      .getConfString("spark.sql.omnimv.washout.automatic.checkTime.interval", "3600")
      .toInt

  // The minimum number of views that trigger automatic wash out.
  def automaticWashOutMinimumViewQuantity: Int = conf
      .getConfString("spark.sql.omnimv.washout.automatic.view.quantity", "20")
      .toInt

  def enableAutoWashOut: Boolean = conf
      .getConfString("spark.sql.omnimv.washout.automatic.enable", "false")
      .toBoolean

}

object OmniMVPluginConfig {
  // mv if enable for rewrite
  val MV_REWRITE_ENABLED = "spark.omnimv.rewrite.enable"

  // mv if enable for rewrite when update
  val MV_UPDATE_REWRITE_ENABLED = "spark.omnimv.update.rewrite.enable"

  // mv query original sql
  val MV_QUERY_ORIGINAL_SQL = "spark.omnimv.query.sql.original"

  // mv query original sql exec db
  val MV_QUERY_ORIGINAL_SQL_CUR_DB = "spark.omnimv.query.sql.cur.db"

  // mv latest update time
  val MV_LATEST_UPDATE_TIME = "spark.omnimv.latest.update.time"

  // spark job descriptor
  val SPARK_JOB_DESCRIPTION = "spark.job.description"

  var ins: Option[OmniMVPluginConfig] = None

  def getConf: OmniMVPluginConfig = synchronized {
    if (ins.isEmpty) {
      ins = Some(getSessionConf)
    }
    ins.get
  }

  def getSessionConf: OmniMVPluginConfig = {
    new OmniMVPluginConfig(SQLConf.get)
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
