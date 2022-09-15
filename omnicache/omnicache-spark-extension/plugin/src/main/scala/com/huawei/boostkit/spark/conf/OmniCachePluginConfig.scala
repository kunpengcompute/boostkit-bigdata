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

  // database where create OmniCache
  val omniCacheDB: String = conf
      .getConfString("spark.sql.omnicache.db", "default")

  // rewrite cur match mv
  def curMatchMV: String = conf
      .getConfString("spark.sql.omnicache.cur.match.mv", "")

  def setCurMatchMV(mv: String): Unit = {
    conf.setConfString("spark.sql.omnicache.cur.match.mv", mv)
  }

  val defaultDataSource: String = conf
      .getConfString("spark.sql.omnicache.default.datasource", "orc")

  val dataSourceSet: Set[String] = Set("orc", "parquet")
}

object OmniCachePluginConfig {

  val MV_REWRITE_ENABLED = "spark.omnicache.rewrite.enable"

  val MV_UPDATE_REWRITE_ENABLED = "spark.omnicache.update.rewrite.enable"

  val MV_QUERY_ORIGINAL_SQL = "spark.omnicache.query.sql.original"

  val MV_QUERY_ORIGINAL_SQL_CUR_DB = "spark.omnicache.query.sql.cur.db"

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

  def isMV(catalogTable: CatalogTable): Boolean = {
    catalogTable.properties.contains(MV_QUERY_ORIGINAL_SQL)
  }

  def isMVInUpdate(spark: SparkSession, quotedMvName: String): Boolean = {
    val names = quotedMvName.replaceAll("`", "")
        .split("\\.").toSeq
    val mv = TableIdentifier(names(1), Some(names.head))
    val catalogTable = spark.sessionState.catalog.getTableMetadata(mv)
    !catalogTable.properties.getOrElse(MV_UPDATE_REWRITE_ENABLED, "true").toBoolean
  }

  def isMVInUpdate(viewTablePlan: LogicalPlan): Boolean = {
    val logicalRelation = viewTablePlan.asInstanceOf[LogicalRelation]
    !logicalRelation.catalogTable.get
        .properties.getOrElse(MV_UPDATE_REWRITE_ENABLED, "true").toBoolean
  }
}
