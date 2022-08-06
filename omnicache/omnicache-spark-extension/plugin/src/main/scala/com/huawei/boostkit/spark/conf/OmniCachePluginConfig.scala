package com.huawei.boostkit.spark.conf

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.internal.SQLConf

class OmniCachePluginConfig(conf: SQLConf) {

  def enableOmniCache: Boolean = conf
      .getConfString("spark.sql.omnicache.enable", "true").toBoolean

  def showMVQuerySqlLen: Int = conf
      .getConfString("spark.sql.omnicache.show.length", "50").toInt

  val omniCacheDB: String = conf
      .getConfString("spark.sql.omnicache.db", "default")

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

  val MV_QUERY_ORIGINAL_SQL = "spark.omnicache.query.sql.original"

  val MV_QUERY_ORIGINAL_SQL_CUR_DB = "spark.omnicache.query.sql.cur.db"

  val MV_LATEST_UPDATE_TIME = "spark.omnicache.latest.update.time"

  var ins: OmniCachePluginConfig = null

  def getConf: OmniCachePluginConfig = synchronized {
    if (ins == null) {
      ins = getSessionConf
    }
    ins
  }

  def getSessionConf: OmniCachePluginConfig = {
    new OmniCachePluginConfig(SQLConf.get)
  }

  def isMV(catalogTable: CatalogTable): Boolean = {
    catalogTable.properties.contains(MV_QUERY_ORIGINAL_SQL)
  }
}
