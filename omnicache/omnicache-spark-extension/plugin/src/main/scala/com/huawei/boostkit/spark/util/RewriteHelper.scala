package com.huawei.boostkit.spark.util

import com.huawei.boostkit.spark.conf.OmniCachePluginConfig
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

class RewriteHelper {

}


object RewriteHelper {
  def containsMV(logicalPlan: LogicalPlan): Boolean = {
    logicalPlan.foreachUp {
      case _@HiveTableRelation(tableMeta, _, _, _, _) =>
        if (OmniCachePluginConfig.isMV(tableMeta)) {
          return true
        }
      case _@LogicalRelation(_, _, catalogTable, _) =>
        if (catalogTable.isDefined) {
          if (OmniCachePluginConfig.isMV(catalogTable.get)) {
            return true
          }
        }
      case _ =>
    }
    false
  }
}