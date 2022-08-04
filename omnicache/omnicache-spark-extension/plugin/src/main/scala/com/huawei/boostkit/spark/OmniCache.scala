package com.huawei.boostkit.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.parser.OmniCacheExtensionSqlParser

class OmniCache extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {

    extensions.injectParser { case (spark, parser) =>
      new OmniCacheExtensionSqlParser(spark, parser)
    }
  }
}

