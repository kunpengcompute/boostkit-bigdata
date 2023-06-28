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

package org.apache.spark.sql.hive.thriftserver

import java.io.PrintStream
import java.nio.charset.StandardCharsets.UTF_8

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.hive.{HiveExternalCatalog, HiveUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/** A singleton object for the master program. The executors should not access this. */
private[hive] object SparkSQLEnv extends Logging {
  logDebug("Initializing SparkSQLEnv")

  var sqlContext: SQLContext = _
  var sparkContext: SparkContext = _

  def init(): Unit = {
    if (sqlContext == null) {
      val sparkConf = new SparkConf(loadDefaults = true)
      // If user doesn't specify the appName, we want to get [SparkSQL::localHostName] instead of
      // the default appName [SparkSQLCLIDriver] in cli or beeline.
      val maybeAppName = sparkConf
          .getOption("spark.app.name")
          .filterNot(_ == classOf[SparkSQLCLIDriver].getName)
          // .filterNot(_ == classOf[HiveThriftServer2].getName)
          .filterNot(_ == "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2")

      sparkConf
          .setAppName(maybeAppName.getOrElse(s"SparkSQL::${Utils.localHostName()}"))
          .set(SQLConf.DATETIME_JAVA8API_ENABLED, true)


      val sparkSession = SparkSession.builder()
          .config(sparkConf)
          .config(HiveUtils.BUILTIN_HIVE_VERSION.key, HiveUtils.builtinHiveVersion)
          .enableHiveSupport().getOrCreate()
      sparkContext = sparkSession.sparkContext
      sqlContext = sparkSession.sqlContext

      // SPARK-29604: force initialization of the session state with the Spark class loader,
      // instead of having it happen during the initialization of the Hive client (which may use a
      // different class loader).
      sparkSession.sessionState

      val metadataHive = sparkSession
          .sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client
      metadataHive.setOut(new PrintStream(System.out, true, UTF_8.name()))
      metadataHive.setInfo(new PrintStream(System.err, true, UTF_8.name()))
      metadataHive.setError(new PrintStream(System.err, true, UTF_8.name()))
    }
  }

  /** Cleans up and shuts down the Spark SQL environments. */
  def stop(): Unit = {
    logDebug("Shutting down Spark SQL Environment")
    // Stop the SparkContext
    if (SparkSQLEnv.sparkContext != null) {
      sparkContext.stop()
      sparkContext = null
      sqlContext = null
    }
  }
}
