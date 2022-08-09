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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.{SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.execution.SparkOptimizer
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.types.StructType

object OmniCacheToSparkAdapter extends SQLConfHelper with Logging {

  def buildCatalogTable(
      table: TableIdentifier,
      schema: StructType,
      partitioning: Seq[String],
      bucketSpec: Option[BucketSpec],
      properties: Map[String, String],
      provider: String,
      location: Option[String],
      comment: Option[String],
      storageFormat: CatalogStorageFormat,
      external: Boolean): CatalogTable = {
    if (external) {
      if (DDLUtils.isHiveTable(Some(provider))) {
        if (location.isEmpty) {
          throw new AnalysisException(s"CREATE EXTERNAL TABLE must be accompanied by LOCATION")
        }
      } else {
        throw new AnalysisException(s"Operation not allowed: CREATE EXTERNAL TABLE ... USING")
      }
    }

    val tableType = if (location.isDefined) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }

    CatalogTable(
      identifier = table,
      tableType = tableType,
      storage = storageFormat,
      schema = schema,
      provider = Some(provider),
      partitionColumnNames = partitioning,
      bucketSpec = bucketSpec,
      properties = properties,
      comment = comment)
  }

  def getStorageFormatAndProvider(
      provider: String,
      options: Map[String, String],
      location: Option[String]): (CatalogStorageFormat, String) = {
    val nonHiveStorageFormat = CatalogStorageFormat.empty.copy(
      locationUri = location.map(CatalogUtils.stringToURI),
      properties = options)
    (nonHiveStorageFormat, provider)
  }
}

case class OmniCacheOptimizer(session: SparkSession, optimizer: Optimizer) extends
    SparkOptimizer(session.sessionState.catalogManager,
      session.sessionState.catalog,
      session.sessionState.experimentalMethods) {

  private lazy val mvRules = Seq(Batch("Materialized View Optimizers", Once,
    Seq(): _*))
  // Seq(new MVRewriteRule(session)): _*))

  override def defaultBatches: Seq[Batch] = {
    mvRules ++ super.defaultBatches
  }
}
