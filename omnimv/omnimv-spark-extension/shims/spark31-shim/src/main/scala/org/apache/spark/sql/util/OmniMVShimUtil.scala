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

package org.apache.spark.sql.util

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.AlterTableRecoverPartitionsCommand
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.{DataSource, FileIndex, HadoopFsRelation}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.sources.BaseRelation

object OmniMVShimUtil {
  def dataSourceWriteAndRead(
      dataSource: DataSource,
      mode: SaveMode,
      query: LogicalPlan,
      outputColumnNames: Seq[String],
      physicalPlan: SparkPlan,
      metrics: Map[String, SQLMetric]): BaseRelation = {
    dataSource.writeAndRead(mode, query, outputColumnNames, physicalPlan)
  }

  def checkColumnNameDuplication(
      outputColumnNames: Seq[String], colType: String, caseSensitiveAnalysis: Boolean): Unit = {
    SchemaUtils.checkColumnNameDuplication(
      outputColumnNames,
      colType,
      caseSensitiveAnalysis)
  }

  def createMVExecutePlan(
      sessionState: SessionState,
      table: CatalogTable): QueryExecution = {
    sessionState.executePlan(AlterTableRecoverPartitionsCommand(table.identifier))
  }

  def catalogIsTemporaryTable(catalog: SessionCatalog, tableName: TableIdentifier): Boolean = {
    catalog.isTemporaryTable(tableName)
  }

  def getAstBuilderCreateMVPlan(queryExecution: QueryExecution): LogicalPlan = {
    queryExecution.optimizedPlan
  }

  def getAstBuilderRefreshMVFileIndexs(sparkPlan: SparkPlan): Seq[FileIndex] = {
    sparkPlan
        .collect {
          case FileSourceScanExec(relation, _, _, _, _, _, _, _, _)
          => relation.location
          case RowDataSourceScanExec(_, _, _, _, _, relation: HadoopFsRelation, _)
          => relation.location
        }
  }
}
