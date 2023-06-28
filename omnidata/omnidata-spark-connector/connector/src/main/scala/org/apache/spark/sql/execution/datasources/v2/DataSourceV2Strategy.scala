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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.JavaConverters._
import org.apache.spark.sql.{AnalysisException, Dataset, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.analysis.{ResolvedNamespace, ResolvedPartitionSpec, ResolvedTable}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, DynamicPruning, Expression, NamedExpression, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.{Filter => LFilter}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.FilterEstimation
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, StagingTableCatalog, SupportsNamespaces, SupportsPartitionManagement, SupportsWrite, Table, TableCapability, TableCatalog, TableChange}
import org.apache.spark.sql.connector.read.LocalScan
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.connector.write.V1Write
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.{FilterExec, LeafExecNode, LocalTableScanExec, ProjectExec, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.ndp.NdpFilterEstimation
import org.apache.spark.sql.execution.streaming.continuous.{WriteToContinuousDataSource, WriteToContinuousDataSourceExec}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.storage.StorageLevel

class DataSourceV2Strategy(session: SparkSession) extends Strategy with PredicateHelper {

  import DataSourceV2Implicits._
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  private def withProjectAndFilter(
      project: Seq[NamedExpression],
      filters: Seq[Expression],
      scan: LeafExecNode,
      needsUnsafeConversion: Boolean,
      selectivity: Option[Double]): SparkPlan = {
    val filterCondition = filters.reduceLeftOption(And)
    val withFilter = filterCondition.map(FilterExec(_, scan, selectivity)).getOrElse(scan)

    if (withFilter.output != project || needsUnsafeConversion) {
      ProjectExec(project, withFilter)
    } else {
      withFilter
    }
  }

  private def refreshCache(r: DataSourceV2Relation)(): Unit = {
    session.sharedState.cacheManager.recacheByPlan(session, r)
  }

  private def recacheTable(r: ResolvedTable)(): Unit = {
    val v2Relation = DataSourceV2Relation.create(r.table, Some(r.catalog), Some(r.identifier))
    session.sharedState.cacheManager.recacheByPlan(session, v2Relation)
  }

  // Invalidates the cache associated with the given table. If the invalidated cache matches the
  // given table, the cache's storage level is returned.
  private def invalidateTableCache(r: ResolvedTable)(): Option[StorageLevel] = {
    val v2Relation = DataSourceV2Relation.create(r.table, Some(r.catalog), Some(r.identifier))
    val cache = session.sharedState.cacheManager.lookupCachedData(v2Relation)
    session.sharedState.cacheManager.uncacheQuery(session, v2Relation, cascade = true)
    if (cache.isDefined) {
      val cacheLevel = cache.get.cachedRepresentation.cacheBuilder.storageLevel
      Some(cacheLevel)
    } else {
      None
    }
  }

  private def invalidateCache(
      r: ResolvedTable,
      recacheTable: Boolean = false)(): Option[StorageLevel] = {
    val v2Relation = DataSourceV2Relation.create(r.table, Some(r.catalog), Some(r.identifier))
    val cache = session.sharedState.cacheManager.lookupCachedData(v2Relation)
    session.sharedState.cacheManager.uncacheQuery(session, v2Relation, cascade = true)
    if (cache.isDefined) {
      val cacheLevel = cache.get.cachedRepresentation.cacheBuilder.storageLevel

      if (recacheTable) {
        val cacheName = cache.get.cachedRepresentation.cacheBuilder.tableName
        // recache with the same name and cache level.
        val ds = Dataset.ofRows(session, v2Relation)
        session.sharedState.cacheManager.cacheQuery(ds, cacheName, cacheLevel)
      }
      Some(cacheLevel)
    } else {
      None
    }
  }

  private def invalidateCache(catalog: TableCatalog, table: Table, ident: Identifier): Unit = {
    val v2Relation = DataSourceV2Relation.create(table, Some(catalog), Some(ident))
    session.sharedState.cacheManager.uncacheQuery(session, v2Relation, cascade = true)
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(project, filters,
        relation @ DataSourceV2ScanRelation(_, V1ScanWrapper(scan, pushed, aggregate), output)) =>
      val v1Relation = scan.toV1TableScan[BaseRelation with TableScan](session.sqlContext)
      if (v1Relation.schema != scan.readSchema()) {
        throw QueryExecutionErrors.fallbackV1RelationReportsInconsistentSchemaError(
          scan.readSchema(), v1Relation.schema)
      }
      val rdd = v1Relation.buildScan()
      val unsafeRowRDD = DataSourceStrategy.toCatalystRDD(v1Relation, output, rdd)
      val dsScan = RowDataSourceScanExec(
        output,
        output.toStructType,
        Set.empty,
        pushed.toSet,
        aggregate,
        unsafeRowRDD,
        v1Relation,
        tableIdentifier = None)
      val condition = filters.reduceLeftOption(And)
      val selectivity = if (condition.nonEmpty) {
        NdpFilterEstimation(FilterEstimation(LFilter(condition.get, relation))).calculateFilterSelectivity(condition.get)
      } else {
        None
      }
      withProjectAndFilter(project, filters, dsScan,
        needsUnsafeConversion = false, selectivity) :: Nil

    case PhysicalOperation(project, filters,
    relation @ DataSourceV2ScanRelation(_, scan: LocalScan, output)) =>
      val localScanExec = LocalTableScanExec(output, scan.rows().toSeq)
      val condition = filters.reduceLeftOption(And)
      val selectivity = if (condition.nonEmpty) {
        NdpFilterEstimation(FilterEstimation(LFilter(condition.get, relation))).calculateFilterSelectivity(condition.get)
      } else {
        None
      }
      withProjectAndFilter(project, filters, localScanExec, needsUnsafeConversion = false, selectivity) :: Nil

    case PhysicalOperation(project, filters, relation: DataSourceV2ScanRelation) =>
      // projection and filters were already pushed down in the optimizer.
      // this uses PhysicalOperation to get the projection and ensure that if the batch scan does
      // not support columnar, a projection is added to convert the rows to UnsafeRow.
      val (runtimeFilters, postScanFilters) = filters.partition {
        case _: DynamicPruning => true
        case _ => false
      }
      val batchExec = BatchScanExec(relation.output, relation.scan, runtimeFilters)
      val condition = filters.reduceLeftOption(And)
      val selectivity = if (condition.nonEmpty) {
        NdpFilterEstimation(FilterEstimation(LFilter(condition.get, relation))).calculateFilterSelectivity(condition.get)
      } else {
        None
      }
      withProjectAndFilter(project, postScanFilters, batchExec,
        !batchExec.supportsColumnar, selectivity) :: Nil

    case PhysicalOperation(p, f, r: StreamingDataSourceV2Relation)
      if r.startOffset.isDefined && r.endOffset.isDefined =>

      val microBatchStream = r.stream.asInstanceOf[MicroBatchStream]
      val scanExec = MicroBatchScanExec(
        r.output, r.scan, microBatchStream, r.startOffset.get, r.endOffset.get)
      val condition = f.reduceLeftOption(And)
      val selectivity = if (condition.nonEmpty) {
        NdpFilterEstimation(FilterEstimation(LFilter(condition.get, r))).calculateFilterSelectivity(condition.get)
      } else {
        None
      }
      // Add a Project here to make sure we produce unsafe rows.
      withProjectAndFilter(p, f, scanExec, !scanExec.supportsColumnar, selectivity) :: Nil

    case PhysicalOperation(p, f, r: StreamingDataSourceV2Relation)
      if r.startOffset.isDefined && r.endOffset.isEmpty =>

      val continuousStream = r.stream.asInstanceOf[ContinuousStream]
      val scanExec = ContinuousScanExec(r.output, r.scan, continuousStream, r.startOffset.get)
      val condition = f.reduceLeftOption(And)
      val selectivity = if (condition.nonEmpty) {
        NdpFilterEstimation(FilterEstimation(LFilter(condition.get, r))).calculateFilterSelectivity(condition.get)
      } else {
        None
      }
      // Add a Project here to make sure we produce unsafe rows.
      withProjectAndFilter(p, f, scanExec, !scanExec.supportsColumnar, selectivity) :: Nil

    case WriteToDataSourceV2(relationOpt, writer, query, customMetrics) =>
      val invalidateCacheFunc: () => Unit = () => relationOpt match {
        case Some(r) => session.sharedState.cacheManager.uncacheQuery(session, r, cascade = true)
        case None => ()
      }
      WriteToDataSourceV2Exec(writer, invalidateCacheFunc, planLater(query), customMetrics) :: Nil

    case CreateV2Table(catalog, ident, schema, parts, props, ifNotExists) =>
      val propsWithOwner = CatalogV2Util.withDefaultOwnership(props)
      CreateTableExec(catalog, ident, schema, parts, propsWithOwner, ifNotExists) :: Nil

    case CreateTableAsSelect(catalog, ident, parts, query, props, options, ifNotExists) =>
      val propsWithOwner = CatalogV2Util.withDefaultOwnership(props)
      val writeOptions = new CaseInsensitiveStringMap(options.asJava)
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicCreateTableAsSelectExec(staging, ident, parts, query, planLater(query),
            propsWithOwner, writeOptions, ifNotExists) :: Nil
        case _ =>
          CreateTableAsSelectExec(catalog, ident, parts, query, planLater(query),
            propsWithOwner, writeOptions, ifNotExists) :: Nil
      }

    case RefreshTable(r: ResolvedTable) =>
      RefreshTableExec(r.catalog, r.identifier, invalidateCache(r, recacheTable = true)) :: Nil

    case ReplaceTable(catalog, ident, schema, parts, props, orCreate) =>
      val propsWithOwner = CatalogV2Util.withDefaultOwnership(props)
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicReplaceTableExec(
            staging, ident, schema, parts, propsWithOwner, orCreate = orCreate,
            invalidateCache) :: Nil
        case _ =>
          ReplaceTableExec(
            catalog, ident, schema, parts, propsWithOwner, orCreate = orCreate,
            invalidateCache) :: Nil
      }

    case ReplaceTableAsSelect(catalog, ident, parts, query, props, options, orCreate) =>
      val propsWithOwner = CatalogV2Util.withDefaultOwnership(props)
      val writeOptions = new CaseInsensitiveStringMap(options.asJava)
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicReplaceTableAsSelectExec(
            staging,
            ident,
            parts,
            query,
            planLater(query),
            propsWithOwner,
            writeOptions,
            orCreate = orCreate,
            invalidateCache) :: Nil
        case _ =>
          ReplaceTableAsSelectExec(
            catalog,
            ident,
            parts,
            query,
            planLater(query),
            propsWithOwner,
            writeOptions,
            orCreate = orCreate,
            invalidateCache) :: Nil
      }

    case AppendData(r @ DataSourceV2Relation(v1: SupportsWrite, _, _, _, _), query, _,
    _, Some(write)) if v1.supports(TableCapability.V1_BATCH_WRITE) =>
      write match {
        case v1Write: V1Write =>
          AppendDataExecV1(v1, query, refreshCache(r), v1Write) :: Nil
        case v2Write =>
          throw QueryCompilationErrors.batchWriteCapabilityError(
            v1, v2Write.getClass.getName, classOf[V1Write].getName)
      }

    case AppendData(r: DataSourceV2Relation, query, _, _, Some(write)) =>
      AppendDataExec(planLater(query), refreshCache(r), write) :: Nil

    case OverwriteByExpression(r @ DataSourceV2Relation(v1: SupportsWrite, _, _, _, _), _, query,
    _, _, Some(write)) if v1.supports(TableCapability.V1_BATCH_WRITE) =>
      write match {
        case v1Write: V1Write =>
          OverwriteByExpressionExecV1(v1, query, refreshCache(r), v1Write) :: Nil
        case v2Write =>
          throw QueryCompilationErrors.batchWriteCapabilityError(
            v1, v2Write.getClass.getName, classOf[V1Write].getName)
      }

    case OverwriteByExpression(r: DataSourceV2Relation, _, query, _, _, Some(write)) =>
      OverwriteByExpressionExec(planLater(query), refreshCache(r), write) :: Nil

    case OverwritePartitionsDynamic(r: DataSourceV2Relation, query, _, _, Some(write)) =>
      OverwritePartitionsDynamicExec(planLater(query), refreshCache(r), write) :: Nil

    case DeleteFromTable(relation, condition) =>
      relation match {
        case DataSourceV2ScanRelation(r, _, output) =>
          val table = r.table
          if (condition.exists(SubqueryExpression.hasSubquery)) {
            throw QueryCompilationErrors.unsupportedDeleteByConditionWithSubqueryError(condition)
          }
          // fail if any filter cannot be converted.
          // correctness depends on removing all matching data.
          val filters = DataSourceStrategy.normalizeExprs(condition.toSeq, output)
            .flatMap(splitConjunctivePredicates(_).map {
              f => DataSourceStrategy.translateFilter(f, true).getOrElse(
                throw QueryCompilationErrors.cannotTranslateExpressionToSourceFilterError(f))
            }).toArray

          if (!table.asDeletable.canDeleteWhere(filters)) {
            throw QueryCompilationErrors.cannotDeleteTableWhereFiltersError(table, filters)
          }

          DeleteFromTableExec(table.asDeletable, filters, refreshCache(r)) :: Nil
        case _ =>
          throw QueryCompilationErrors.deleteOnlySupportedWithV2TablesError()
      }

    case WriteToContinuousDataSource(writer, query, customMetrics) =>
      WriteToContinuousDataSourceExec(writer, planLater(query), customMetrics) :: Nil

    case DescribeNamespace(ResolvedNamespace(catalog, ns), extended, output) =>
      DescribeNamespaceExec(output, catalog.asNamespaceCatalog, ns, extended) :: Nil

    case DescribeRelation(r: ResolvedTable, partitionSpec, isExtended, output) =>
      if (partitionSpec.nonEmpty) {
        throw QueryCompilationErrors.describeDoesNotSupportPartitionForV2TablesError()
      }
      DescribeTableExec(output, r.table, isExtended) :: Nil

    case DescribeColumn(_: ResolvedTable, column, isExtended, output) =>
      column match {
        case c: Attribute =>
          DescribeColumnExec(output, c, isExtended) :: Nil
        case nested =>
          throw QueryCompilationErrors.commandNotSupportNestedColumnError(
            "DESC TABLE COLUMN", toPrettySQL(nested))
      }

    case DropTable(r: ResolvedTable, ifExists, purge) =>
      DropTableExec(r.catalog, r.identifier, ifExists, purge, invalidateCache(r)) :: Nil

    case _: NoopCommand =>
      LocalTableScanExec(Nil, Nil) :: Nil

    case RenameTable(r @ ResolvedTable(catalog, oldIdent, _, _), newIdent, isView) =>
      if (isView) {
        throw QueryCompilationErrors.cannotRenameTableWithAlterViewError()
      }
      RenameTableExec(
        catalog,
        oldIdent,
        newIdent.asIdentifier,
        invalidateTableCache(r),
        session.sharedState.cacheManager.cacheQuery) :: Nil

    case SetNamespaceProperties(ResolvedNamespace(catalog, ns), properties) =>
      AlterNamespaceSetPropertiesExec(catalog.asNamespaceCatalog, ns, properties) :: Nil

    case SetNamespaceLocation(ResolvedNamespace(catalog, ns), location) =>
      AlterNamespaceSetPropertiesExec(
        catalog.asNamespaceCatalog,
        ns,
        Map(SupportsNamespaces.PROP_LOCATION -> location)) :: Nil

    case CommentOnNamespace(ResolvedNamespace(catalog, ns), comment) =>
      AlterNamespaceSetPropertiesExec(
        catalog.asNamespaceCatalog,
        ns,
        Map(SupportsNamespaces.PROP_COMMENT -> comment)) :: Nil

    case CreateNamespace(catalog, namespace, ifNotExists, properties) =>
      CreateNamespaceExec(catalog, namespace, ifNotExists, properties) :: Nil

    case DropNamespace(ResolvedNamespace(catalog, ns), ifExists, cascade) =>
      DropNamespaceExec(catalog, ns, ifExists, cascade) :: Nil

    case ShowNamespaces(ResolvedNamespace(catalog, ns), pattern, output) =>
      ShowNamespacesExec(output, catalog.asNamespaceCatalog, ns, pattern) :: Nil

    case ShowTables(ResolvedNamespace(catalog, ns), pattern, output) =>
      ShowTablesExec(output, catalog.asTableCatalog, ns, pattern) :: Nil

    case SetCatalogAndNamespace(catalogManager, catalogName, ns) =>
      SetCatalogAndNamespaceExec(catalogManager, catalogName, ns) :: Nil

    case r: ShowCurrentNamespace =>
      ShowCurrentNamespaceExec(r.output, r.catalogManager) :: Nil

    case r @ ShowTableProperties(rt: ResolvedTable, propertyKey, output) =>
      ShowTablePropertiesExec(output, rt.table, propertyKey) :: Nil

    case AnalyzeTable(_: ResolvedTable, _, _) | AnalyzeColumn(_: ResolvedTable, _, _) =>
      throw QueryCompilationErrors.analyzeTableNotSupportedForV2TablesError()

    case AddPartitions(
    r @ ResolvedTable(_, _, table: SupportsPartitionManagement, _), parts, ignoreIfExists) =>
      AddPartitionExec(
        table,
        parts.asResolvedPartitionSpecs,
        ignoreIfExists,
        recacheTable(r)) :: Nil

    case DropPartitions(
    r @ ResolvedTable(_, _, table: SupportsPartitionManagement, _),
    parts,
    ignoreIfNotExists,
    purge) =>
      DropPartitionExec(
        table,
        parts.asResolvedPartitionSpecs,
        ignoreIfNotExists,
        purge,
        recacheTable(r)) :: Nil

    case RenamePartitions(
    r @ ResolvedTable(_, _, table: SupportsPartitionManagement, _), from, to) =>
      RenamePartitionExec(
        table,
        Seq(from).asResolvedPartitionSpecs.head,
        Seq(to).asResolvedPartitionSpecs.head,
        recacheTable(r)) :: Nil

    case RecoverPartitions(_: ResolvedTable) =>
      throw QueryCompilationErrors.alterTableRecoverPartitionsNotSupportedForV2TablesError()

    case SetTableSerDeProperties(_: ResolvedTable, _, _, _) =>
      throw QueryCompilationErrors.alterTableSerDePropertiesNotSupportedForV2TablesError()

    case LoadData(_: ResolvedTable, _, _, _, _) =>
      throw QueryCompilationErrors.loadDataNotSupportedForV2TablesError()

    case ShowCreateTable(rt: ResolvedTable, asSerde, output) =>
      if (asSerde) {
        throw QueryCompilationErrors.showCreateTableAsSerdeNotSupportedForV2TablesError()
      }
      ShowCreateTableExec(output, rt.table) :: Nil

    case TruncateTable(r: ResolvedTable) =>
      TruncateTableExec(
        r.table.asTruncatable,
        recacheTable(r)) :: Nil

    case TruncatePartition(r: ResolvedTable, part) =>
      TruncatePartitionExec(
        r.table.asPartitionable,
        Seq(part).asResolvedPartitionSpecs.head,
        recacheTable(r)) :: Nil

    case ShowColumns(_: ResolvedTable, _, _) =>
      throw QueryCompilationErrors.showColumnsNotSupportedForV2TablesError()

    case r @ ShowPartitions(
    ResolvedTable(catalog, _, table: SupportsPartitionManagement, _),
    pattern @ (None | Some(_: ResolvedPartitionSpec)), output) =>
      ShowPartitionsExec(
        output,
        catalog,
        table,
        pattern.map(_.asInstanceOf[ResolvedPartitionSpec])) :: Nil

    case RepairTable(_: ResolvedTable, _, _) =>
      throw QueryCompilationErrors.repairTableNotSupportedForV2TablesError()

    case r: CacheTable =>
      CacheTableExec(r.table, r.multipartIdentifier, r.isLazy, r.options) :: Nil

    case r: CacheTableAsSelect =>
      CacheTableAsSelectExec(
        r.tempViewName, r.plan, r.originalText, r.isLazy, r.options, r.referredTempFunctions) :: Nil

    case r: UncacheTable =>
      def isTempView(table: LogicalPlan): Boolean = table match {
        case SubqueryAlias(_, v: View) => v.isTempView
        case _ => false
      }
      UncacheTableExec(r.table, cascade = !isTempView(r.table)) :: Nil

    case a: AlterTableCommand if a.table.resolved =>
      val table = a.table.asInstanceOf[ResolvedTable]
      AlterTableExec(table.catalog, table.identifier, a.changes) :: Nil

    case _ => Nil
  }
}