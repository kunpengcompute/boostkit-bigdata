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

package org.apache.spark.sql.catalyst.parser

import com.huawei.boostkit.spark.conf.OmniCachePluginConfig
import com.huawei.boostkit.spark.conf.OmniCachePluginConfig._
import com.huawei.boostkit.spark.util.RewriteHelper
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.{ParseTree, RuleNode}
import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.{SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.parser.OmniCacheSqlExtensionsParser._
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._

class OmniCacheExtensionAstBuilder(spark: SparkSession, delegate: ParserInterface)
    extends OmniCacheSqlExtensionsBaseVisitor[AnyRef] with SQLConfHelper with Logging {

  /**
   * Parse CreateMVContext to OmniCacheCreateMvCommand
   *
   * @param ctx the parse tree
   * */
  override def visitCreateMV(ctx: CreateMVContext): LogicalPlan = withOrigin(ctx) {

    val (identifier, ifNotExists) = visitCreateMVHeader(ctx.createMVHeader())
    val disableRewrite = Option(ctx.DISABLE()).map(_.getText)
    val comment = visitCommentSpecList(ctx.commentSpec())
    val partCols = Option(ctx.identifierList()).map(visitIdentifierList).getOrElse(Seq.empty)
    var properties = Map.empty[String, String]

    val query = source(ctx.query())
    properties += (MV_QUERY_ORIGINAL_SQL -> query)
    properties += (MV_REWRITE_ENABLED -> disableRewrite.isEmpty.toString)
    properties += (MV_QUERY_ORIGINAL_SQL_CUR_DB -> spark.sessionState.catalog.getCurrentDatabase)
    properties += (MV_UPDATE_REWRITE_ENABLED -> "true")

    val (databaseName, name) = identifier match {
      case Seq(mv) => (None, mv)
      case Seq(database, mv) => (Some(database), mv)
      case _ => throw new AnalysisException(
        "The mv name is not valid: %s".format(identifier.mkString("."))
      )
    }

    try {
      val provider =
        OmniCachePluginConfig.getConf.defaultDataSource
      RewriteHelper.disableCachePlugin()
      val qe = spark.sql(query).queryExecution
      val logicalPlan = qe.optimizedPlan
      if (RewriteHelper.containsMV(qe.analyzed)) {
        throw new RuntimeException("not support create mv from mv")
      }
      OmniCacheCreateMvCommand(databaseName, name, provider, comment, properties,
        ifNotExists, partCols, logicalPlan, logicalPlan.output.map(_.name))
    } catch {
      case e: Throwable =>
        RewriteHelper.enableCachePlugin()
        throw e
    }
  }

  /**
   * Parse CreateMVHeaderContext to OmniCacheHeader
   *
   * @param ctx the parse tree
   * */
  override def visitCreateMVHeader(ctx: CreateMVHeaderContext
  ): OmniCacheHeader = withOrigin(ctx) {
    val ifNotExists = ctx.EXISTS() != null
    val multipartIdentifier = ctx.multipartIdentifier.parts.asScala.map(_.getText)
    (multipartIdentifier, ifNotExists)
  }

  override def visitRefreshMV(ctx: RefreshMVContext): LogicalPlan = withOrigin(ctx) {
    val tableIdent = visitMultipartIdentifier(ctx.multipartIdentifier())

    val (databaseName, name) = tableIdent match {
      case Seq(mv) => (None, mv)
      case Seq(database, mv) => (Some(database), mv)
      case _ => throw new AnalysisException(
        "The mv name is not valid: %s".format(tableIdent.mkString("."))
      )
    }

    val tableIdentifier = TableIdentifier(name, databaseName)

    if (!spark.sessionState.catalog.tableExists(tableIdentifier)) {
      throw new AnalysisException(
        s"Table or view not found: $tableIdentifier .")
    }

    var catalogTable = spark.sessionState.catalog.getTableMetadata(tableIdentifier)
    val queryStr = catalogTable.properties.get(MV_QUERY_ORIGINAL_SQL)
    if (queryStr.isEmpty) {
      throw new RuntimeException("cannot refresh a table with refresh mv")
    }

    // preserver preDatabase and set curDatabase
    val preDatabase = spark.catalog.currentDatabase
    val curDatabase = catalogTable.properties.getOrElse(MV_QUERY_ORIGINAL_SQL_CUR_DB, "")
    if (curDatabase.isEmpty) {
      throw new RuntimeException(
        s"mvTable: ${catalogTable.identifier.quotedString}'s curDatabase is empty!")
    }
    try {
      spark.sessionState.catalogManager.setCurrentNamespace(Array(curDatabase))
      // disable plugin
      RewriteHelper.disableCachePlugin()
      val data = spark.sql(queryStr.get).queryExecution.optimizedPlan

      val hadoopRelation = spark.table(catalogTable.identifier).logicalPlan match {
        case SubqueryAlias(_, LogicalRelation(t: HadoopFsRelation, _, _, _)) => t
        case LogicalRelation(t: HadoopFsRelation, _, _, _) => t
        case _ => throw new AnalysisException(s"$tableIdentifier should be converted to " +
            "HadoopFsRelation.")
      }

      val allPaths = hadoopRelation.location.rootPaths
      val outputPath = if (allPaths.length == 1) {
        val path = allPaths.head
        val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
        path.makeQualified(fs.getUri, fs.getWorkingDirectory)
      } else {
        throw new IllegalArgumentException("Expected exactly one path to be specified, but " +
            s"got: ${allPaths.mkString(", ")}")
      }

      val caseSensitive = spark.sessionState.conf.caseSensitiveAnalysis
      val partitionColumns = catalogTable.partitionColumnNames
      PartitioningUtils.validatePartitionColumn(data.schema, partitionColumns, caseSensitive)

      val fileIndex = Some(catalogTable.identifier).map { tableIdent =>
        spark.table(tableIdent).queryExecution.analyzed.collect {
          case LogicalRelation(t: HadoopFsRelation, _, _, _) => t.location
        }.head
      }
      // For partitioned relation r, r.schema's column ordering can be different from the column
      // ordering of data.logicalPlan (partition columns are all moved after data column).  This
      // will be adjusted within InsertIntoHadoopFsRelation.
      RefreshMaterializedViewCommand(
        outputPath = outputPath,
        staticPartitions = Map.empty,
        ifPartitionNotExists = false,
        partitionColumns = partitionColumns.map(UnresolvedAttribute.quoted),
        bucketSpec = hadoopRelation.bucketSpec,
        fileFormat = hadoopRelation.fileFormat,
        options = hadoopRelation.options,
        query = data,
        mode = SaveMode.Overwrite,
        catalogTable = Some(catalogTable),
        fileIndex = fileIndex,
        outputColumnNames = data.output.map(_.name))
    } catch {
      case e: Throwable =>
        RewriteHelper.enableCachePlugin()
        throw e
    } finally {
      spark.sessionState.catalogManager.setCurrentNamespace(Array(preDatabase))
    }
  }

  /**
   * Parse ShowMVsContext to ShowMaterializedViewCommand
   *
   * @param ctx the parse tree
   * */
  override def visitShowMVs(ctx: ShowMVsContext): LogicalPlan = withOrigin(ctx) {
    val multiPart = Option(ctx.multipartIdentifier).map(visitMultipartIdentifier)
    if (multiPart.isDefined) {
      val identifier = multiPart.get
      identifier match {
        case Seq(mv) => ShowMaterializedViewCommand(None, Some(mv))
        case Seq(database, mv) => ShowMaterializedViewCommand(Some(database), Some(mv))
        case _ => throw new AnalysisException(
          "The mv name is not valid: %s".format(identifier.mkString(".")))
      }
    } else {
      ShowMaterializedViewCommand(None, None)
    }
  }

  /**
   * Parse DropMVContext to DropMaterializedViewCommand
   *
   * @param ctx the parse tree
   * */
  override def visitDropMV(ctx: DropMVContext): LogicalPlan = withOrigin(ctx) {
    val multiPart = Option(ctx.multipartIdentifier).map(visitMultipartIdentifier)
    val ifExists = Option(ctx.EXISTS())
    if (multiPart.isDefined) {
      val identifier = multiPart.get
      identifier match {
        case Seq(mv) => DropMaterializedViewCommand(
          TableIdentifier(mv, Some(spark.sessionState.catalog.getCurrentDatabase)),
          ifExists.isDefined,
          purge = true)
        case Seq(database, mv) => DropMaterializedViewCommand(
          TableIdentifier(mv, Some(database)),
          ifExists.isDefined,
          purge = true
        )
        case _ => throw new AnalysisException(
          "The mv name is not valid: %s".format(identifier.mkString(".")))
      }
    } else {
      throw new AnalysisException("mv name cannot be empty")
    }
  }

  /**
   * Create an [[ExplainCommand]] logical plan.
   * The syntax of using this command in SQL is:
   * {{{
   *   EXPLAIN (EXTENDED | CODEGEN | COST | FORMATTED) SELECT * FROM ...
   * }}}
   */
  override def visitExplain(ctx: ExplainContext): LogicalPlan = withOrigin(ctx) {
    if (ctx.LOGICAL != null) {
      operationNotAllowed("EXPLAIN LOGICAL", ctx)
    }

    val statement = plan(ctx.statement)
    if (statement == null) {
      null // This is enough since ParseException will raise later.
    } else {
      ExplainCommand(
        logicalPlan = statement,
        mode = {
          if (ctx.EXTENDED != null) ExtendedMode
          else if (ctx.CODEGEN != null) CodegenMode
          else if (ctx.COST != null) CostMode
          else if (ctx.FORMATTED != null) FormattedMode
          else SimpleMode
        })
    }
  }

  /**
   * Parse AlterRewriteMVContext to AlterRewriteMaterializedViewCommand
   *
   * @param ctx the parse tree
   * */
  override def visitAlterRewriteMV(ctx: AlterRewriteMVContext): LogicalPlan = withOrigin(ctx) {
    val identifier = visitMultipartIdentifier(ctx.multipartIdentifier)
    val enableRewrite = Option(ctx.ENABLE()).isDefined

    identifier match {
      case Seq(mv) => AlterRewriteMaterializedViewCommand(
        TableIdentifier(mv, Some(spark.sessionState.catalog.getCurrentDatabase)),
        enableRewrite)
      case Seq(database, mv) => AlterRewriteMaterializedViewCommand(
        TableIdentifier(mv, Some(database)),
        enableRewrite)
      case _ => throw new AnalysisException(
        "The mv name is not valid: %s".format(identifier.mkString(".")))
    }
  }

  /**
   * Override the default behavior for all visit methods. This will only return a non-null result
   * when the context has only one child. This is done because there is no generic method to
   * combine the results of the context children. In all other cases null is returned.
   */
  override def visitChildren(node: RuleNode): AnyRef = {
    if (node.getChildCount == 1) {
      node.getChild(0).accept(this)
    } else {
      null
    }
  }

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  type OmniCacheHeader = (Seq[String], Boolean)

  /**
   * Create a comment string.
   */
  override def visitCommentSpec(ctx: CommentSpecContext): String = withOrigin(ctx) {
    string(ctx.STRING)
  }

  protected def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }

  /* ********************************************************************************************
   * Plan parsing
   * ******************************************************************************************** */
  protected def plan(tree: ParserRuleContext): LogicalPlan = typedVisit(tree)

  /**
   * Create an optional comment string.
   */
  protected def visitCommentSpecList(ctx: CommentSpecContext): Option[String] = {
    Option(ctx).map(visitCommentSpec)
  }

  /**
   * Create a multi-part identifier.
   */
  override def visitMultipartIdentifier(ctx: MultipartIdentifierContext): Seq[String] =
    withOrigin(ctx) {
      ctx.parts.asScala.map(_.getText)
    }

  /**
   * Create a Sequence of Strings for a parenthesis enclosed alias list.
   */
  override def visitIdentifierList(ctx: IdentifierListContext): Seq[String] = withOrigin(ctx) {
    visitIdentifierSeq(ctx.identifierSeq)
  }

  /**
   * Create a Sequence of Strings for an identifier list.
   */
  override def visitIdentifierSeq(ctx: IdentifierSeqContext): Seq[String] = withOrigin(ctx) {
    ctx.ident.asScala.map(_.getText)
  }
}
