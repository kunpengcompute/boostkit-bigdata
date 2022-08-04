package org.apache.spark.sql.catalyst.parser

import com.huawei.boostkit.spark.conf.OmniCachePluginConfig
import com.huawei.boostkit.spark.conf.OmniCachePluginConfig._
import com.huawei.boostkit.spark.util.RewriteHelper
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.{ParseTree, RuleNode}

import scala.collection.JavaConverters._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.parser.OmniCacheSqlExtensionsParser._
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.ExplainCommand

import java.util.Locale

class OmniCacheExtensionAstBuilder(spark: SparkSession, delegate: ParserInterface)
    extends OmniCacheSqlExtensionsBaseVisitor[AnyRef] with SQLConfHelper with Logging {

  override def visitCreateMV(ctx: CreateMVContext): LogicalPlan = withOrigin(ctx) {

    val (identifier, ifNotExists) = visitCreateMVHeader(ctx.createMVHeader())
    val disableRewrite = Option(ctx.DISABLE()).map(_.getText)
    val datasource = Option(ctx.tableProvider()).map(_.multipartIdentifier.getText)
    val comment = visitCommentSpecList(ctx.commentSpec())
    val partCols = Option(ctx.identifierList()).map(visitIdentifierList).getOrElse(Seq.empty)
    var properties = Map.empty[String, String]

    val query = source(ctx.query())
    properties += (MV_QUERY_ORIGINAL_SQL -> query)
    properties += (MV_REWRITE_ENABLED -> disableRewrite.isEmpty.toString)
    properties += (MV_QUERY_ORIGINAL_SQL_CUR_DB -> spark.sessionState.catalog.getCurrentDatabase)

    val (databaseName, name) = identifier match {
      case Seq(mv) => (None, mv)
      case Seq(database, mv) => (Some(database), mv)
      case _ => throw new AnalysisException(
        "The mv name is not valid: %s".format(identifier.mkString("."))
      )
    }

    val provider = if (datasource.isEmpty) {
      OmniCachePluginConfig.getConf.defaultDataSource
    } else {
      val lowerDs = datasource.get.toLowerCase(Locale.ROOT)
      if (OmniCachePluginConfig.getConf.dataSourceSet.contains(lowerDs)) {
        lowerDs
      } else {
        throw new RuntimeException(
          s"using unknow datasource: ${datasource.get}! only support orc and parquet"
        )
      }
    }

    val qe = spark.sql(query).queryExecution
    val logicalPlan = qe.optimizedPlan
    if (RewriteHelper.containsMV(qe.analyzed)) {
      throw new RuntimeException("not support create mv from mv")
    }
    // TODO
    null
  }

  override def visitCreateMVHeader(ctx: CreateMVHeaderContext
  ): OmniCacheHeader = withOrigin(ctx) {
    val ifNotExists = ctx.EXISTS() != null
    val multipartIdentifier = ctx.multipartIdentifier.parts.asScala.map(_.getText)
    (multipartIdentifier, ifNotExists)
  }

  override def visitShowMVs(ctx: ShowMVsContext): LogicalPlan = withOrigin(ctx) {
    val multiPart = Option(ctx.multipartIdentifier).map(visitMultipartIdentifier)
    if (multiPart.isDefined) {
      val identifier = multiPart.get
      identifier match {
        // TODO
        case _ =>
      }
    } else {
      // TODO
    }
    null
  }

  override def visitDropMV(ctx: DropMVContext): LogicalPlan = withOrigin(ctx) {
    val multiPart = Option(ctx.multipartIdentifier).map(visitMultipartIdentifier)
    if (multiPart.isDefined) {
      val identifier = multiPart.get
      identifier match {
        // TODO
        case _ =>
      }
    } else {
      // TODO
    }
    null
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

  override def visitAlterRewriteMV(ctx: AlterRewriteMVContext): LogicalPlan = withOrigin(ctx) {
    val identifier = visitMultipartIdentifier(ctx.multipartIdentifier)


    // TODO

    null

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
