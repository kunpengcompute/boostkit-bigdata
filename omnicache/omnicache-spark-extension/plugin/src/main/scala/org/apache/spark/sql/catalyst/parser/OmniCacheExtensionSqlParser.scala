package org.apache.spark.sql.catalyst.parser

import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.{FunctionIdentifier, SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.types.{DataType, StructType}

import java.util.Locale

class OmniCacheExtensionSqlParser(spark: SparkSession,
    delegate: ParserInterface) extends ParserInterface with SQLConfHelper with Logging {

  lazy val astBuilder = new OmniCacheExtensionAstBuilder(spark, delegate)

  override def parsePlan(sqlText: String): LogicalPlan = {
    if (isMaterializedViewCommand(sqlText)) {
      val plan = parse(sqlText) { parser =>
        astBuilder.visit(parser.singleStatement()).asInstanceOf[LogicalPlan]
      }
      plan
    } else {
      delegate.parsePlan(sqlText)
    }
  }

  override def parseExpression(sqlText: String): Expression = {
    delegate.parseExpression(sqlText)
  }

  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    delegate.parseTableIdentifier(sqlText)
  }

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    delegate.parseFunctionIdentifier(sqlText)
  }

  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    delegate.parseMultipartIdentifier(sqlText)
  }

  override def parseTableSchema(sqlText: String): StructType = {
    delegate.parseTableSchema(sqlText)
  }

  override def parseDataType(sqlText: String): DataType = {
    delegate.parseDataType(sqlText)
  }

  def isMaterializedViewCommand(sqlText: String): Boolean = {
    val normalized = sqlText.toLowerCase(Locale.ROOT).trim().replaceAll("\\s+", " ")
    normalized.contains("show materialized views") ||
        normalized.contains("create materialized view") ||
        normalized.contains("drop materialized view") ||
        normalized.contains("alter materialized view")
  }

  def parse[T](command: String)(toResult: OmniCacheSqlExtensionsParser => T): T = {
    logDebug(s"Parsing command: $command")

    val lexer = new OmniCacheSqlExtensionsLexer(
      new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new OmniCacheSqlExtensionsParser(tokenStream)
    parser.addParseListener(PostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)
    parser.legacy_setops_precedence_enbled = conf.setOpsPrecedenceEnforced
    parser.legacy_exponent_literal_as_decimal_enabled = conf.exponentLiteralAsDecimalEnabled
    parser.SQL_standard_keyword_behavior = conf.ansiEnabled

    try {
      try {
        // first, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      }
      catch {
        case e: ParseCancellationException =>
          // if we fail, parse with LL mode
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    }
    catch {
      case e: ParseException if e.command.isDefined =>
        throw e
      case e: ParseException =>
        throw e.withCommand(command)
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new ParseException(Option(command), e.message, position, position)
    }
  }
}
