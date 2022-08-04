package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.{FunctionIdentifier, SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

class OmniCacheExtensionSqlParser(spark: SparkSession,
    delegate: ParserInterface) extends ParserInterface with SQLConfHelper {


  override def parsePlan(sqlText: String): LogicalPlan = {
    null
  }

  override def parseExpression(sqlText: String): Expression = ???

  override def parseTableIdentifier(sqlText: String): TableIdentifier = ???

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = ???

  override def parseMultipartIdentifier(sqlText: String): Seq[String] = ???

  override def parseTableSchema(sqlText: String): StructType = ???

  override def parseDataType(sqlText: String): DataType = ???
}
