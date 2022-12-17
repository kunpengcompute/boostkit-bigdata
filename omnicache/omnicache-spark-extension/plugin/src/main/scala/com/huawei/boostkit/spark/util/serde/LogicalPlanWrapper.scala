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

package com.huawei.boostkit.spark.util.serde

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LeafNode, LogicalPlan}
import org.apache.spark.sql.execution.FileRelation
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{DataType, StructType}

trait LogicalPlanWrapper

/**
 * parent class no default constructor
 */
trait NoDefaultConstructor extends LogicalPlanWrapper

/**
 * class contains variable like SparkSession,Configuration
 */
trait InMemoryStates extends LogicalPlanWrapper

abstract class SubqueryExpressionWrapper
    extends Unevaluable with NoDefaultConstructor {
  override def nullable: Boolean = throw new UnsupportedOperationException()

  override def dataType: DataType = throw new UnsupportedOperationException()
}

case class ScalarSubqueryWrapper(plan: LogicalPlan, children: Seq[Expression], exprId: ExprId)
    extends SubqueryExpressionWrapper {
  override def dataType: DataType = plan.schema.fields.head.dataType

  override def toString: String = s"scalar-subquery-wrapper#${exprId.id}"
}

case class ListQueryWrapper(plan: LogicalPlan, children: Seq[Expression], exprId: ExprId,
    childOutputs: Seq[Attribute])
    extends SubqueryExpressionWrapper {
  override def toString(): String = s"list-wrapper#${exprId.id}"
}

case class InSubqueryWrapper(values: Seq[Expression], query: ListQueryWrapper)
    extends Predicate with Unevaluable {
  override def children: Seq[Expression] = values :+ query

  override def nullable: Boolean = throw new UnsupportedOperationException()
}

case class ExistsWrapper(plan: LogicalPlan, children: Seq[Expression], exprId: ExprId)
    extends SubqueryExpressionWrapper {
  override def toString(): String = s"exists-wrapper#${exprId.id}"
}

case class ScalaUDFWrapper(
    function: AnyRef,
    dataType: DataType,
    children: Seq[Expression],
    inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Nil,
    outputEncoder: Option[ExpressionEncoder[_]] = None,
    udfName: Option[String] = None,
    nullable: Boolean = true,
    udfDeterministic: Boolean = true)
    extends Expression with Unevaluable with NoDefaultConstructor

case class IntersectWrapper(
    left: LogicalPlan,
    right: LogicalPlan,
    isAll: Boolean)
    extends BinaryNode with NoDefaultConstructor {

  override def output: Seq[Attribute] =
    left.output.zip(right.output).map { case (leftAttr, rightAttr) =>
      leftAttr.withNullability(leftAttr.nullable && rightAttr.nullable)
    }
}

case class ExceptWrapper(
    left: LogicalPlan,
    right: LogicalPlan,
    isAll: Boolean)
    extends BinaryNode with NoDefaultConstructor {

  override def output: Seq[Attribute] = left.output
}

case class InMemoryFileIndexWrapper(rootPathsSpecified: Seq[String])
    extends FileIndex with InMemoryStates {
  override def rootPaths: Seq[Path] = throw new UnsupportedOperationException()

  override def listFiles(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] =
    throw new UnsupportedOperationException()

  override def inputFiles: Array[String] = throw new UnsupportedOperationException()

  override def refresh(): Unit = throw new UnsupportedOperationException()

  override def sizeInBytes: Long = throw new UnsupportedOperationException()

  override def partitionSchema: StructType = throw new UnsupportedOperationException()
}

case class CatalogFileIndexWrapper(table: CatalogTable,
    override val sizeInBytes: Long)
    extends FileIndex with InMemoryStates {
  override def rootPaths: Seq[Path] = throw new UnsupportedOperationException()

  override def listFiles(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] =
    throw new UnsupportedOperationException()

  override def inputFiles: Array[String] = throw new UnsupportedOperationException()

  override def refresh(): Unit = throw new UnsupportedOperationException()

  override def partitionSchema: StructType = throw new UnsupportedOperationException()
}

case class HadoopFsRelationWrapper(
    location: FileIndex,
    partitionSchema: StructType,
    dataSchema: StructType,
    bucketSpec: Option[BucketSpec],
    fileFormat: FileFormat,
    options: Map[String, String])
    extends BaseRelation with FileRelation with InMemoryStates {
  override def sqlContext: SQLContext = throw new UnsupportedOperationException()

  override def schema: StructType = throw new UnsupportedOperationException()

  override def inputFiles: Array[String] = throw new UnsupportedOperationException()
}

case class LogicalRelationWrapper(
    relation: BaseRelation,
    output: Seq[AttributeReference],
    catalogTable: Option[CatalogTable],
    override val isStreaming: Boolean)
    extends LeafNode with InMemoryStates

abstract class FileFormatWrapper extends FileFormat {
  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String], files: Seq[FileStatus]): Option[StructType] =
    throw new UnsupportedOperationException()

  override def prepareWrite(sparkSession: SparkSession,
      job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory =
    throw new UnsupportedOperationException()
}

case object CSVFileFormatWrapper extends FileFormatWrapper

case object JsonFileFormatWrapper extends FileFormatWrapper
