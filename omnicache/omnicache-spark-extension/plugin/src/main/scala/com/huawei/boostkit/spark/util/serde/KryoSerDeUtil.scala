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

import com.esotericsoftware.kryo.io.Input
import java.io.ByteArrayOutputStream
import java.util.Base64
import org.apache.hadoop.fs.Path

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.rules.RewriteTime
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat

object KryoSerDeUtil {

  /**
   * serialize object to byte array
   *
   * @param kryoSerializer kryoSerializer
   * @param obj            obj
   * @tparam T obj type
   * @return serialized byte array
   */
  def serialize[T](kryoSerializer: KryoSerializer, obj: T): Array[Byte] = {
    val kryo = kryoSerializer.newKryo()
    val baos = new ByteArrayOutputStream()
    val output = kryoSerializer.newKryoOutput()
    output.setOutputStream(baos)

    kryo.writeClassAndObject(output, obj)
    output.close()
    baos.toByteArray
  }

  /**
   * serialize object to string
   *
   * @param kryoSerializer kryoSerializer
   * @param obj            obj
   * @tparam T obj type
   * @return serialized string
   */
  def serializeToStr[T](kryoSerializer: KryoSerializer, obj: T): String = {
    val byteArray = serialize[T](kryoSerializer, obj)
    Base64.getEncoder.encodeToString(byteArray)
  }

  /**
   * deserialize byte array to object
   *
   * @param kryoSerializer kryoSerializer
   * @param byteArray      byteArray
   * @tparam T obj type
   * @return deserialized object
   */
  def deserialize[T](kryoSerializer: KryoSerializer, byteArray: Array[Byte]): T = {
    RewriteTime.withTimeStat("deserialize.C") {
      val kryo = RewriteTime.withTimeStat("deserialize.newKryo.C") {
        kryoSerializer.newKryo()
      }
      val input = new Input()
      input.setBuffer(byteArray)

      val obj = RewriteTime.withTimeStat("deserialize.readClassAndObject.C") {
        kryo.readClassAndObject(input)
      }
      obj.asInstanceOf[T]
    }
  }

  /**
   * deserialize string to object
   *
   * @param kryoSerializer kryoSerializer
   * @param str            str
   * @tparam T obj type
   * @return deserialized object
   */
  def deserializeFromStr[T](kryoSerializer: KryoSerializer, str: String): T = {
    val byteArray = RewriteTime.withTimeStat("Base64.getDecoder.decode.C") {
      Base64.getDecoder.decode(str)
    }
    deserialize[T](kryoSerializer, byteArray)
  }

  /**
   * serialize logicalPlan to string
   *
   * @param kryoSerializer kryoSerializer
   * @param plan           plan
   * @return serialized string
   */
  def serializePlan(kryoSerializer: KryoSerializer, plan: LogicalPlan): String = {
    val wrappedPlan = wrap(plan)
    serializeToStr[LogicalPlan](kryoSerializer, wrappedPlan)
  }

  /**
   * deserialize string to logicalPlan
   *
   * @param kryoSerializer kryoSerializer
   * @param spark          spark
   * @param serializedPlan serializedPlan
   * @return logicalPlan
   */
  def deserializePlan(
      kryoSerializer: KryoSerializer, spark: SparkSession, serializedPlan: String): LogicalPlan = {
    val wrappedPlan = deserializeFromStr[LogicalPlan](kryoSerializer, serializedPlan)
    unwrap(spark, wrappedPlan)
  }

  /**
   * wrap logicalPlan if cannot serialize
   *
   * @param plan plan
   * @return wrapped plan
   */
  def wrap(plan: LogicalPlan): LogicalPlan = {
    // subqeury contains plan
    val newPlan = plan.transformAllExpressions {
      case e: ScalarSubquery =>
        ScalarSubqueryWrapper(wrap(e.plan), e.children, e.exprId)
      case e: ListQuery =>
        ListQueryWrapper(wrap(e.plan), e.children, e.exprId, e.childOutputs)
      case e: InSubquery =>
        InSubqueryWrapper(
          e.values,
          ListQueryWrapper(
            wrap(e.query.plan),
            e.query.children,
            e.query.exprId,
            e.query.childOutputs))
      case e: Exists =>
        ExistsWrapper(wrap(e.plan), e.children, e.exprId)
      case e: ScalaUDF =>
        ScalaUDFWrapper(
          e.function,
          e.dataType,
          e.children,
          e.inputEncoders,
          e.outputEncoder,
          e.udfName,
          e.nullable,
          e.udfDeterministic)
    }
    newPlan.transform {
      case p: With =>
        With(wrap(p.child), p.cteRelations.map {
          case (r, s) => (r, SubqueryAlias(s.alias, wrap(s.child)))
        })
      case p: Intersect =>
        IntersectWrapper(wrap(p.left), wrap(p.right), p.isAll)
      case p: Except =>
        ExceptWrapper(wrap(p.left), wrap(p.right), p.isAll)
      case LogicalRelation(
      HadoopFsRelation(
      location: FileIndex,
      partitionSchema,
      dataSchema,
      bucketSpec,
      fileFormat,
      options),
      output,
      catalogTable,
      isStreaming) =>
        LogicalRelationWrapper(
          HadoopFsRelationWrapper(
            wrapFileIndex(location),
            partitionSchema,
            dataSchema,
            bucketSpec,
            wrapFileFormat(fileFormat),
            options),
          output,
          catalogTable,
          isStreaming)
    }
  }

  /**
   * unwrap logicalPlan to original logicalPlan
   *
   * @param spark spark
   * @param plan  plan
   * @return original logicalPlan
   */
  def unwrap(spark: SparkSession, plan: LogicalPlan): LogicalPlan = {
    RewriteTime.withTimeStat("unwrap.C") {
      val newPlan = plan.transform {
        case p: With =>
          With(unwrap(spark, p.child), p.cteRelations.map {
            case (r, s) => (r, SubqueryAlias(s.alias, unwrap(spark, s.child)))
          })
        case p: IntersectWrapper =>
          Intersect(unwrap(spark, p.left), unwrap(spark, p.right), p.isAll)
        case p: ExceptWrapper =>
          Except(unwrap(spark, p.right), unwrap(spark, p.right), p.isAll)
        case LogicalRelationWrapper(
        HadoopFsRelationWrapper(
        location: FileIndex,
        partitionSchema,
        dataSchema,
        bucketSpec,
        fileFormat,
        options),
        output,
        catalogTable,
        isStreaming) =>
          LogicalRelation(
            HadoopFsRelation(
              unwrapFileIndex(spark, location),
              partitionSchema,
              dataSchema,
              bucketSpec,
              unwrapFileFormat(fileFormat),
              options)(spark),
            output,
            catalogTable,
            isStreaming)
        case h: HiveTableRelation =>
          h.copy(prunedPartitions = None)
      }

      newPlan.transformAllExpressions {
        case e: ScalarSubqueryWrapper =>
          ScalarSubquery(unwrap(spark, e.plan), e.children, e.exprId)
        case e: ListQueryWrapper =>
          ListQueryWrapper(unwrap(spark, e.plan), e.children, e.exprId, e.childOutputs)
        case e: InSubqueryWrapper =>
          InSubquery(
            e.values,
            ListQuery(
              unwrap(spark, e.query.plan),
              e.query.children,
              e.query.exprId,
              e.query.childOutputs))
        case e: ExistsWrapper =>
          Exists(unwrap(spark, e.plan), e.children, e.exprId)
        case e: ScalaUDFWrapper =>
          ScalaUDF(
            e.function,
            e.dataType,
            e.children,
            e.inputEncoders,
            e.outputEncoder,
            e.udfName,
            e.nullable,
            e.udfDeterministic
          )
      }
    }
  }

  def wrapFileIndex(fileIndex: FileIndex): FileIndex = {
    fileIndex match {
      case location: InMemoryFileIndex =>
        InMemoryFileIndexWrapper(location.rootPaths.map(path => path.toString))
      case location: CatalogFileIndex =>
        CatalogFileIndexWrapper(location.table, location.sizeInBytes)
      case other =>
        other
    }
  }

  def unwrapFileIndex(spark: SparkSession, fileIndex: FileIndex): FileIndex = {
    fileIndex match {
      case location: InMemoryFileIndexWrapper =>
        new InMemoryFileIndex(
          spark,
          location.rootPathsSpecified.map(path => new Path(path)),
          Map(),
          None)
      case location: CatalogFileIndexWrapper =>
        new CatalogFileIndex(
          spark,
          location.table,
          location.sizeInBytes)
      case other =>
        other
    }
  }

  def wrapFileFormat(fileFormat: FileFormat): FileFormat = {
    fileFormat match {
      case _: CSVFileFormat => CSVFileFormatWrapper
      case _: JsonFileFormat => JsonFileFormatWrapper
      case other => other
    }
  }

  def unwrapFileFormat(fileFormat: FileFormat): FileFormat = {
    fileFormat match {
      case CSVFileFormatWrapper => new CSVFileFormat
      case JsonFileFormatWrapper => new JsonFileFormat
      case other => other
    }
  }
}
