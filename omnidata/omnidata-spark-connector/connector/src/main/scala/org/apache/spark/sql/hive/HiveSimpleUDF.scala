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

package org.apache.spark.sql.hive

import org.apache.hadoop.hive.ql.exec.{FunctionRegistry, UDF}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ConversionHelper
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, UserDefinedExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.types.DataType
import org.apache.hadoop.hive.ql.udf.{UDFType => HiveUDFType}

import scala.collection.JavaConverters.seqAsJavaListConverter

case class HiveSimpleUDF(
                                        name: String, funcWrapper: HiveFunctionWrapper, children: Seq[Expression])
  extends Expression
    with HiveInspectors
    with CodegenFallback
    with Logging
    with UserDefinedExpression {

  override lazy val deterministic: Boolean = isUDFDeterministic && children.forall(_.deterministic)

  override def nullable: Boolean = true

  @transient
  lazy val function = funcWrapper.createFunction[UDF]()

  @transient
  private lazy val method =
    function.getResolver.getEvalMethod(children.map(_.dataType.toTypeInfo).asJava)

  @transient
  private lazy val arguments = children.map(toInspector).toArray

  @transient
  private lazy val isUDFDeterministic = {
    val udfType = function.getClass.getAnnotation(classOf[HiveUDFType])
    udfType != null && udfType.deterministic() && !udfType.stateful()
  }

  override def foldable: Boolean = isUDFDeterministic && children.forall(_.foldable)

  // Create parameter converters
  @transient
  private lazy val conversionHelper = new ConversionHelper(method, arguments)

  override lazy val dataType = javaTypeToDataType(method.getGenericReturnType)

  @transient
  private lazy val wrappers = children.map(x => wrapperFor(toInspector(x), x.dataType)).toArray

  @transient
  lazy val unwrapper = unwrapperFor(ObjectInspectorFactory.getReflectionObjectInspector(
    method.getGenericReturnType, ObjectInspectorOptions.JAVA))

  @transient
  private lazy val cached: Array[AnyRef] = new Array[AnyRef](children.length)

  @transient
  private lazy val inputDataTypes: Array[DataType] = children.map(_.dataType).toArray

  // TODO: Finish input output types.
  override def eval(input: InternalRow): Any = {
    val inputs = wrap(children.map(_.eval(input)), wrappers, cached, inputDataTypes)
    val ret = FunctionRegistry.invoke(
      method,
      function,
      conversionHelper.convertIfNecessary(inputs : _*): _*)
    unwrapper(ret)
  }

  override def toString: String = {
    s"$nodeName#${funcWrapper.functionClassName}(${children.mkString(",")})"
  }

  override def prettyName: String = name

  override def sql: String = s"$name(${children.map(_.sql).mkString(", ")})"
}
