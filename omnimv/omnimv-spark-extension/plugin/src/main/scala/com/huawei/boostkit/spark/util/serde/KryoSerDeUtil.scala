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

import com.huawei.boostkit.spark.util.serde.LogicalPlanWrapperUtil._
import com.esotericsoftware.kryo.io.Input
import java.io.ByteArrayOutputStream
import java.util.Base64

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.optimizer.rules.RewriteTime
import org.apache.spark.sql.catalyst.plans.logical._

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
      if (byteArray == null || byteArray.size == 0) {
        input.setBuffer(new Array[Byte](4096))
      } else {
        input.setBuffer(byteArray)
      }

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
}
