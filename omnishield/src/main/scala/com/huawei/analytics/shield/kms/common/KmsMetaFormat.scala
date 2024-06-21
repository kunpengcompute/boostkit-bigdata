/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.analytics.shield.kms.common

import com.fasterxml.jackson.databind.{DeserializationFeature, MapperFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

case class KmsMetaFormat(encryptedDataKey: String)

/**
 * Convert kmsMetaFormat to JSON or JSON to kmsMetaFormat
 */
class KmsMetaFormatSerializer {
  val jacksonJsonSerializer = new JacksonJsonSerializer()

  def deserialize(jsonStr: String): KmsMetaFormat = {
    jacksonJsonSerializer.deSerialize(classOf[KmsMetaFormat], jsonStr)
  }

  def serialize(kmsMetaFormat: KmsMetaFormat): String = {
    jacksonJsonSerializer.serialize(kmsMetaFormat)
  }
}

/**
 * for json Serializer
 */
class JacksonJsonSerializer {
  val objMapper = new ObjectMapper()
  objMapper.registerModule(DefaultScalaModule)
  objMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  objMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
  objMapper.configure(SerializationFeature.INDENT_OUTPUT, true)
  objMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)

  def deSerialize[T](clazz: Class[T], dest: String): T = {
    objMapper.readValue[T](dest, clazz)
  }

  def serialize(src: Object): String = {
    objMapper.writeValueAsString(src)
  }
}

/**
 * for KmsMeta Serializer
 */
object KmsMetaFormatSerializer {
  val serializer = new KmsMetaFormatSerializer()

  def apply(jsonStr: String): KmsMetaFormat = {
    serializer.deserialize(jsonStr)
  }

  def apply(kmsMetaFormat: KmsMetaFormat): String = {
    serializer.serialize(kmsMetaFormat)
  }
}
