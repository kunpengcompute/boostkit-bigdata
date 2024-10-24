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

package com.huawei.analytics.shield.utils

object ShieldConfParam {
  val WRITE_DATA_KEY_CIPHER_TEXT = "shield.write.dataKey.cipherText"
  val PRIMARY_KEY_NAME = "spark.shield.primaryKey.name"
  val DATA_KEY_LENGTH = "spark.shield.dataKey.length"
  val SPARK_IO_CODECS ="spark.hadoop.io.compression.codecs"
  val SPARK_REMOTE_ATTESTATION_ENABLE ="spark.remote.attestation.enable"
  val SPARK_REMOTE_ATTESTATION_AGENT_CLASS ="spark.remote.attestation.agent.class"
  val SPARK_REMOTE_ATTESTATION_SECRET ="spark.remote.attestation.secret"

  def genKmsTypeConf(primaryKeyName: String): String = {
    s"spark.shield.primaryKey.$primaryKeyName.kms.type"
  }

  def genDataKeyLengthConf(dataKeyCipherTextStr: String): String = {
    s"spark.shield.${dataKeyCipherTextStr}.dataKey.length"
  }

  def genShieldCryptoModeConf(dataKeyCipherTextStr: String): String = {
    s"shield.${dataKeyCipherTextStr}.cryptoMode"
  }

  def genShieldReadPrimaryKeyConf(dataKeyCipherTextStr: String): String = {
    s"shield.read.dataKey.$dataKeyCipherTextStr.keyName"
  }
}

object ValueConf {
  val DEFAULT_DATA_KEY_LENGTH = 256
  val DATA_KEY_LENGTH_128 = 128
}