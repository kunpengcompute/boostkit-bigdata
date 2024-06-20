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

package com.huawei.analytics.shield.crypto.dataframe

import com.huawei.analytics.shield.crypto.{AES_GCM_NOPADDING, AlgorithmMode, PLAIN_TEXT}
import com.huawei.analytics.shield.kms.common.KeyOperator
import com.huawei.analytics.shield.utils.LogError.invalidArgumentError

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

/**
 * EncryptedDataFrameReader
 *
 * @param sparkSession   sparkSession
 * @param encryptMode    encryptMode
 * @param primaryKeyName primaryKeyName
 * @param keyOperator    keyOperator
 */
class ShieldDataFrameReader(
                             sparkSession: SparkSession,
                             encryptMode: AlgorithmMode,
                             primaryKeyName: String,
                             keyOperator: KeyOperator) {
  protected val extraOptions = new scala.collection.mutable.HashMap[String, String]
  protected var dataFrameReader: DataFrameReader = sparkSession.read

  def addExtraOption(key: String, value: String): this.type = {
    this.extraOptions += (key -> value)
    this
  }

  def addHeaderParam(value: String): this.type = {
    this.extraOptions += ("header" -> value)
    this
  }

  def schema(schema: StructType): this.type = {
    dataFrameReader = dataFrameReader.schema(schema)
    this
  }

  def setContext(path: String): Unit = {
    encryptMode match {
      case PLAIN_TEXT =>
      case AES_GCM_NOPADDING =>
        val encryptedDataKeyStr = keyOperator.readEncryptedDataKeyFromMeta(path)
        val dataKeyPlainKeyStr = keyOperator.encoderWithBas64(keyOperator.getDataKeyPlainText(encryptedDataKeyStr))
        sparkSession.sparkContext.hadoopConfiguration
          .set(s"shield.read.dataKey.$encryptedDataKeyStr.plainText", dataKeyPlainKeyStr)
        sparkSession.sparkContext.hadoopConfiguration
          .set("shield.cryptoMode", encryptMode.encryptionAlgorithm)
      case _ =>
        invalidArgumentError("unknown EncryptMode " + AlgorithmMode.toString)
    }
  }

  def csv(path: String): DataFrame = {
    setContext(path)
    dataFrameReader.options(extraOptions).csv(path)
  }

}
