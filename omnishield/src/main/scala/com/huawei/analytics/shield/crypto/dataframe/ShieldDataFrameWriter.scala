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

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Locale

/**
 * EncryptedDataFrameWriter
 *
 * @param sparkSession   sparkSession
 * @param df             DataFrame
 * @param algorithmMode  algorithmMode
 * @param primaryKeyName primaryKeyName
 * @param KeyOperator    keyOperator
 */
class ShieldDataFrameWriter(
                             sparkSession: SparkSession,
                             df: DataFrame,
                             algorithmMode: AlgorithmMode,
                             primaryKeyName: String,
                             keyOperator: KeyOperator) {
  protected val extraOptions = new scala.collection.mutable.HashMap[String, String]

  def option(key: String, value: String): this.type = {
    this.extraOptions += (key -> value)
    this
  }

  def addHeaderParam(value: String): this.type = {
    this.extraOptions += ("header" -> value)
    this
  }

  private var mode: SaveMode = SaveMode.ErrorIfExists

  def mode(saveMode: String): this.type = {
    this.mode = saveMode.toLowerCase(Locale.ROOT) match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "ignore" => SaveMode.Ignore
      case "error" | "errorifexists" | "default" => SaveMode.ErrorIfExists
      case _ =>
        invalidArgumentError(s"Unknown save mode: $saveMode.")
        null
    }
    this
  }

  def setContext(): Unit = {
    algorithmMode match {
      case PLAIN_TEXT =>
      case AES_GCM_NOPADDING =>
        option("compression", "com.huawei.analytics.shield.crypto.CryptoCodec")
        val (dataKeyPlainBytes, dataKeyCipherBytes) = keyOperator.getDataKeyPair
        val dataKeyPlainTextStr = keyOperator.encoderWithBas64(dataKeyPlainBytes)
        sparkSession.sparkContext.hadoopConfiguration
          .set("shield.write.dataKey.plainText", dataKeyPlainTextStr)
        val dataKeyCipherTextStr = keyOperator.encoderWithBas64(dataKeyCipherBytes)
        sparkSession.sparkContext.hadoopConfiguration
          .set("shield.write.dataKey.cipherText", dataKeyCipherTextStr)
        sparkSession.sparkContext.hadoopConfiguration
          .set("shield.cryptoMode", algorithmMode.encryptionAlgorithm)
      case _ =>
        invalidArgumentError("unknown or wrong encryptMode " + AlgorithmMode.toString)
    }
  }

  def csv(path: String): Unit = {
    setContext()
    df.write.options(extraOptions).mode(mode).csv(path)
    algorithmMode match {
      case AES_GCM_NOPADDING =>
        keyOperator.writeEncryptedDataKeyToMeta(path)
      case _ =>
    }
  }
}

