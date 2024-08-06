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

package com.huawei.analytics.shield

import com.huawei.analytics.shield.OmniContext.{getDatKeyLength, getKmsType, getPrimaryKeyNames}
import com.huawei.analytics.shield.crypto.dataframe.{ShieldDataFrameReader, ShieldDataFrameWriter}
import com.huawei.analytics.shield.crypto.AlgorithmMode
import com.huawei.analytics.shield.kms.common.{KeyOperator, KeyOperatorManager}
import com.huawei.analytics.shield.utils.LogError.invalidArgumentError
import com.huawei.analytics.shield.utils.ShieldConfParam.{DATA_KEY_LENGTH, PRIMARY_KEY_NAME, SPARK_IO_CODECS, genDataKeyLengthConf, genKmsTypeConf, genShieldCryptoModeConf, genShieldReadPrimaryKeyConf}
import com.huawei.analytics.shield.utils.ValueConf.{DATA_KEY_LENGTH_128, DEFAULT_DATA_KEY_LENGTH}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * OmniContext who wraps a SparkSession and provides read functions to
 * read encrypted data files to plain-text RDD or DataFrame, also provides
 * write functions to save DataFrame to encrypted data files.
 */
class OmniContext {
  protected var sparkSession: Option[SparkSession] = None
  protected var keyOperatorManger: KeyOperatorManager = new KeyOperatorManager


  /**
   * Interface for loading data in external storage to Dataset.
   *
   * @param cryptoMode crypto mode, such as PLAIN_TEXT or AES_GCM
   * @return a EncryptedDataFrameReader
   */
  def getDataFrameReader(cryptoMode: AlgorithmMode): ShieldDataFrameReader = {
    val keyOperator = loadSingleKeyOperator(cryptoMode)
    new ShieldDataFrameReader(this, cryptoMode, keyOperator)
  }

  /**
   * Interface for saving the content of the non-streaming Dataset out into external storage.
   *
   * @param dataFrame  dataframe to save.
   * @param cryptoMode crypto mode, such as PLAIN_TEXT or AES_XXX
   * @return a DataFrameWriter[Row]
   */
  def getDataFrameWriter(dataFrame: DataFrame,
                         cryptoMode: AlgorithmMode): ShieldDataFrameWriter = {
    val keyOperator = loadSingleKeyOperator(cryptoMode)
    new ShieldDataFrameWriter(this, dataFrame, cryptoMode, keyOperator)
  }

  /**
   * Get SparkSession from OmniContext
   *
   * @return SparkSession in OmniContext
   */
  def getSparkSession: SparkSession = {
    sparkSession.get
  }

  def getSparkConf: SparkConf = {
    sparkSession.get.sparkContext.getConf
  }

  def getHadoopConf: Configuration = {
    sparkSession.get.sparkContext.hadoopConfiguration
  }

  def loadSingleKeyOperator(cryptoMode: AlgorithmMode): KeyOperator = {
    val conf = getSparkConf
    val pk = getPrimaryKeyNames(conf)
    val keyLength = getDatKeyLength(conf)
    val kmsType = getKmsType(conf, pk)
    loadKeyOperator(pk, kmsType, keyLength, cryptoMode)
  }

  def loadKeyOperator(keyName: String, kmsType: String, keyLength: Int, cryptoMode: AlgorithmMode): KeyOperator = {
    val mapKey = keyOperatorManger.genOperatorKey(keyName, kmsType)
    if (keyOperatorManger.containKey(mapKey)) {
      keyOperatorManger.getOperator(mapKey)
    } else {
      val hadoopConfig: Configuration =
        if (SparkSession.builder().getOrCreate().sparkContext.hadoopConfiguration != null) {
          SparkSession.builder().getOrCreate().sparkContext.hadoopConfiguration
        } else {
          new Configuration()
        }
      val keyOperator = new KeyOperator
      keyOperator.init(keyName, kmsType, keyLength, cryptoMode, hadoopConfig)
      keyOperatorManger.putKeyOperator(mapKey, keyOperator)
      keyOperator
    }
  }

  def setCommonConfig(mode: String, dataKeyCipherStr: String, keyOperator: KeyOperator): Unit = {
    getHadoopConf.set(genShieldReadPrimaryKeyConf(dataKeyCipherStr), keyOperator.getPrimaryKeyName)
    getHadoopConf.set(genShieldCryptoModeConf(dataKeyCipherStr), mode)
    getHadoopConf.set(genDataKeyLengthConf(dataKeyCipherStr), keyOperator.getDataKeyLength.toString)
  }
}


object OmniContext {

  def initOmniContext(sparkConf: SparkConf,
                      appName: String,
                      args: Map[String, String]): OmniContext = {
    val conf = createSparkConf(sparkConf)
    args.foreach { arg =>
      conf.set(arg._1, arg._2)
    }
    initOmniContext(conf, appName)
  }

  def initOmniContext(sparkConf: SparkConf, appName: String): OmniContext = {
    val conf = createSparkConf(sparkConf)
    conf.setAppName(appName)
    conf.set(SPARK_IO_CODECS, "com.huawei.analytics.shield.crypto.CryptoCodec")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    loadOmniContext(sparkSession)
  }

  /**
   * init omni context with an existed SparkSession
   *
   * @param sparkSession a SparkSession
   * @return a omniContext
   */
  def initOmniContext(sparkSession: SparkSession): OmniContext = {
    val conf = sparkSession.sparkContext.getConf
    invalidArgumentError("spark.hadoop.io.compression.codecs not found!",
      !conf.contains(SPARK_IO_CODECS))
    invalidArgumentError("spark.hadoop.io.compression.codecs property must be " +
      "set to com.huawei.analytics.shield.crypto.CryptoCodec",
      conf.get(SPARK_IO_CODECS) != "com.huawei.analytics." +
        "shield.crypto.CryptoCodec")
    loadOmniContext(sparkSession)
  }

  /**
   * load key from kms and set key into KeyOperator
   *
   * @param sparkSession a SparkSession
   * @return a OmniContext
   */
  def loadOmniContext(sparkSession: SparkSession): OmniContext = {
    val omniContext = new OmniContext
    omniContext.sparkSession = Some(sparkSession)
    omniContext
  }

  private def getPrimaryKeyNames(conf: SparkConf): String = {
    invalidArgumentError("spark.shield.primaryKey.name not found!",
      !conf.contains(PRIMARY_KEY_NAME))
   conf.get(PRIMARY_KEY_NAME)
  }

  private def getDatKeyLength(conf: SparkConf): Int = {
    val dataKeyLength = conf.getInt(DATA_KEY_LENGTH, DEFAULT_DATA_KEY_LENGTH)
    invalidArgumentError("The value of dataKeyLength can only be 128 or 256.",
      dataKeyLength != DATA_KEY_LENGTH_128 && dataKeyLength != DEFAULT_DATA_KEY_LENGTH)
    dataKeyLength
  }

  private def getKmsType(conf: SparkConf, primaryKeyName: String): String = {
    val kmsType = genKmsTypeConf(primaryKeyName)
    invalidArgumentError("kms type not found!", !conf.contains(kmsType))
    conf.get(kmsType)
  }

  private def createSparkConf(existingConf: SparkConf = null): SparkConf = {
    var _conf = existingConf
    if (_conf == null) {
      _conf = new SparkConf()
    }
    _conf
  }
}