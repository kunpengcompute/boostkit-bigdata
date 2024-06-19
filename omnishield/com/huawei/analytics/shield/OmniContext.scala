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

import com.huawei.analytics.shield.crypto.dataframe.{ShieldDataFrameReader, ShieldDataFrameWriter}
import com.huawei.analytics.shield.crypto.AlgorithmMode
import com.huawei.analytics.shield.kms.common.KeyOperator
import com.huawei.analytics.shield.kms._
import com.huawei.analytics.shield.utils.LogError.invalidArgumentError

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * OmniContext who wraps a SparkSession and provides read functions to
 * read encrypted data files to plain-text RDD or DataFrame, also provides
 * write functions to save DataFrame to encrypted data files.
 */
class OmniContext {
  protected var keyOperator = new KeyOperator
  protected var sparkSession: Option[SparkSession] = None
  protected var primaryKeyName: String = ""


  /**
   * Interface for loading data in external storage to Dataset.
   *
   * @param cryptoMode crypto mode, such as PLAIN_TEXT or AES_GCM
   * @return a EncryptedDataFrameReader
   */
  def getDataFrameReader(cryptoMode: AlgorithmMode): ShieldDataFrameReader = {
    new ShieldDataFrameReader(sparkSession.get, cryptoMode,
      primaryKeyName, keyOperator)

  }

  /**
   * Interface for saving the content of the non-streaming Dataset out into external storage.
   *
   * @param dataFrame  dataframe to save.
   * @param cryptoMode crypto mode, such as PLAIN_TEXT or AES_XXX
   * @param pkName     primaryKeyName
   * @return a DataFrameWriter[Row]
   */
  def getDataFrameWriter(dataFrame: DataFrame,
                         cryptoMode: AlgorithmMode): ShieldDataFrameWriter = {
    new ShieldDataFrameWriter(sparkSession.get, dataFrame, cryptoMode,
      primaryKeyName, keyOperator)
  }

  /**
   * Get SparkSession from OmniContext
   *
   * @return SparkSession in OmniContext
   */
  def getSparkSession: SparkSession = {
    sparkSession.get
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
    conf.set("spark.hadoop.io.compression.codecs", "com.huawei.analytics.shield.crypto.CryptoCodec")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val context = loadOmniContext(sparkSession)
    context
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
      !conf.contains("spark.hadoop.io.compression.codecs"))
    invalidArgumentError("spark.hadoop.io.compression.codecs property must be " +
      "set to com.huawei.analytics.shield.crypto.CryptoCodec",
      conf.get("spark.hadoop.io.compression.codecs") != "com.huawei.analytics." +
        "shield.crypto.CryptoCodec")
    val omniContext = loadOmniContext(sparkSession)
    omniContext
  }

  /**
   * load omni context
   *
   * @param sparkSession a SparkSession
   * @return a OmniContext
   */
  def loadOmniContext(sparkSession: SparkSession): OmniContext = {
    val omniContext = new OmniContext
    omniContext.sparkSession = Some(sparkSession)
    val conf = sparkSession.sparkContext.getConf
    val pk = getPrimaryKeyNames(conf)
    val keyLength = getDatKeyLength(conf)
    val kms = getKmsType(conf, pk)
    omniContext.primaryKeyName = pk
    omniContext.keyOperator = new KeyOperator(pk, kms, keyLength)
    omniContext
  }

  def getPrimaryKeyNames(conf: SparkConf): String = {
    invalidArgumentError("spark.shield.primaryKey.name not found!",
      !conf.contains(s"spark.shield.primaryKey.name"))
    val name = conf.get(s"spark.shield.primaryKey.name")
    name
  }

  def getDatKeyLength(conf: SparkConf): Int = {
    val dataKeyLength = conf.getInt(s"spark.shield.dataKey.length", 256)
    invalidArgumentError("The value of dataKeyLength can only be 128 or 256.",
      dataKeyLength != 128 && dataKeyLength != 256)
    dataKeyLength
  }

  def getKmsType(conf: SparkConf, primaryKeyName: String): KeyManagementService = {
    invalidArgumentError("kms type not found!",
      !conf.contains(s"spark.shield.primaryKey.$primaryKeyName.kms.type"))
    val kmsType = conf.get(s"spark.shield.primaryKey.$primaryKeyName.kms.type")
    val clz = Class.forName(kmsType).newInstance()
    val kms = clz.asInstanceOf[KeyManagementService]
    kms
  }

  def createSparkConf(existingConf: SparkConf = null): SparkConf = {
    var _conf = existingConf
    if (_conf == null) {
      _conf = new SparkConf()
    }
    _conf
  }

}
