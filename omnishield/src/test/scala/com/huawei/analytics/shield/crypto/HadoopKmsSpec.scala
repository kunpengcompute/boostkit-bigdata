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

package com.huawei.analytics.shield.crypto

import com.huawei.analytics.shield.OmniContext
import com.huawei.analytics.shield.crypto.helper.DataFrameHelper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.crypto.key.{KeyProvider, KeyProviderFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import java.util.Base64

class HadoopKmsSpec extends DataFrameHelper {
  val tmp: File = createTmpDir("HadoopKms")
  var keyProvider: KeyProvider = null

  def getProvider(config: Configuration): KeyProvider = {
    if (keyProvider == null) {
      val providerList = KeyProviderFactory.getProviders(config)
      keyProvider = providerList.get(0)
    }
    keyProvider
  }

  def creatPk(primaryKeyName: String, hadoopConf: Configuration): Unit = {

    val provider = getProvider(hadoopConf)
    val pKey = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf)
    val aes128 = new KeyProvider.Options(hadoopConf)
    provider.createKey(primaryKeyName, pKey, aes128)
  }

  def delPk(primaryKeyName: String): Unit = {
    val hadoopConf = SparkSession.getDefaultSession.get.sparkContext.hadoopConfiguration
    val provider = getProvider(hadoopConf)
    provider.deleteKey(primaryKeyName)
  }

  "use 128 key do encrypt/decrypt with hadoop kms" should "work" in {
    val primaryKeyName = "tkey1"
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]")
    val gen128Param = Map(
      "spark.testing.memory" -> "512000000",
      s"spark.shield.primaryKey.name" -> s"$primaryKeyName",
      s"spark.shield.primaryKey.$primaryKeyName.kms.type" -> "com.huawei.analytics.shield.kms.example.HadoopKeyManagementService",
      "spark.shield.dataKey.length" -> "128",
      "spark.hadoop.io.compression.codecs" -> "com.huawei.analytics.shield.crypto.CryptoCodec"
    )
    gen128Param.foreach { arg =>
      sparkConf.set(arg._1, arg._2)
    }
    val sparkSc = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSc.sparkContext.hadoopConfiguration.set("hadoop.security.key.provider.path", "kms://http@localhost:9600/kms")
    val hadoopConf = SparkSession.getDefaultSession.get.sparkContext.hadoopConfiguration
    creatPk(primaryKeyName, hadoopConf)

    val sc: OmniContext = OmniContext.initOmniContext(sparkConf, "gen128key")
    val df: DataFrame = generateDF(sc.getSparkSession)
    sc.getDataFrameWriter(df, AES_GCM_NOPADDING).csv(tmp + "/128")
    val d1 = df.schema.map(_.name).mkString(",") + "\n" +
      df.collect().map(v => s"${v.get(0)},${v.get(1)},${v.get(2)}").mkString("\n")
    val plainTextBase64 = sc.getSparkSession.sparkContext
      .hadoopConfiguration.get("shield.write.dataKey.plainText")
    val cipherTextBase64 = sc.getSparkSession.sparkContext
      .hadoopConfiguration.get("shield.write.dataKey.cipherText")
    val enPlainText = Base64.getDecoder.decode(plainTextBase64)
    enPlainText.length should be(16 + 16)
    val decryptDF = sc.getDataFrameReader(AES_GCM_NOPADDING).schema(df.schema).csv(tmp + "/128")
    val d2 = decryptDF.schema.map(_.name).mkString(",") + "\n" +
      decryptDF.collect().map(v => s"${v.get(0)},${v.get(1)},${v.get(2)}").mkString("\n")
    val dePlainTextBase64 = sc.getSparkSession.sparkContext.hadoopConfiguration
      .get(s"shield.read.dataKey.$cipherTextBase64.plainText")
    val dePlainText = Base64.getDecoder.decode(dePlainTextBase64)
    dePlainText.length should be(16 + 16)
    d2 should be(d1)
    delPk(primaryKeyName)
  }

  "use 256 key do encrypt/decrypt with hadoop kms" should "work" in {
    val primaryKeyName = "tkey2"
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]")
    val gen256Param = Map(
      "spark.testing.memory" -> "512000000",
      s"spark.shield.primaryKey.name" -> s"$primaryKeyName",
      s"spark.shield.primaryKey.$primaryKeyName.kms.type" -> "com.huawei.analytics.shield.kms.example.HadoopKeyManagementService",
      "spark.shield.dataKey.length" -> "256",
      "spark.hadoop.io.compression.codecs" -> "com.huawei.analytics.shield.crypto.CryptoCodec"
    )
    gen256Param.foreach { arg =>
      sparkConf.set(arg._1, arg._2)
    }
    val sparkSc = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSc.sparkContext.hadoopConfiguration.set("hadoop.security.key.provider.path", "kms://http@localhost:9600/kms")
    val hadoopConf = SparkSession.getDefaultSession.get.sparkContext.hadoopConfiguration
    creatPk(primaryKeyName, hadoopConf)

    val sc: OmniContext = OmniContext.initOmniContext(sparkConf, "gen256key")
    val df: DataFrame = generateDF(sc.getSparkSession)
    sc.getDataFrameWriter(df, AES_GCM_NOPADDING).csv(tmp + "/256")
    val d1 = df.schema.map(_.name).mkString(",") + "\n" +
      df.collect().map(v => s"${v.get(0)},${v.get(1)},${v.get(2)}").mkString("\n")
    val plainTextBase64 = sc.getSparkSession.sparkContext
      .hadoopConfiguration.get("shield.write.dataKey.plainText")
    val cipherTextBase64 = sc.getSparkSession.sparkContext
      .hadoopConfiguration.get("shield.write.dataKey.cipherText")
    val enPlainText = Base64.getDecoder.decode(plainTextBase64)
    enPlainText.length should be(16 + 32)
    val decryptDF = sc.getDataFrameReader(AES_GCM_NOPADDING).schema(df.schema).csv(tmp + "/256")
    val d2 = decryptDF.schema.map(_.name).mkString(",") + "\n" +
      decryptDF.collect().map(v => s"${v.get(0)},${v.get(1)},${v.get(2)}").mkString("\n")
    val dePlainTextBase64 = sc.getSparkSession.sparkContext.hadoopConfiguration
      .get(s"shield.read.dataKey.$cipherTextBase64.plainText")
    val dePlainText = Base64.getDecoder.decode(dePlainTextBase64)
    dePlainText.length should be(16 + 32)
    d2 should be(d1)
    delPk(primaryKeyName)
  }
}