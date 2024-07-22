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

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame

import java.io.{File, FileWriter}
import java.util.Base64

class SimpleKmsSpec extends DataFrameHelper {

  val tmp: File = createTmpDir("SimpleKms")
  val sparkConf: SparkConf = new SparkConf().setMaster("local[4]")
  sparkConf.set("spark.testing.memory", "512000000")
  val commonParam = Map(
    "spark.shield.primaryKey.name" -> "12345678123456781234567812345678",
    "spark.shield.primaryKey.12345678123456781234567812345678.kms.type" -> "com.huawei.analytics.shield.kms.example.SimpleKeyManagementService",
    "spark.shield.dataKey.length" -> "256"
  )
  val (csvPath, data) = generateCsvData(tmp.getPath)

  "use 128 key do encrypt/decrypt with simple kms" should "work" in {
    val gen128Param = Map(
      "spark.shield.primaryKey.name" -> "1234567812345678",
      "spark.shield.primaryKey.1234567812345678.kms.type" -> "com.huawei.analytics.shield.kms.example.SimpleKeyManagementService",
      "spark.shield.dataKey.length" -> "128"
    )
    val sc: OmniContext = OmniContext.initOmniContext(sparkConf, "gen128key", gen128Param)
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
    val decryptDF = sc.getDataFrameReader(AES_GCM_NOPADDING).schema(df.schema)
      .csv(tmp + "/128").sort("name")
    val d2 = decryptDF.schema.map(_.name).mkString(",") + "\n" +
      decryptDF.collect().map(v => s"${v.get(0)},${v.get(1)},${v.get(2)}").mkString("\n")
    val dePlainTextBase64 = sc.getSparkSession.sparkContext.hadoopConfiguration
      .get(s"shield.read.dataKey.$cipherTextBase64.plainText")
    val dePlainText = Base64.getDecoder.decode(dePlainTextBase64)
    dePlainText.length should be(16 + 16)
    d2 should be(d1)
  }

  "use 256 key do encrypt/decrypt with simple kms" should "work" in {
    val gen128Param = Map(
      "spark.shield.primaryKey.name" -> "12345678123456781234567812345678",
      "spark.shield.primaryKey.12345678123456781234567812345678.kms.type" -> "com.huawei.analytics.shield.kms.example.SimpleKeyManagementService",
      "spark.shield.dataKey.length" -> "256"
    )
    val sc: OmniContext = OmniContext.initOmniContext(sparkConf, "gen128key", gen128Param)
    val df: DataFrame = generateDF(sc.getSparkSession)
    sc.getDataFrameWriter(df, AES_GCM_NOPADDING).csv(tmp + "/256")
    val d1 = df.schema.map(_.name).mkString(",") + "\n" +
      df.collect().map(v => s"${v.get(0)},${v.get(1)},${v.get(2)}").mkString("\n")
    val plainTextBase64 = sc.getSparkSession.sparkContext
      .hadoopConfiguration.get("shield.write.dataKey.plainText")
    val cipherTextBase64 = sc.getSparkSession.sparkContext
      .hadoopConfiguration.get("shield.write.dataKey.cipherText")
    val enPlainText = Base64.getDecoder.decode(plainTextBase64)
    enPlainText.length should be(64)
    val decryptDF = sc.getDataFrameReader(AES_GCM_NOPADDING).schema(df.schema)
      .csv(tmp + "/256").sort("name")
    val d2 = decryptDF.schema.map(_.name).mkString(",") + "\n" +
      decryptDF.collect().map(v => s"${v.get(0)},${v.get(1)},${v.get(2)}").mkString("\n")
    val dePlainTextBase64 = sc.getSparkSession.sparkContext.hadoopConfiguration
      .get(s"shield.read.dataKey.$cipherTextBase64.plainText")
    val dePlainText = Base64.getDecoder.decode(dePlainTextBase64)
    dePlainText.length should be(64)
    d2 should be(d1)
  }

  "sparkSession.read" should "work" in {
    val omniContext: OmniContext = OmniContext.initOmniContext(sparkConf, "sc", commonParam)
    val df = omniContext.getDataFrameReader(PLAIN_TEXT).csv(csvPath)
    val d1 = df.collect().map(v => s"${v.get(0)},${v.get(1)},${v.get(2)}").mkString("\n")
    d1 + "\n" should be(data)
    val df2 = omniContext.getDataFrameReader(PLAIN_TEXT).addExtraOption("header", "true").csv(csvPath)
    val d2 = df2.schema.map(_.name).mkString(",") + "\n" +
      df2.collect().map(v => s"${v.get(0)},${v.get(1)},${v.get(2)}").mkString("\n")
    d2 + "\n" should be(data)
  }

  "read from plain csv with header" should "work" in {
    val omniContext: OmniContext = OmniContext.initOmniContext(sparkConf, "withHeader", commonParam)
    val df = omniContext.getDataFrameReader(PLAIN_TEXT)
      .addExtraOption("header", "true").csv(csvPath)
    val d = df.schema.map(_.name).mkString(",") + "\n" +
      df.collect().map(v => s"${v.get(0)},${v.get(1)},${v.get(2)}").mkString("\n")
    d + "\n" should be(data)
  }

  "read from plain csv without header" should "work" in {
    val omniContext: OmniContext = OmniContext.initOmniContext(sparkConf, "withOutHeader", commonParam)
    val df = omniContext.getDataFrameReader(PLAIN_TEXT).csv(csvPath)
    val d = df.collect().map(v => s"${v.get(0)},${v.get(1)},${v.get(2)}").mkString("\n")
    d + "\n" should be(data)
  }

//  "encrypt/Decrypt BigFile" should "work" in {
//    val bigFile = tmp + "/big_file.csv"
//    val outFile = tmp + "/plain_big_file.csv"
//    val enFile = tmp + "/en_big_file.csv" + CryptoCodec.getDefaultExtension()
//    val fw = new FileWriter(bigFile)
//    val genNum = 40000000
//    (0 until genNum).foreach { i =>
//      fw.append(s"gdni,$i,Engineer\npglyal,$i,Engineer\nyvomq,$i,Developer\n")
//    }
//    fw.close()
//    val conf = new Configuration
//    conf.setInt("spark.shield.dataKey.length", 256)
//
//    val crypto = new ShieldEncryptor(conf)
//    val dataKeyPlaintext = "ZmtnS1llYmU0MnJteUtJUw=="
//    conf.set("shield.write.dataKey.cipherText", dataKeyPlaintext)
//    conf.set("shield.cryptoMode", "AES/GCM/NoPadding")
//    crypto.init(conf)
//    crypto.doFinal(bigFile, enFile)
//
//    crypto.init(AES_GCM_NOPADDING, DECRYPT, dataKeyPlaintext)
//    crypto.doFinal(enFile, outFile)
//    new File(bigFile).length() should be(new File(outFile).length())
//  }

  "csv read/write different size" should "work" in {
    val omniContext: OmniContext = OmniContext.initOmniContext(sparkConf, "size", commonParam)
    val filteredPath = tmp + "/filtered-csv"
    val df = omniContext.getDataFrameReader(PLAIN_TEXT)
      .addExtraOption("header", "true").csv(csvPath)
    df.count() should be(repeatedNum * 3)
    (1 to 10).foreach { i =>
      val step = 1000
      val filtered = df.filter(_.getString(1).toInt < i * step)
      val filteredData = df.collect()
      omniContext.getDataFrameWriter(filtered, AES_GCM_NOPADDING).mode("overwrite")
        .option("header", "true").csv(filteredPath)
      val decrypt = omniContext.getDataFrameReader(AES_GCM_NOPADDING).addExtraOption("header", "true").csv(filteredPath)
      decrypt.count() should be(i * step * 3) //3000 6000
      decrypt.collect().zip(filteredData).foreach { v =>
        v._1.getAs[String]("age") should be(v._2.getAs[String]("age"))
        v._1.getAs[String]("job") should be(v._2.getAs[String]("job"))
        v._1.getAs[String]("name") should be(v._2.getAs[String]("name"))
      }
    }
  }

  "encrypt/decrypt json file with simple kms" should "work" in {
    val jsonParam = Map(
      "spark.shield.primaryKey.name" -> "1234567812345678",
      "spark.shield.primaryKey.1234567812345678.kms.type" -> "com.huawei.analytics.shield.kms.example.SimpleKeyManagementService",
      "spark.shield.dataKey.length" -> "128"
    )
    val sc: OmniContext = OmniContext.initOmniContext(sparkConf, "json", jsonParam)
    val df: DataFrame = generateDF(sc.getSparkSession)
    sc.getDataFrameWriter(df, AES_GCM_NOPADDING).json(tmp + "/json")
    val d1 = df.schema.map(_.name).mkString(",") + "\n" +
      df.collect().map(v => s"${v.get(0)},${v.get(1)},${v.get(2)}").mkString("\n")
    val decryptDF = sc.getDataFrameReader(AES_GCM_NOPADDING).schema(df.schema)
      .json(tmp + "/json").sort("name")
    val d2 = decryptDF.schema.map(_.name).mkString(",") + "\n" +
      decryptDF.collect().map(v => s"${v.get(0)},${v.get(1)},${v.get(2)}").mkString("\n")
    d2 should be(d1)
  }

  "encrypt/decrypt text file with simple kms" should "work" in {
    val jsonParam = Map(
      "spark.shield.primaryKey.name" -> "1234567812345678",
      "spark.shield.primaryKey.1234567812345678.kms.type" -> "com.huawei.analytics.shield.kms.example.SimpleKeyManagementService",
      "spark.shield.dataKey.length" -> "128"
    )
    val sc: OmniContext = OmniContext.initOmniContext(sparkConf, "text", jsonParam)
    val df: DataFrame = generateSingleColumnDF(sc.getSparkSession)
    sc.getDataFrameWriter(df, AES_GCM_NOPADDING).text(tmp + "/text")
    val d1 = df.schema.map(_.name).mkString(",") + "\n" +
      df.collect().map(v => s"${v.get(0)}").mkString("\n")
    val decryptDF = sc.getDataFrameReader(AES_GCM_NOPADDING).schema(df.schema)
      .text(tmp + "/text").sort("name")
    val d2 = decryptDF.schema.map(_.name).mkString(",") + "\n" +
      decryptDF.collect().map(v => s"${v.get(0)}").mkString("\n")
    d2 should be(d1)
  }
}