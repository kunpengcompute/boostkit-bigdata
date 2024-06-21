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
import com.huawei.analytics.shield.utils.Encrypt
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.io.File

class EncryptUtilSpec extends DataFrameHelper {

  val tmp: File = createTmpDir("utils")

  val (csvPath, data) = generateCsvData(tmp.getPath)

  "no compression set" should "work" in {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]")
    sparkConf.set("spark.testing.memory", "512000000")
    sparkConf.set("spark.shield.primaryKey.name",
      "12345678123456781234567812345678")
    sparkConf.set("spark.shield.12345678123456781234567812345678.defaultKey.kms.type",
      "com.huawei.analytics.shield.kms.example.SimpleKeyManagementService")
    SparkSession.builder().config(sparkConf).getOrCreate()
    val inputFilePath = tmp.getAbsoluteFile + "/people.csv"
    val outputFilePath = tmp.getAbsoluteFile + "/en"
    val args = Array("-i", s"file://$inputFilePath", "-o", s"file://$outputFilePath", "-a", "aes/gcm/nopadding", "-e", "encrypt", "-t", "csv")
    var hasErr = false
    try {
      Encrypt.main(args)
    } catch {
      case e: IllegalArgumentException =>
        hasErr = true
        e.getMessage should be("spark.hadoop.io.compression.codecs not found!")
    } finally {
      hasErr should be(true)
    }
  }

  "use EncryptUtil encrypt/decrypt" should "work" in {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]")
    sparkConf.set("spark.testing.memory", "512000000")
    sparkConf.set("spark.shield.primaryKey.name",
      "12345678123456781234567812345678")
    sparkConf.set("spark.shield.primaryKey.12345678123456781234567812345678.kms.type",
      "com.huawei.analytics.shield.kms.example.SimpleKeyManagementService")
    sparkConf.set("spark.hadoop.io.compression.codecs",
      "com.huawei.analytics.shield.crypto.CryptoCodec")
    //start encrypt csv file
    SparkSession.builder().config(sparkConf).getOrCreate()
    val enInput = tmp.getAbsoluteFile + "/people.csv"
    val enOutput = tmp.getAbsoluteFile + "/en"
    val enArgs = Array("-i", s"file://$enInput", "-o", s"file://$enOutput", "-a", "aes/gcm/nopadding",
      "-e", "encrypt", "-t", "csv")
    Encrypt.main(enArgs)
    //start decrypt csv file
    if (SparkSession.getDefaultSession.isDefined) {
      SparkSession.getDefaultSession.get.stop()
    }
    SparkSession.builder().config(sparkConf).getOrCreate()
    val inputFilePath = tmp.getAbsoluteFile + "/en"
    val outputFilePath = tmp.getAbsoluteFile + "/de"
    val deArgs = Array("-i", s"file://$inputFilePath", "-o", s"file://$outputFilePath", "-a", "aes/gcm/nopadding",
      "-e", "decrypt", "-t", "csv")
    Encrypt.main(deArgs)
    //check res after decrypt
    val omniContext: OmniContext = OmniContext.initOmniContext(sparkConf, "util")
    val df1 = omniContext.getDataFrameReader(PLAIN_TEXT).schema(schema).csv(csvPath)
    val df2 = omniContext.getDataFrameReader(PLAIN_TEXT).schema(schema).csv(tmp.getAbsoluteFile +"/de/part*")
    val df1data = df1.schema.map(_.name).mkString(",") + "\n" +
      df1.collect().map(v => s"${v.get(0)},${v.get(1)},${v.get(2)}").mkString("\n")
    val df2data = df2.schema.map(_.name).mkString(",") + "\n" +
      df2.collect().map(v => s"${v.get(0)},${v.get(1)},${v.get(2)}").mkString("\n")
    df1data should be(df2data)
  }
}
