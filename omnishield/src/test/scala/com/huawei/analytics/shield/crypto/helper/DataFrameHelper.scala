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


package com.huawei.analytics.shield.crypto.helper

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.FileWriter
import scala.util.Random


class DataFrameHelper extends ShieldSpecHelper {

  val header = "name,age,job\n"
  val schema: StructType = StructType(List(
    StructField("name", StringType),
    StructField("age", StringType),
    StructField("job", StringType)
  ))
  val singleSchema: StructType = StructType(List(
    StructField("name", StringType)
  ))
  val repeatedNum = 100000

  def generateKeys(): (String, String) = {
    val appid: String = "123456789012"
    val apikey: String = "210987654321"
    (appid, apikey)
  }

  def generateSingleColumnDF(spark: SparkSession): DataFrame = {
    val rdd = spark.sparkContext.parallelize(Seq(
      Row("gdni"),
      Row("pglyal"),
      Row("yvomq")
    ))
    spark.createDataFrame(rdd, singleSchema).sort("name")
  }

  def generateDF(spark: SparkSession): DataFrame = {
    val rdd = spark.sparkContext.parallelize(Seq(
      Row("gdni", "25", "Engineer"),
      Row("pglyal", "30", "Engineer"),
      Row("yvomq", "26", "Developer")
    ))
    val df = spark.createDataFrame(rdd, schema).sort("name")
    df
  }

  def generateCsvData(basePath: String): (String, String) = {
    Random.setSeed(1)
    val fileName = basePath + "/people.csv"
    val fw = new FileWriter(fileName)
    val data = new StringBuilder()
    data.append(header)
    (0 until repeatedNum).foreach { i =>
      data.append(s"gdni,$i,Engineer\npglyal,$i,Engineer\nyvomq,$i,Developer\n")
    }
    fw.append(data)
    fw.close()
    (fileName, data.toString())
  }

}


