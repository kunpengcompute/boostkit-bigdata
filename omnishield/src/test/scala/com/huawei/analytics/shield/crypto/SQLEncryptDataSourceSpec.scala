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

import com.huawei.analytics.shield.crypto.helper.EncryptHelper

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf

class SQLEncryptDataSourceSpec extends EncryptHelper {

  val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").set("spark.testing.memory", "512000000")
    .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, "com.huawei.analytics.shield.sql.DataSourceEncryptPlugin")
    .set("spark.hadoop.io.compression.codecs", "com.huawei.analytics.shield.crypto.CryptoCodec")

  val tableTypes = List("text", "csv", "json")
  val modeTypes = List("aes", "sm4")

  "create encrypt text/csv/json data source" should "work" in {
    val sc = SparkSession.builder().config(sparkConf).getOrCreate()
    for (tb <- tableTypes) {
      for (mode <- modeTypes) {
        sc.sql(s"drop table if exists ${tb}tb")
        sc.sql(s"create table ${tb}tb (id string) using ${tb} " +
          "OPTIONS (`keyname` '12345678123456781234567812345678'," +
          "`encrypt` 'true'," +
          s"`cryptomode` '${mode}/gcm/nopadding'," +
          "`kmstype` 'com.huawei.analytics.shield.kms.example.SimpleKeyManagementService'," +
          "`keylength` '128')")
        val res = sc.sql(s"show create table ${tb}tb").first().get(0).toString
        res.contains("encryptdatakey") should be(true)
        sc.sql(s"drop table ${tb}tb")
      }
    }
  }

  "insert encrypt text/csv/json data" should "work" in {
    val sc = SparkSession.builder().config(sparkConf).getOrCreate()
    val tableTypes = List("text", "csv", "json")
    for (tb <- tableTypes) {
      for (mode <- modeTypes) {
        sc.sql(s"drop table if exists ${tb}tb")
        sc.sql(s"create table ${tb}tb (id string) using ${tb} " +
          "OPTIONS (`keyname` '12345678123456781234567812345678'," +
          "`encrypt` 'true'," +
          s"`cryptomode` '${mode}/gcm/nopadding'," +
          "`kmstype` 'com.huawei.analytics.shield.kms.example.SimpleKeyManagementService'," +
          "`keylength` '128')")
        sc.sql(s"insert into ${tb}tb values ('abc')")
        val res = sc.sql(s"select id from ${tb}tb").first().get(0).toString
        res should be("abc")
        sc.sql(s"drop table ${tb}tb")
      }
    }
  }
}
