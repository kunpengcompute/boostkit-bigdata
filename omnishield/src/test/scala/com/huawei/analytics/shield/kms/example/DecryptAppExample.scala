package com.huawei.analytics.shield.kms.example

import com.huawei.analytics.shield.OmniContext
import com.huawei.analytics.shield.crypto.{AES_GCM_NOPADDING, PLAIN_TEXT}
import org.apache.spark.SparkConf

object DecryptAppExample {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf()
    val primaryKeyName = args(0)
    val kmsClassPath = args(1)
    val inputDir = args(2)
    val outputDir = args(3)
    val shieldParam = Map(
      s"spark.shield.primaryKey.name" -> s"$primaryKeyName",
      s"spark.shield.primaryKey.$primaryKeyName.kms.type" -> s"$kmsClassPath",
      "spark.shield.dataKey.length" -> "128",
      "spark.hadoop.io.compression.codecs" -> "com.huawei.analytics.shield.crypto.CryptoCodec"
    )
    //set shieldParam to spark conf
    shieldParam.foreach { arg =>
      sparkConf.set(arg._1, arg._2)
    }
    val sc: OmniContext = OmniContext.initOmniContext(sparkConf, "EncryptAppExample")

    //read encrypt data from inputDir
    val encryptDF = sc.getDataFrameReader(AES_GCM_NOPADDING).csv(inputDir)

    //write plain text to outputDir
    sc.getDataFrameWriter(encryptDF, PLAIN_TEXT).csv(outputDir)

    sc.getSparkSession.stop()
  }
}
