package com.huawei.analytics.shield.kms.example

import com.huawei.analytics.shield.OmniContext
import com.huawei.analytics.shield.crypto.{AES_GCM_NOPADDING, PLAIN_TEXT}
import org.apache.spark.SparkConf

object EncryptAppExample {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf()
    val primaryKeyName = args(0)
    val kmsClassPath = args(1)
    val inputPath = args(2)
    val outputDir = args(3)
    //kms.type class path
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

    //read plain text (df) from inputPath
    val srcDF = sc.getDataFrameReader(PLAIN_TEXT).csv(inputPath)

    //write encrypt data to outputDir
    sc.getDataFrameWriter(srcDF, AES_GCM_NOPADDING).csv(outputDir)

    sc.getSparkSession.stop()
  }
}
