package com.huawei.analytics.shield.crypto.helper

import org.apache.hadoop.conf.Configuration

import scala.util.Random

class EncryptHelper extends DataFrameHelper {
  // create conf
  val conf: Configuration = new Configuration()
  val dataKeyPlaintext = "eJBD87n3DqeV7PPoYLMvAQpTWcJBW13M3uejvTXBLOTiMLsLy6vPp1Exg8+jKsls"
  val encryptedDataKey =
    "iLGGnWTafjavrNumKSwiENzvswhXiFUum/4G7LW7sg29dPgwUnaL3XJvTp+IwE0+0qenrsmEwT+zhojVQNnoWNrwGCaLmDCLKwUtNvERLPM="
  conf.set(s"shield.${encryptedDataKey}.cryptoMode", "AES/GCM/NOPadding")
  conf.set("shield.write.dataKey.plainText", dataKeyPlaintext)
  conf.set("shield.write.dataKey.cipherText", encryptedDataKey)
  conf.set("spark.shield.dataKey.length", "256")
  conf.set(s"shield.read.dataKey.$encryptedDataKey.plainText", dataKeyPlaintext)

  val inputDataLength: Int = 2000 * 2
  val inputData: Array[Byte] = new Array[Byte](inputDataLength)
  Random.nextBytes(inputData)
}
