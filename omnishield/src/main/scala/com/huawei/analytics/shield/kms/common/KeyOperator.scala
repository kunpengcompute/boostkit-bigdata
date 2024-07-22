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

package com.huawei.analytics.shield.kms.common

import com.huawei.analytics.shield.kms.KeyManagementService

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

import java.net.URI
import java.util.Base64

/**
 * Tool for loading keys from the KMS
 *
 * @param primaryKeyName primaryKeyName or primaryKeyId
 * @param kms            kms
 */
class KeyOperator(primaryKeyName: String = "",
                  kms: KeyManagementService = null,
                  dataKeyLength: Int = 256) {
  val META_FILE_NAME = ".meta"
  protected var encryptedDataKey: Array[Byte] = null
  protected val hadoopConfig: Configuration =
    if (SparkSession.builder().getOrCreate().sparkContext.hadoopConfiguration != null) {
      SparkSession.builder().getOrCreate().sparkContext.hadoopConfiguration
    } else {
      new Configuration()
    }

  def getDataKeyLength:Int = {
    dataKeyLength
  }

  /**
   * Convert Array[Byte] to String by base64
   *
   * @param input input
   * @return
   */
  def encoderWithBas64(input: Array[Byte]): String = {
    Base64.getEncoder.encodeToString(input)
  }

  /**
   * get data key PlainText from encryptedDataKey
   *
   * @param encryptedDataKey encryptedDataKey
   * @return dataKeyPlainText
   */
  def getDataKeyPlainText(encryptedDataKey: String): Array[Byte] = {
    kms.setDataKeyLength(dataKeyLength)
    val encryptedDataKeyBytes = Base64.getDecoder.decode(encryptedDataKey)
    val plainBytes = kms.getDataKeyPlainText(primaryKeyName, hadoopConfig, encryptedDataKeyBytes)
    plainBytes
  }

  /**
   * for encrypted data key, generate data key and encrypt it by kms
   * for data key PlainText, decrypt encryptDataKey by kms
   *
   * @return data key PlainText and  encrypted data key
   */
  def getDataKeyPair: (Array[Byte], Array[Byte]) = {
    kms.setDataKeyLength(dataKeyLength)
    encryptedDataKey = kms.getEncryptDataKey(primaryKeyName, hadoopConfig)
    val dataKeyPlainText = kms.getDataKeyPlainText(primaryKeyName, hadoopConfig, encryptedDataKey)
    (dataKeyPlainText, encryptedDataKey)
  }

  def getMetaPath(outputPath: String): String = {
    outputPath + "/" + META_FILE_NAME
  }

  /**
   * write encryptedDataKey to meta after spark df has been written
   *
   * @param fileDirPath filepath
   */
  def writeEncryptedDataKeyToMeta(fileDirPath: String): Unit = {
    val metaPath = new Path(getMetaPath(fileDirPath)).toString
    val encryptedDataKeyStr = Base64.getEncoder.encodeToString(encryptedDataKey)
    val jsonStr = KmsMetaFormatSerializer(KmsMetaFormat(encryptedDataKeyStr))
    val keyReaderWriter = new FileMetaHandler
    keyReaderWriter.writeKeyToFile(metaPath, jsonStr, hadoopConfig)
  }

  /**
   * read base64 string from meta file
   *
   * @param fileDirPath path
   * @return encryptKey(base64)
   */
  def readEncryptedDataKeyFromMeta(fileDirPath: String): String = {
    val metaPath = new Path(getMetaPath(fileDirPath)).toString
    val fs: FileSystem = FileSystem.get(new URI(metaPath), hadoopConfig)
    val inStream = fs.open(new Path(metaPath))
    val jsonStr = scala.io.Source.fromInputStream(inStream).takeWhile(_ != null).mkString
    val encryptedDataKey = KmsMetaFormatSerializer(jsonStr).encryptedDataKey
    encryptedDataKey
  }
}

class FileMetaHandler {

  def writeKeyToFile(path: String, content: String,
                     config: Configuration = null): Unit = {
    val hadoopConfig = if (config != null) config else new Configuration()
    val fs: FileSystem = FileSystem.get(new URI(path), hadoopConfig)
    val outputStream = fs.create(new Path(path))
    outputStream.writeBytes(content + "\n")
    outputStream.close()
  }

  def readKeyFromFile(path: String, config: Configuration = null): String = {
    val hadoopConfig = if (config != null) config else new Configuration()
    val fs = FileSystem.get(new URI(new Path(path).toString), hadoopConfig)
    val inStream = fs.open(new Path(path))
    val content = scala.io.Source.fromInputStream(inStream).getLines().next()
    content
  }
}
