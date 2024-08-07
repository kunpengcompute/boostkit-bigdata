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

import com.huawei.analytics.shield.crypto.AlgorithmMode
import com.huawei.analytics.shield.kms.KeyManagementService
import com.huawei.analytics.shield.utils.LogError.invalidArgumentError
import com.huawei.analytics.shield.utils.ShieldConfParam.{genDataKeyLengthConf, genKmsTypeConf, genShieldCryptoModeConf, genShieldReadPrimaryKeyConf}
import com.huawei.analytics.shield.utils.ValueConf.DEFAULT_DATA_KEY_LENGTH

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import java.net.URI
import java.util.Base64

/**
 * Tool for loading keys from the KMS
 *
 */
class KeyOperator() {
  private val META_FILE_NAME = ".meta"
  private var encryptedDataKey: Array[Byte] = _
  private var primaryKeyName: String = _
  private var kms: KeyManagementService = _
  private var dataKeyLength: Int = 256
  private var mode: AlgorithmMode = _
  private var hadoopConfig: Configuration = _

  def init(encryptedDataKeyStr:String, conf: Configuration): Unit = {
    encryptedDataKey = decoderWithBase64(encryptedDataKeyStr)
    val keyName = conf.get(genShieldReadPrimaryKeyConf(encryptedDataKeyStr))
    val kmsType = conf.get(genKmsTypeConf(keyName))
    val keyLength = conf.getInt(genDataKeyLengthConf(encryptedDataKeyStr),DEFAULT_DATA_KEY_LENGTH)
    val cryptoMode = AlgorithmMode.parse(conf.get(genShieldCryptoModeConf(encryptedDataKeyStr)))
    init(keyName,kmsType,keyLength,cryptoMode,conf)
  }

  def init(keyName: String, kmsType: String, keyLength: Int, algorithmMode: AlgorithmMode, conf: Configuration): Unit = {
    primaryKeyName = keyName
    kms = loadKmsService(kmsType)
    dataKeyLength = keyLength
    mode = algorithmMode
    hadoopConfig = conf
  }

  def getDataKeyLength:Int = {
    dataKeyLength
  }

  def getPrimaryKeyName:String = {
    primaryKeyName
  }

  def getAlgorithmMode:AlgorithmMode = {
    mode
  }

  /**
   * Convert Array[Byte] to String by base64
   *
   * @param input input
   * @return
   */
  def encoderWithBase64(input: Array[Byte]): String = {
    Base64.getEncoder.encodeToString(input)
  }

  /**
   * Convert Array[Byte] to String by base64
   *
   * @param input input
   * @return
   */
  def decoderWithBase64(input: String): Array[Byte] = {
    Base64.getDecoder.decode(input)
  }

  /**
   * get data key PlainText from encryptedDataKey
   *
   * @param encryptedDataKey encryptedDataKey
   * @return dataKeyPlainText
   */
  def getDataKeyPlainText(encryptedDataKey: String): Array[Byte] = {
    val encryptedDataKeyBytes = Base64.getDecoder.decode(encryptedDataKey)
    val plainBytes = kms.getDataKeyPlainText(primaryKeyName, hadoopConfig, encryptedDataKeyBytes)
    plainBytes
  }

  def getDataKeyCipherText: Array[Byte] = {
    encryptedDataKey = kms.getEncryptDataKey(primaryKeyName, dataKeyLength, hadoopConfig)
    encryptedDataKey
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
    if (encryptedDataKey == null) {
      invalidArgumentError("encryptedDataKey is null. Generate encryptedDataKey before writing it to meta file." )
    }
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

  private def loadKmsService(className: String): KeyManagementService = {
    val clz = Class.forName(className).newInstance()
    val kms = clz.asInstanceOf[KeyManagementService]
    kms
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