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

package com.huawei.analytics.shield.kms.example

import com.huawei.analytics.shield.kms.KeyManagementService
import com.huawei.analytics.shield.utils.LogError.invalidArgumentError

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.CryptoExtension
import org.apache.hadoop.crypto.key.{KeyProvider, KeyProviderCryptoExtension, KeyProviderFactory}
import org.apache.spark.internal.Logging

import java.security.SecureRandom

/**
 * hadoop kms service
 */
class HadoopKeyManagementService protected() extends KeyManagementService with Logging {

  def apply(): HadoopKeyManagementService = {
    logInfo("hadoop kms init")
    new HadoopKeyManagementService()
  }

  def setDataKeyLength(length: Int): Unit = {
    DATA_KEY_LENGTH = length / 8
  }

  private val random = new SecureRandom
  private var DATA_KEY_LENGTH = 32
  private val SIGN_KEY_LENGTH = 16
  private val IV_LENGTH = 16

  /**
   * get iv from input
   *
   * @param input  input
   * @param output output
   */
  private def unmangleIv(input: Array[Byte], output: Array[Byte]): Unit = {
    var i = 0
    while (i < output.length && i < input.length) {
      output(i) = (0xff ^ input(i)).toByte
      i += 1
    }
  }

  def buildKeyVersionName(key: KeyMetadata): String = key.getKeyName + "@" + key.getVersion

  def getProvider(config: Configuration): KeyProvider = {
    val providerList = KeyProviderFactory.getProviders(config)
    invalidArgumentError("No KMS is initialized.", providerList.size() == 0)
    providerList.get(0)
  }

  override def getEncryptDataKey(primaryKeyName: String, config: Configuration): Array[Byte] = {
    val provider = getProvider(config)
    val meta = provider.getMetadata(primaryKeyName)
    val key = new KeyMetadata(primaryKeyName, meta.getVersions - 1, meta.getAlgorithm)
    log.info(s"hadoop kms use ${DATA_KEY_LENGTH * 8} bit dataKey")
    val encryptedKey = new Array[Byte](SIGN_KEY_LENGTH + DATA_KEY_LENGTH)
    random.nextBytes(encryptedKey)
    val iv = new Array[Byte](IV_LENGTH)
    unmangleIv(encryptedKey, iv)

    KeyProviderCryptoExtension.EncryptedKeyVersion.createForDecryption(key.getKeyName,
      buildKeyVersionName(key), iv, encryptedKey)
    encryptedKey
  }

  override def getDataKeyPlainText(primaryKeyName: String, config: Configuration,
                                   encryptedDataKeyString: Array[Byte]): Array[Byte] = {
    val provider = getProvider(config)
    val meta = provider.getMetadata(primaryKeyName)
    val key = new KeyMetadata(primaryKeyName, meta.getVersions - 1, meta.getAlgorithm);
    val encryptedKey = encryptedDataKeyString
    val iv = new Array[Byte](IV_LENGTH)
    unmangleIv(encryptedKey, iv)

    val param: KeyProviderCryptoExtension.EncryptedKeyVersion =
      KeyProviderCryptoExtension.EncryptedKeyVersion.createForDecryption(key.getKeyName,
        buildKeyVersionName(key), iv, encryptedKey)
    provider match {
      case extension: KeyProviderCryptoExtension =>
        val decryptedKey = extension.decryptEncryptedKey(param)
        decryptedKey.getMaterial
      case extension: CryptoExtension =>
        val decryptedKey = extension.decryptEncryptedKey(param)
        decryptedKey.getMaterial
      case _ => throw new UnsupportedOperationException(provider.getClass.getCanonicalName + " is not supported.")
    }
  }
}

class KeyMetadata(keyName: String = null, version: Int = 0, algorithm: String) {
  /**
   * Get the name of the key.
   */
  def getKeyName: String = {
    keyName
  }

  /**
   * Get the encryption algorithm for this key.
   *
   * @return the algorithm
   */
  def getAlgorithm: String = {
    algorithm
  }

  /**
   * Get the version of this key.
   *
   * @return the version
   */
  def getVersion: Int = {
    version
  }

  override def toString: String = {
    keyName + '@' + version + ' ' + algorithm
  }
}
