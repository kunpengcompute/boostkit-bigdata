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

package com.huawei.analytics.shield.ra.example.app

import com.huawei.analytics.shield.kms.example.{HadoopKeyManagementService, KeyMetadata}
import com.huawei.analytics.shield.ra.RemoteAttestationAgent

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.CryptoExtension
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension
import org.apache.spark.internal.Logging

/**
 * hadoop kms service
 */
class HadoopKmsRatsTLS protected() extends HadoopKeyManagementService with Logging {

  private var secret: String = null
  private val IV_LENGTH = 16

  private def unmangleIv(input: Array[Byte], output: Array[Byte]): Unit = {
    var i = 0
    while (i < output.length && i < input.length) {
      output(i) = (0xff ^ input(i)).toByte
      i += 1
    }
  }


  override def getDataKeyPlainText(primaryKeyName: String, config: Configuration,
                                   encryptedDataKeyString: Array[Byte]): Array[Byte] = {
    val clz = Class.forName("rats.RatsTLSRemoteAttestationAgent")
    val ins = clz.newInstance()
    val agent = ins.asInstanceOf[RemoteAttestationAgent]
    secret = agent.getSecret("key2")
    println(s"Successfully read the secret! key2 values : $secret")
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