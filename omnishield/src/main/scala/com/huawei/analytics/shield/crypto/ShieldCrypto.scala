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

import org.apache.hadoop.conf.Configuration
import org.bouncycastle.jce.provider.BouncyCastleProvider

import java.security.spec.AlgorithmParameterSpec
import java.security.{SecureRandom, Security}
import java.util
import java.util.Base64
import javax.crypto.spec.{GCMParameterSpec, IvParameterSpec, SecretKeySpec}
import javax.crypto.{Cipher, Mac}

/**
 * ShieldCrypto general crypto for encrypt and decrypt data.
 *
 * @since 2024/5/15
 */
class ShieldCrypto(conf: Configuration) extends Crypto {
  protected var cipher: Cipher = null
  protected var mac: Mac = null
  protected var algorithmParameterSpec: AlgorithmParameterSpec = null
  protected var encryptionKeySpec: SecretKeySpec = null
  protected var signingKeySpec: SecretKeySpec = null
  protected var initializationVector: Array[Byte] = null
  protected var SIGNING_KEY_BYTE_LENGTH = 16
  protected var encryptKeyLength = conf.getInt(s"spark.shield.dataKey.length", 256) / 8
  protected var cryptoMode: AlgorithmMode = null
  protected val HEAD_LENGTH = 400
  protected val FIX_LENGTH = 8

  /**
   * Init this crypto with give params.
   *
   * @param mode             en/decrypt mode, one of ENCRYPT or DECRYPT.
   * @param dataKeyPlaintext signing key and data key.
   * @param conf             conf
   */
  def init(mode: OperationMode, dataKeyPlaintext: String, conf: Configuration): Unit = {
    init(mode, dataKeyPlaintext, conf, null)
  }

  /**
   * Init this crypto with give params.
   *
   * @param mode             en/decrypt mode, one of ENCRYPT or DECRYPT.
   * @param dataKeyPlaintext signing key and data key.
   * @param conf             conf
   * @param iv               iv
   */
  def init(mode: OperationMode, dataKeyPlaintext: String, conf: Configuration, iv: Array[Byte]): Unit = {
    this.cryptoMode = AlgorithmMode.parse(conf.get("shield.cryptoMode"))
    //    opMode = mode
    val secret = Base64.getDecoder.decode(dataKeyPlaintext)
    val signingKey = util.Arrays.copyOfRange(secret, 0, SIGNING_KEY_BYTE_LENGTH)
    val encryptKey =
      util.Arrays.copyOfRange(secret, SIGNING_KEY_BYTE_LENGTH, SIGNING_KEY_BYTE_LENGTH + encryptKeyLength)
    clearSensitiveInformation(secret)
    val r = new SecureRandom()
    val initializationVector = if (iv == null) Array.tabulate(16)(_ => (r.nextInt(256) - 128).toByte) else iv
    algorithmParameterSpec = createParameterSpec(cryptoMode, initializationVector)
    encryptionKeySpec = new SecretKeySpec(encryptKey, cryptoMode.secretKeyAlgorithm)
    clearSensitiveInformation(encryptKey)
    Security.addProvider(new BouncyCastleProvider)
    cipher = Cipher.getInstance("AES/GCM/NoPadding", BouncyCastleProvider.PROVIDER_NAME)
    cipher.init(mode.opmode, encryptionKeySpec, algorithmParameterSpec)
    mac = Mac.getInstance(cryptoMode.signingAlgorithm)
    signingKeySpec = new SecretKeySpec(signingKey, cryptoMode.signingAlgorithm)
    clearSensitiveInformation(signingKey)
    mac.init(signingKeySpec)
  }

  /**
   * create the corresponding AlgorithmParameterSpec based on cryptoMode.
   *
   * @param cryptoMode           cryptoMode to en/decrypt data, such as AES_GCM_NOPADDING.
   * @param initializationVector the IV source buffer.
   * @return AlgorithmParameterSpec
   */
  protected def createParameterSpec(cryptoMode: AlgorithmMode, initializationVector: Array[Byte]): AlgorithmParameterSpec = {
    cryptoMode match {
      case AES_GCM_NOPADDING => new GCMParameterSpec(128, initializationVector)
      case _ => new IvParameterSpec(initializationVector)
    }
  }

  override def update(content: Array[Byte]): Array[Byte] = {
    val cipherResult: Array[Byte] = cipher.update(content)
    mac.update(cipherResult)
    cipherResult
  }

  def doFinal(): (Array[Byte], Array[Byte]) = {
    val cipherResult: Array[Byte] = cipher.doFinal()
    val hmac: Array[Byte] = mac.doFinal(cipherResult)
    (cipherResult, hmac)
  }

  override def update(content: Array[Byte], offset: Int, length: Int): Array[Byte] = {
    val cipherResult: Array[Byte] = cipher.update(content, offset, length)
    mac.update(cipherResult)
    cipherResult
  }

  override def doFinal(content: Array[Byte]): (Array[Byte], Array[Byte]) = {
    val cipherResult: Array[Byte] = cipher.doFinal(content)
    val hmac: Array[Byte] = mac.doFinal(cipherResult)
    (cipherResult, hmac)
  }

  override def doFinal(content: Array[Byte], offset: Int, length: Int): (Array[Byte], Array[Byte]) = {
    val cipherResult: Array[Byte] = cipher.doFinal(content, offset, length)
    val hmac: Array[Byte] = mac.doFinal(cipherResult)
    (cipherResult, hmac)
  }

  /**
   * clear sensitive information(eg. datakey, signingkey)
   *
   * @param info given sensitive information bytes
   */
  def clearSensitiveInformation(info: Array[Byte]): Unit = {
    util.Arrays.fill(info, 0x00.toByte)
  }
}

object ShieldCrypto {

  /**
   * Create encrypter by type string
   */
  def apply(conf: Configuration): ShieldCrypto = {
    new ShieldCrypto(conf)
  }
}
