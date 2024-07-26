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

import com.huawei.analytics.shield.utils.LogError

import org.apache.hadoop.conf.Configuration

import java.util.Locale
import javax.crypto.Cipher

/**
 * basic crypto interface
 *
 * @since 2024/5/15
 */
trait Crypto {

  /**
   * execute part operation (encryption or decryption)
   *
   * @param content content to be processed.
   * @return processed content.
   */
  def update(content: Array[Byte]): Array[Byte]

  /**
   * execute part operation (encryption or decryption)
   *
   * @param content content to be processed.
   * @param offset offset
   * @param length length.
   * @return processed content.
   */
  def update(content: Array[Byte], offset: Int, length: Int): Array[Byte]

  /**
   * the final encrypt/decrypt step, process the given content and compute the mac
   *
   * @param content content to be processed.
   * @return (processed content, Message Authentication Code)
   */
  def doFinal(content: Array[Byte]): (Array[Byte], Array[Byte])

  /**
   * the final encrypt/decrypt step, process the given content and compute the mac
   *
   * @param content content to be processed.
   * @param offset offset.
   * @param length length.
   * @return (processed content, Message Authentication Code)
   */
  def doFinal(content: Array[Byte], offset: Int, length: Int): (Array[Byte], Array[Byte])
}

object Crypto {
  /**
   * create a crypto with cryptoMode.
   * @param cryptoMode CryptoMode
   * @return crypto
   */
  def apply(cryptoMode: AlgorithmMode, conf: Configuration): Crypto = {
    cryptoMode match {
      case AES_GCM_NOPADDING =>
        new ShieldCrypto(conf)
      case _ =>
        LogError.invalidArgumentError(s"wrong encrypt algorithm: $cryptoMode, only AES/GCM/NOPadding are supported.")
        null
    }
  }
}

/**
 * Operation Mode for crypto
 */
trait OperationMode extends Serializable {
  def opmode: Int
}

/**
 * Encrypt Operation Mode for crypto
 */
object ENCRYPT extends OperationMode {
  override def opmode: Int = Cipher.ENCRYPT_MODE
}

/**
 * Decrypt Operation Mode for crypto
 */
object DECRYPT extends OperationMode {
  override def opmode: Int = Cipher.DECRYPT_MODE
}

/**
 * CryptoMode
 */
trait AlgorithmMode extends Serializable {
  /**
   * encryption algorithm of crypto
   * @return encryption algorithm.
   */
  def encryptionAlgorithm: String

  /**
   * signing algorithm of crypto
   * @return signing algorithm.
   */
  def signingAlgorithm: String

  /**
   * secret key algorithm of crypto.
   * @return secret key algorithm.
   */
  def secretKeyAlgorithm: String
}

object AlgorithmMode {
  /**
   * Create cryptoMode by string.
   */
  def parse(s: String): AlgorithmMode = {
    s.toLowerCase(Locale.ROOT) match {
      case "aes/gcm/nopadding" =>
        AES_GCM_NOPADDING
      case "sm4/gcm/nopadding" =>
        SM4_GCM_NOPADDING
      case _ =>
        LogError.invalidArgumentError(s"wrong encrypt algorithm: $s, only AES/GCM/NOPadding ," +
          s" SM4/GCM/NOPADDING are supported.")
        null
    }
  }

  def parseDataFrame(s: String): AlgorithmMode = {
    s.toLowerCase(Locale.ROOT) match {
      case "aes/gcm/nopadding" =>
        AES_GCM_NOPADDING
      case _ =>
        LogError.invalidArgumentError(s"wrong encrypt algorithm: $s, only AES/GCM/NOPadding are supported.")
        null
    }
  }
}

/**
 * AlgorithmMode AES_GCM_NOPADDING.
 */
object AES_GCM_NOPADDING extends AlgorithmMode {
  /**
   * encryption algorithm of crypto
   * @return AES/GCM/NoPadding
   */
  override def encryptionAlgorithm: String = "AES/GCM/NoPadding"

  /**
   * signing algorithm of crypto
   * @return HmacSHA256
   */
  override def signingAlgorithm: String = "HmacSHA256"

  /**
   * secret key algorithm of crypto.
   * @return AES
   */
  override def secretKeyAlgorithm: String = "AES"
}

/**
 * AlgorithmMode SM4_GCM_NOPADDING.
 */
object SM4_GCM_NOPADDING extends AlgorithmMode {

  override def encryptionAlgorithm: String = "SM4/GCM/NoPadding"

  override def signingAlgorithm: String = "HmacSHA256"

  override def secretKeyAlgorithm: String = "SM4"
}

/**
 * AlgorithmMode PLAIN_TEXT.
 */
object PLAIN_TEXT extends AlgorithmMode {
  /**
   * encryption algorithm of crypto
   * @return plain_text
   */
  override def encryptionAlgorithm: String = "plain_text"

  /**
   * signing algorithm of crypto
   * @return
   */
  override def signingAlgorithm: String = ""

  /**
   * secret key algorithm of crypto.
   * @return
   */
  override def secretKeyAlgorithm: String = ""
}
