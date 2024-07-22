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
import com.huawei.analytics.shield.utils.LogError.invalidOperationError

import org.apache.hadoop.conf.Configuration

import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
 * shield decryptor
 *
 * @since 2024/5/15
 */
class ShieldDecryptor(conf: Configuration) extends ShieldCrypto(conf: Configuration) {
  val hmacSize = 32

  /**
   * init a decryptor
   *
   * @param dataKeyPlaintext     dataKeyPlaintext
   * @param initializationVector initializationVector
   */
  def init(dataKeyPlaintext: String, algorithmMode: AlgorithmMode, keyLength: Int, initializationVector: Array[Byte]): Unit = {
    init(DECRYPT, algorithmMode, keyLength, dataKeyPlaintext, initializationVector)
  }

  /**
   * read header from given inputstream
   *
   * @param in InputStream
   * @return header
   */
  def getHeader(in: InputStream): (String, Array[Byte]) = {
    val headerBytes = read(in, HEAD_LENGTH)
    val headerBuffer = ByteBuffer.wrap(headerBytes)
    val encryptedDataKeyBytesLength = headerBuffer.getInt
    val ivLength = headerBuffer.getInt
    val ivStartPos = FIX_LENGTH + encryptedDataKeyBytesLength
    val encryptedDataKeyBytes: Array[Byte] = headerBytes.slice(FIX_LENGTH, ivStartPos)
    val iv: Array[Byte] = headerBytes.slice(ivStartPos, ivStartPos + ivLength)
    val encryptedDataKeyStr = new String(encryptedDataKeyBytes, StandardCharsets.UTF_8)
    (encryptedDataKeyStr, iv)
  }

  /**
   * read given length content from given inputstream
   *
   * @param stream InputStream
   * @param length length
   * @return content
   */
  protected def read(stream: InputStream, length: Int): Array[Byte] = {
    val retval = new Array[Byte](length)
    val bytesRead: Int = stream.read(retval)
    invalidOperationError(bytesRead != length,
      s"Not enough bits to read!, excepted $length, but got $bytesRead." )
    retval
  }

  /**
   * partially decrypt the content
   *
   * @param in     InputStream
   * @param buffer array which stores the content from in
   * @return decrypted content
   */
  def decrypt(in: InputStream, buffer: Array[Byte]): Array[Byte] = {
    if (in.available() == 0) {
      return new Array[Byte](0)
    }
    val readLen = in.read(buffer)
    val availableLength = in.available()
    if (availableLength > hmacSize) {
      return update(buffer, 0, readLen)
    }
    val last = new Array[Byte](availableLength)
    if (availableLength > 0) {
      in.read(last)
    }
    val hmacStartPos = readLen - hmacSize + availableLength
    val expectedHmac = buffer.slice(hmacStartPos, readLen) ++ last
    val (lastPart, realHmac) = doFinal(buffer, 0, hmacStartPos)
    LogError.invalidArgumentError("hmac not match", expectedHmac.sameElements(realHmac))
    lastPart
  }

  /**
   * reset the decryptor
   */
  def reset(): Unit = {
    cipher = null
    initializationVector = null
    algorithmParameterSpec = null
    encryptionKeySpec = null
    cipher = null
    mac = null
    cryptoMode = null
  }

}

object ShieldDecryptor {
  /**
   * Create encrypter by type string
   */
  def apply(conf: Configuration): ShieldDecryptor = {
    new ShieldDecryptor(conf)
  }
}


