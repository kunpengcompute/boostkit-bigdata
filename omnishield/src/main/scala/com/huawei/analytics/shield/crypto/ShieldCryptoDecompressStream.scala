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

import com.huawei.analytics.shield.kms.common.KeyOperator

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.DecompressorStream

import java.io.InputStream

/**
 * ShieldCryptoDecompressStream for decryption
 *
 * @since 2024/5/15
 */
class ShieldCryptoDecompressStream(in: InputStream,
                                   conf: Configuration) extends DecompressorStream(in) {
  val DEFAULT_BUFFER_SIZE: Int = 4 * 1024
  buffer = new Array[Byte](conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE))
  val shieldDecryptor: ShieldDecryptor = ShieldDecryptor(conf)
  var isDecryptorInit = false

  override def decompress(b: Array[Byte], off: Int, len: Int): Int = {
    // just return when reach the end of file
    if (in.available() == 0) {
      eof = true
      return -1
    }
    // init the decryptor in the first time
    if (!isDecryptorInit) {
      val (encryptedDataKey, initializationVector) = shieldDecryptor.getHeader(in)
      val keyOperator = new KeyOperator
      keyOperator.init(encryptedDataKey, conf)
      val dataKeyPlainStr = keyOperator.encoderWithBase64(keyOperator.getDataKeyPlainText(encryptedDataKey))
      shieldDecryptor.init(dataKeyPlainStr, keyOperator.getAlgorithmMode,
        keyOperator.getDataKeyLength, initializationVector)
      isDecryptorInit = true
    }
    val decompressBytes = shieldDecryptor.decrypt(in, buffer)
    decompressBytes.copyToArray(b, 0)
    decompressBytes.length
  }

  override def resetState(): Unit = {
    shieldDecryptor.reset()
    isDecryptorInit = false
  }
}

object ShieldCryptoDecompressStream {
  def apply(conf: Configuration, in: InputStream): ShieldCryptoDecompressStream = {
    new ShieldCryptoDecompressStream(in, conf)
  }
}