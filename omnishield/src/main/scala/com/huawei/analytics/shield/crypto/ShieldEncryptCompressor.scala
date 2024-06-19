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

import com.huawei.analytics.shield.utils.LogError.invalidOperationError

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.Compressor

/**
 * shield encrypt compressor
 *
 * @since 2024/5/15
 */
class ShieldEncryptCompressor(conf: Configuration)
  extends Compressor {
  val shieldEncryptor: ShieldEncryptor = ShieldEncryptor(conf)
  var hasHeader = false
  shieldEncryptor.init(conf)
  var isFinished = false
  var tryFinished = false
  var buf: Array[Byte] = null
  var offset = 0
  var length = 0
  private var bytesRead = 0L
  private var bytesWritten = 0L

  override def setInput(b: Array[Byte], off: Int, len: Int): Unit = {
    this.buf = b
    this.offset = off
    this.length = len
  }

  override def needsInput(): Boolean = {
    length <= 0
  }

  override def setDictionary(b: Array[Byte], off: Int, len: Int): Unit = {
    invalidOperationError(condition = true, "Unsupported operation.")
  }

  override def getBytesRead: Long = {
    bytesRead
  }

  override def getBytesWritten: Long = {
    bytesWritten
  }

  override def finish(): Unit = {
    tryFinished = true
  }

  override def finished(): Boolean = {
    isFinished
  }

  override def compress(b: Array[Byte], off: Int, len: Int): Int = {
    // lazy encrypt, in order to doFinal in the right time.
    if (tryFinished) {
      val (cipherResult, hmac) = shieldEncryptor.doFinal(this.buf, this.offset, this.length)
      isFinished = true
      cipherResult.copyToArray(b, 0)
      hmac.copyToArray(b, cipherResult.length)
      cipherResult.length + hmac.length
    } else {
      val header = if (!hasHeader) {
        hasHeader = true
        shieldEncryptor.generateHeader()
      } else {
        null
      }
      var compressedPart = shieldEncryptor.update(this.buf, this.offset, this.length)
      bytesRead += this.length
      this.length = 0
      if (header != null) {
        compressedPart = header ++ compressedPart
      }
      compressedPart.copyToArray(b, 0)
      bytesWritten += compressedPart.length
      compressedPart.length
    }
  }

  override def reset(): Unit = {
    tryFinished = false
    isFinished = false
    buf = null
    offset = 0
    length = 0
    hasHeader = false
    bytesRead = 0L
    bytesWritten = 0L
  }

  override def end(): Unit = {
    invalidOperationError(condition = true,"Unsupported operation end.")
  }

  override def reinit(conf: Configuration): Unit = {
    reset()
  }
}

object ShieldEncryptCompressor {

  def getCompressorType: Class[_ <: Compressor] = {
    classOf[ShieldEncryptCompressor]
  }
}


