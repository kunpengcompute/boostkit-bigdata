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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import javax.crypto.spec.{GCMParameterSpec, IvParameterSpec}

/**
 * shield encryptor
 *
 * @since 2024/5/15
 */
class ShieldEncryptor(conf: Configuration) extends ShieldCrypto(conf: Configuration) {
  protected var encryptedDataKey: String = ""

  /**
   * init a encryptor
   *
   * @param conf conf
   */
  def init(conf: Configuration): Unit = {
    encryptedDataKey = conf.get("shield.write.dataKey.cipherText")
    init(ENCRYPT, conf.get("shield.write.dataKey.plainText"), conf)
  }

  /**
   * If encrypt data, should generate header and put return value to the head.
   *
   * @return header bytes
   */
  def generateHeader(): Array[Byte] = {
    invalidOperationError(cipher == null, s"you should init corresponding ShieldCrypto first.")
    val encryptedDataKeyBytes = encryptedDataKey.getBytes(StandardCharsets.UTF_8)
    val headBuffer = ByteBuffer.allocate(HEAD_LENGTH)
    headBuffer.putInt(encryptedDataKeyBytes.length)
    val iv = algorithmParameterSpec match {
      case ivParameterSpec: IvParameterSpec =>
        ivParameterSpec.getIV
      case gcmParameterSpec: GCMParameterSpec =>
        gcmParameterSpec.getIV
    }
    headBuffer.putInt(iv.length)
    headBuffer.put(encryptedDataKeyBytes)
    headBuffer.put(iv)
    val suffixLength = (HEAD_LENGTH - FIX_LENGTH - encryptedDataKeyBytes.length - iv.length).max(0)
    val suffix: Array[Byte] = Array.fill(suffixLength)(0)
    headBuffer.put(suffix)
    headBuffer.array()
  }
}

object ShieldEncryptor {

  /**
   * Create encrypter by type string
   */
  def apply(conf: Configuration): ShieldEncryptor = {
    new ShieldEncryptor(conf)
  }
}
