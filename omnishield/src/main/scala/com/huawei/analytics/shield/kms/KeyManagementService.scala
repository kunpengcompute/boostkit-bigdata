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

package com.huawei.analytics.shield.kms

import org.apache.hadoop.conf.Configuration

/**
 * kms trait
 */
trait KeyManagementService {

  /**
   * set dataKey length, 128 bit or 256 bit
   *
   * @param length length
   */
  def setDataKeyLength(length: Int): Unit

  /** This func should generate a {@link length} bit dataKey (plaintext) and
   *  encrypt this dataKey by kms with primaryKey
   *
   * @param primaryKeyName primaryKeyName
   * @param config         config
   * @return EncryptDataKey
   */
  def getEncryptDataKey(primaryKeyName: String, config: Configuration = null): Array[Byte]

  /** This func should decrypt encryptedDataKey by kms with primaryKey and return DataKeyPlainText bytes
   *
   * @param primaryKeyName        primaryKeyName
   * @param config                config
   * @param encryptedDataKeyBytes encryptedDataKey
   * @return DataKeyPlainText
   */
  def getDataKeyPlainText(primaryKeyName: String,
                          config: Configuration = null,
                          encryptedDataKeyBytes: Array[Byte]): Array[Byte]
}