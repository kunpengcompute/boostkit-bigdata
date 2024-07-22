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
import com.huawei.analytics.shield.kms.common.FileMetaHandler

import org.apache.hadoop.conf.Configuration

import java.security.SecureRandom
import scala.collection.mutable

class SimpleKeyManagementService protected() extends KeyManagementService{
  val enrollMap = new mutable.HashMap[String, String]
  val keyReaderWriter = new FileMetaHandler
  var _appId: String = _
  var _apiKey: String = _
  var keyLength = 48

  override def setDataKeyLength(length: Int): Unit = {
    keyLength = length/8
  }

  override def getEncryptDataKey(primaryKeyName: String, config: Configuration = null): Array[Byte] = {
      val randVect = (1 to keyLength).map { x => new SecureRandom().nextInt(10) }
      val dataKeyPlaintext = randVect.mkString
      var dataKeyCiphertext = ""
      for (i <- 0 until keyLength) {
        dataKeyCiphertext += '0' + ((primaryKeyName(i) - '0') +
          (dataKeyPlaintext(i) - '0')) % 10
      }
      dataKeyCiphertext.getBytes
  }

  override def getDataKeyPlainText(primaryKeyName: String,
                                   config: Configuration = null,
                                   encryptedDataKey: Array[Byte]): Array[Byte] = {
      var dataKeyPlaintext = ""
      for (i <- 0 until keyLength) {
        dataKeyPlaintext += '0' + ((encryptedDataKey(i) - '0') -
          (primaryKeyName(i) - '0') + 10) % 10
      }
      dataKeyPlaintext.getBytes()
  }
}