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

package com.huawei.analytics.shield.ra

import scala.collection.mutable

abstract class RemoteAttestationAgent {

  def doRemoteAttestation(executorHostName: String, executorId: String): Boolean

  def requestSecret(executorHostName: String, executorId: String, secretId: String): Unit

  def getSecret(key: String): String = {
    SecretMap.get(key)
  }

  def putSecret(key: String, value: String): Unit = {
    SecretMap.put(key, value)
  }
}

private object SecretMap {
  private val keyMap: mutable.HashMap[String, String] = mutable.HashMap[String, String]()

  def getKeyMap: mutable.HashMap[String, String] = {
    keyMap
  }

  def put(key: String, value: String): Unit = {
    keyMap.put(key, value)
  }

  def get(key: String): String = {
    if (keyMap.contains(key)) {
      keyMap(key)
    } else {
      null
    }
  }
}