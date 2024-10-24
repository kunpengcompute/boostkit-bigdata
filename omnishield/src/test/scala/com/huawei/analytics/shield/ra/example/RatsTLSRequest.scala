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

package com.huawei.analytics.shield.ra.example

class RatsTLSRequest extends Serializable {
  var executorName: String = _
  var executorId: String = _

  var ratsTlsServerIp: String = _
  var ratsTlsServerPort: String = _

  def setExecutorInfo(executorName: String, executorId: String): Unit = {
    this.executorName = executorName
    this.executorId = executorId
  }

  def setupRatsTlsServerInfo(ip: String, port: String): Unit = {
    this.ratsTlsServerIp = ip
    this.ratsTlsServerPort = port
  }
}