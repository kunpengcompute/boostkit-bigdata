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
import org.scalatest.FlatSpec

class CryptoSpec extends FlatSpec {

  "parse test" should "work" in {
    val gcm = "AeS/GCm/nopadding"
    val gcmMode = AlgorithmMode.parse(gcm)
    assert(gcmMode.equals(AES_GCM_NOPADDING),
      s"\n$gcm should create a case object AES_GCM_NOPADDING")
  }

  "apply test" should "work" in {
    val conf: Configuration = new Configuration()
    assert(Crypto(AES_GCM_NOPADDING, conf).isInstanceOf[ShieldCrypto],
      s"\n AES_GCM_NOPADDING should be supported to create ShieldEncrypt")
    assertThrows [IllegalArgumentException] {
      Crypto(PLAIN_TEXT, conf)
    }
  }
}
