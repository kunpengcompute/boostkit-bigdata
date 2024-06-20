package com.huawei.analytics.shield.utils

import com.huawei.analytics.shield.crypto.{CryptoCodec, PLAIN_TEXT, ShieldEncryptCompressor}
import com.huawei.analytics.shield.kms.common.KmsMetaFormat
import com.huawei.analytics.shield.utils.LogError.invalidOperationError
import org.scalatest.FlatSpec

class LogSpec extends FlatSpec{

  "UT cover" should "work" in {
    PLAIN_TEXT.secretKeyAlgorithm
    PLAIN_TEXT.signingAlgorithm
    PLAIN_TEXT.encryptionAlgorithm
    val format = KmsMetaFormat("a")
    format.toString
    val cc = new CryptoCodec
    cc.getConf
    cc.getCompressorType
    cc.createDirectDecompressor
    val se = ShieldEncryptCompressor
    se.getCompressorType
    assertThrows [InvalidOperationException] {
      invalidOperationError(condition = true, "err op")
    }
  }
}
