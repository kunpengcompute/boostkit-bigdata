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

import com.huawei.analytics.shield.crypto.helper.EncryptHelper

import java.io.ByteArrayInputStream

class ShieldEncryptCompressorSpec extends EncryptHelper {

  private val buffer: Array[Byte] = new Array[Byte](65535)
  private val compressedData: Array[Byte] = new Array[Byte](65535)
  private var compressLen: Int = 0
  val outputData: Array[Byte] = new Array[Byte](inputDataLength)

  "data compression and decompression test" should "work" in {

    // create compressor
    val compressor = new ShieldEncryptCompressor(conf)

    // compress header
    assert(compressor.needsInput(),
      s"Before setInput, needsInput needs to return true")
    compressor.setInput(inputData, 0, inputDataLength / 2)
    assert(!compressor.needsInput(),
      s"After setInput, needsInput needs to return false")
    var len = compressor.compress(buffer, 0, buffer.length)
    assert(compressor.getBytesWritten == len,
      "After header compression, " +
        "the length of the compressed header must be the same as the bytesWritten of the compressor.")
    buffer.copyToArray(compressedData, 0, len)
    compressLen += len

    // compress inputData form 0 to inputDataLength / 2
    compressor.setInput(inputData, inputDataLength / 2, inputDataLength / 2)
    len = compressor.compress(buffer, 0, buffer.length)
    Array.copy(buffer, 0, compressedData,compressLen, len)
    compressLen += len

    // compress inputData form inputDataLength / 2 to inputDataLength
    compressor.finish()
    len = compressor.compress(buffer, 0, buffer.length)
    assert(compressor.getBytesRead == inputDataLength,
      "After inputData compression, " +
        "The length of the inputData must be the same as the bytesRead of the compressor.")
    assert(compressor.finished(), "After inputData compression, The compressor should be in the finished state.")
    Array.copy(buffer, 0, compressedData,compressLen, len)
    compressLen += len

    // reinit compressor
    compressor.reinit(conf)
    assert(!compressor.finished(), "After reinit compressor, The compressor should not be in the finished state.")

    // Convert the compressed data into inputStream
    val inputStream = new ByteArrayInputStream(compressedData, 0, compressLen)

    // create DecompressStream
    val cryptoCodec: CryptoCodec = new CryptoCodec()
    cryptoCodec.setConf(conf)
    val cryptoDecompressStream = cryptoCodec.createInputStream(inputStream).asInstanceOf[ShieldCryptoDecompressStream]

    // decompress
    len = cryptoDecompressStream.decompress(buffer, 0, buffer.length)
    buffer.copyToArray(outputData, 0, len)

    // Reset and close the stream
    cryptoDecompressStream.resetState()
    cryptoDecompressStream.close()

    // result comparison
    assert(outputData.sameElements(inputData),
      "The output data after the compression and decompression of the input data must be the same as the input data.")
  }
}
