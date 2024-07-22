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

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.compress._

import java.io.{IOException, InputStream, OutputStream}

/**
 * crypto codec, the main class called by spark
 *
 * @since 2024/5/15
 */
class CryptoCodec
  extends Configurable with CompressionCodec with DirectDecompressionCodec {
  protected var conf: Configuration = null

  val DEFAULT_BUFFER_SIZE: Int = 4 * 1024

  override def setConf(conf: Configuration): Unit = {
    this.conf = conf
  }

  override def getConf: Configuration = conf

  @throws[IOException]
  override def createOutputStream(out: OutputStream): CompressionOutputStream = {
    val compressor = this.createCompressor()
    this.createOutputStream(out, compressor)
  }

  @throws[IOException]
  override def createOutputStream(out: OutputStream, compressor: Compressor): CompressorStream = {
    new CompressorStream(out, compressor, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE))
  }

  override def getCompressorType: Class[_ <: Compressor] = {
    ShieldEncryptCompressor.getCompressorType
  }

  override def createCompressor: Compressor = {
    new ShieldEncryptCompressor(conf)
  }

  @throws[IOException]
  override def createInputStream(in: InputStream): CompressionInputStream = {
    ShieldCryptoDecompressStream(conf, in)
  }

  @throws[IOException]
  override def createInputStream(
                                  in: InputStream,
                                  decompressor: Decompressor): ShieldCryptoDecompressStream = {
    ShieldCryptoDecompressStream(conf, in)
  }

  override def getDecompressorType: Class[_ <: Decompressor] = {
    null
  }

  override def createDecompressor: Decompressor = {
    null
  }

  /**
   * {@inheritDoc }
   */
  override def createDirectDecompressor: DirectDecompressor = {
    null
  }

  override def getDefaultExtension: String = {
    ".gcm"
  }
}
