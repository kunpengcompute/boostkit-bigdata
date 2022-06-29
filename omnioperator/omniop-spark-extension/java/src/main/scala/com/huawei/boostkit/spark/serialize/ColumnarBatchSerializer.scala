/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.spark.serialize

import com.google.common.io.ByteStreams
import com.huawei.boostkit.spark.ColumnarPluginConfig
import com.huawei.boostkit.spark.compress.{CompressionUtil, DecompressionStream}
import java.io.{BufferedInputStream, DataInputStream, EOFException, InputStream, OutputStream}
import java.nio.ByteBuffer
import scala.reflect.ClassTag
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

class ColumnarBatchSerializer(readBatchNumRows: SQLMetric, numOutputRows: SQLMetric)
  extends Serializer
    with Serializable {
  /** Creates a new [[SerializerInstance]]. */
  override def newInstance(): SerializerInstance =
    new ColumnarBatchSerializerInstance(readBatchNumRows, numOutputRows)
}

private class ColumnarBatchSerializerInstance(
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric)
  extends SerializerInstance with Logging {
  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {
      val columnarConf = ColumnarPluginConfig.getSessionConf
      val shuffleCompressBlockSize = columnarConf.columnarShuffleCompressBlockSize
      val enableShuffleCompress = columnarConf.enableShuffleCompress
      var shuffleCompressionCodec = columnarConf.columnarShuffleCompressionCodec

      if (!enableShuffleCompress) {
        shuffleCompressionCodec = "uncompressed"
      }

      private var numBatchesTotal: Long = _
      private var numRowsTotal: Long = _

      private[this] val dIn: DataInputStream = if (enableShuffleCompress) {
        val codec = CompressionUtil.createCodec(shuffleCompressionCodec)
        new DataInputStream(new BufferedInputStream(
          new DecompressionStream(in, codec, shuffleCompressBlockSize)))
      } else {
        new DataInputStream(new BufferedInputStream(in))
      }
      private[this] var columnarBuffer: Array[Byte] = new Array[Byte](1024)
      val ibuffer: ByteBuffer = ByteBuffer.allocateDirect(4)

      private[this] val EOF: Int = -1

      override def asKeyValueIterator: Iterator[(Int, ColumnarBatch)] = {
        new Iterator[(Int, ColumnarBatch)] {
          private[this] def readSize(): Int = try {
            dIn.readInt()
          } catch {
            case e: EOFException =>
              dIn.close()
              EOF
          }

          private[this] var dataSize: Int = readSize()
          override def hasNext: Boolean = dataSize != EOF

          override def next(): (Int, ColumnarBatch) = {
            if (columnarBuffer.length < dataSize) {
              columnarBuffer = new Array[Byte](dataSize)
            }
            ByteStreams.readFully(dIn, columnarBuffer, 0, dataSize)
            // protobuf serialize
            val columnarBatch: ColumnarBatch = ShuffleDataSerializer.deserialize(columnarBuffer.slice(0, dataSize))
            dataSize = readSize()
            if (dataSize == EOF) {
              dIn.close()
              columnarBuffer = null
            }
            (0, columnarBatch)
          }
        }
      }

      override def asIterator: Iterator[Any] = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def readKey[T: ClassTag](): T = {
        // We skipped serialization of the key in writeKey(), so just return a dummy value since
        // this is going to be discarded anyways.
        null.asInstanceOf[T]
      }

      override def readValue[T: ClassTag](): T = {
        val dataSize = dIn.readInt()
        if (columnarBuffer.size < dataSize) {
          columnarBuffer = new Array[Byte](dataSize)
        }
        ByteStreams.readFully(dIn, columnarBuffer, 0, dataSize)
        // protobuf serialize
        val columnarBatch: ColumnarBatch = ShuffleDataSerializer.deserialize(columnarBuffer.slice(0, dataSize))
        numBatchesTotal += 1
        numRowsTotal += columnarBatch.numRows()
        columnarBatch.asInstanceOf[T]
      }

      override def readObject[T: ClassTag](): T = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def close(): Unit = {
        if (numBatchesTotal > 0) {
          readBatchNumRows.set(numRowsTotal.toDouble / numBatchesTotal)
        }
        numOutputRows += numRowsTotal
        dIn.close()
      }
    }
  }

  override def serialize[T: ClassTag](t: T): ByteBuffer =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException

  override def serializeStream(s: OutputStream): SerializationStream =
    throw new UnsupportedOperationException
}