/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.ock.spark.serialize

import com.huawei.ock.spark.jni.OckShuffleJniReader
import nova.hetu.omniruntime.`type`.DataType
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer
import scala.reflect.ClassTag

class OckColumnarBatchSerializer(readBatchNumRows: SQLMetric, numOutputRows: SQLMetric)
  extends Serializer with Serializable {

  /** Creates a new [[SerializerInstance]]. */
  override def newInstance(): SerializerInstance =
    new OckColumnarBatchSerializerInstance(readBatchNumRows, numOutputRows)
}

class OckColumnarBatchSerializerInstance(
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric)
  extends SerializerInstance with Logging {

  override def deserializeStream(in: InputStream): DeserializationStream = {
    // This method is never called by shuffle code.
    throw new UnsupportedOperationException
  }

  def deserializeReader(reader: OckShuffleJniReader,
                        vectorTypes: Array[DataType],
                        maxLength: Int,
                        maxRowNum: Int): DeserializationStream = {
    new DeserializationStream {
      val serializer = new OckShuffleDataSerializer(reader, vectorTypes, maxLength, maxRowNum)

      private var numBatchesTotal: Long = _
      private var numRowsTotal: Long = _

      override def asKeyValueIterator: Iterator[(Int, ColumnarBatch)] = {
        new Iterator[(Int, ColumnarBatch)] {
          override def hasNext: Boolean = !serializer.isFinish()

          override def next(): (Int, ColumnarBatch) = {
            val columnarBatch: ColumnarBatch = serializer.deserialize()
            // todo check need count?
            numBatchesTotal += 1
            numRowsTotal += columnarBatch.numRows()
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
        val columnarBatch: ColumnarBatch = serializer.deserialize()
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