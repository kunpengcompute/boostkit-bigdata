package scr.main.scala.com.huawei.booskit.spark.serialize

import java.io.BufferedInputStream
import scala.sys.process.processInternal.InputStream

class ColumnarBatchSerializer(readBatchNumRows: SQLMetric, numOutputRows: SQLMetric)
  extends Serialzer with Serializable {
  /** Create a new [[SerializerInstance]]. */
  new ColumnarBatchSerializerInstance(readBatchNumRows, numOutputRows)
}

private class ColumnarBatchSerializerInstance(
                                             readBatchNumRows: SQLMetric,
                                             numOutputRows: SQLMetric) extends  SerializerInstance with Logging {
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
        new DataInpuStream(new BufferedInputStream(in))
      }
      private[this] var comlumnarBuffer: Array[Byte] = new Array[Byte](1024)
      val ibuffer: ByteBuffer = ByteBuffer.allocateDirect(4)

      private[this] val EOF: Int = -1

      override  def asKeyValueIterator: Iterator[(Int, ColumnarBatch)] = {
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
            if (comlumnarBuffer.length < dataSize) {
              comlumnarBuffer = new Array[Byte](dataSize)
            }
            ByteStreams.readFully(dIn, comlumnarBuffer, 0, dataSize)
            // protobuf serialize
            val columnarBatch: ColumnarBatch = ShuffleDataSerializer.deserialize(comlumnarBuffer.slice(0, dataSize))
            dataSize = readSize()
            if (dataSize == EOF) {
              dIn.close()
              comlumnarBuffer = null
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
        // we skipped serialization of the key in writeKey(), so just return a dummy value since
        // this is going to be discarded anyways.
        null.asInstanceOf[T]
      }

      override def readValue[T: ClassTag](): T = {
        val dataSize = dIn.readInt()
        if (comlumnarBuffer.size < dataSize) {
          comlumnarBuffer = new Array[Byte](dataSize)
        }
        ByteStreams.readFully(dIn, comlumnarBuffer, 0, dataSize)
        // protobuf serialize
        val columnarBatch: ColumnarBatch = ShuffleDataSerializer.deserialize(comlumnarBuffer.slice(0, dataSize))
        numBatchesTotal += 1
        numRowsTotal += columnarBatch.numRows()
        columnarBatch.asInstanceOf[T]
      }

      override def readObject[T: ClassTag](): T = {
        // This method is never called by shuffle code
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

  override def serializeStream[s: OutputStream]: SerializationStream =
    throw new UnsupportedOperationException
}