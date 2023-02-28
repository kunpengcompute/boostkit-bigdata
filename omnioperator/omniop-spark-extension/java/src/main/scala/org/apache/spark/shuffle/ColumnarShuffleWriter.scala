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
package org.apache.spark.shuffle

import com.google.common.annotations.VisibleForTesting
import com.huawei.boostkit.spark.ColumnarPluginConfig
import com.huawei.boostkit.spark.jni.SparkJniWrapper
import com.huawei.boostkit.spark.vectorized.SplitResult
import com.huawei.boostkit.spark.util.OmniAdaptorUtil.transColBatchToOmniVecs
import nova.hetu.omniruntime.vector.VecBatch
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import java.io.IOException

class ColumnarShuffleWriter[K, V](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, V],
    mapId: Long,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency.asInstanceOf[ColumnarShuffleDependency[K, V, V]]

  private val blockManager = SparkEnv.get.blockManager

  private var stopping = false

  private var mapStatus: MapStatus = _

  private val localDirs = blockManager.diskBlockManager.localDirs.mkString(",")

  val columnarConf = ColumnarPluginConfig.getSessionConf
  val shuffleSpillBatchRowNum = columnarConf.columnarShuffleSpillBatchRowNum
  val shuffleSpillMemoryThreshold = columnarConf.columnarShuffleSpillMemoryThreshold
  val shuffleCompressBlockSize = columnarConf.columnarShuffleCompressBlockSize
  val shuffleNativeBufferSize = columnarConf.columnarShuffleNativeBufferSize
  val enableShuffleCompress = columnarConf.enableShuffleCompress
  var shuffleCompressionCodec = columnarConf.columnarShuffleCompressionCodec

  if (!enableShuffleCompress) {
    shuffleCompressionCodec = "uncompressed"
  }

  private val jniWrapper = new SparkJniWrapper()

  private var nativeSplitter: Long = 0

  private var splitResult: SplitResult = _

  private var partitionLengths: Array[Long] = _

  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    if (!records.hasNext) {
      partitionLengths = new Array[Long](dep.partitioner.numPartitions)
      shuffleBlockResolver.writeMetadataFileAndCommit(dep.shuffleId, mapId, partitionLengths, Array[Long](), null)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
      return
    }

    val dataTmp = Utils.tempFileWith(shuffleBlockResolver.getDataFile(dep.shuffleId, mapId))
    if (nativeSplitter == 0) {
      nativeSplitter = jniWrapper.make(
        dep.partitionInfo,
        shuffleNativeBufferSize,
        shuffleCompressionCodec,
        dataTmp.getAbsolutePath,
        blockManager.subDirsPerLocalDir,
        localDirs,
        shuffleCompressBlockSize,
        shuffleSpillBatchRowNum,
        shuffleSpillMemoryThreshold)
    }


    while (records.hasNext) {
      val cb = records.next()._2.asInstanceOf[ColumnarBatch]
      if (cb.numRows == 0 || cb.numCols == 0) {
        logInfo(s"Skip ColumnarBatch of ${cb.numRows} rows, ${cb.numCols} cols")
      } else {
        val startTime = System.nanoTime()
        val input = transColBatchToOmniVecs(cb)
        for (col <- 0 until cb.numCols()) {
          dep.dataSize += input(col).getRealValueBufCapacityInBytes
          dep.dataSize += input(col).getRealNullBufCapacityInBytes
          dep.dataSize += input(col).getRealOffsetBufCapacityInBytes
        }
        val vb = new VecBatch(input, cb.numRows())
        jniWrapper.split(nativeSplitter, vb.getNativeVectorBatch)
        dep.splitTime.add(System.nanoTime() - startTime)
        dep.numInputRows.add(cb.numRows)
        writeMetrics.incRecordsWritten(cb.numRows)
      }
    }
    val startTime = System.nanoTime()
    splitResult = jniWrapper.stop(nativeSplitter)

    dep.splitTime.add(System.nanoTime() - startTime - splitResult.getTotalSpillTime -
      splitResult.getTotalWriteTime - splitResult.getTotalComputePidTime)
    dep.spillTime.add(splitResult.getTotalSpillTime)
    dep.bytesSpilled.add(splitResult.getTotalBytesSpilled)
    writeMetrics.incBytesWritten(splitResult.getTotalBytesWritten)
    writeMetrics.incWriteTime(splitResult.getTotalWriteTime + splitResult.getTotalSpillTime)

    partitionLengths = splitResult.getPartitionLengths
    try {
      shuffleBlockResolver.writeMetadataFileAndCommit(
        dep.shuffleId,
        mapId,
        partitionLengths,
        Array[Long](),
        dataTmp)
    } finally {
      if (dataTmp.exists() && !dataTmp.delete()) {
        logError(s"Error while deleting temp file ${dataTmp.getAbsolutePath}")
      }
    }
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        None
      } else {
        stopping = true
        if (success) {
          Option(mapStatus)
        } else {
          None
        }
      }
    } finally {
      if (nativeSplitter != 0) {
        jniWrapper.close(nativeSplitter)
        nativeSplitter = 0
      }
    }
  }

  @VisibleForTesting
  def getPartitionLengths: Array[Long] = partitionLengths

}