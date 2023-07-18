/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.shuffle.ock

import com.huawei.boostkit.spark.util.OmniAdaptorUtil.transColBatchToOmniVecs
import com.huawei.boostkit.spark.vectorized.SplitResult
import com.huawei.ock.spark.jni.OckShuffleJniWriter
import com.huawei.ock.ucache.shuffle.NativeShuffle
import nova.hetu.omniruntime.vector.VecBatch
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{OCKConf, OCKFunctions}
import org.apache.spark.{SparkEnv, TaskContext}

class OckColumnarShuffleWriter[K, V](
    applicationId: String,
    ockConf: OCKConf,
    handle: BaseShuffleHandle[K, V, V],
    mapId: Long,
    context: TaskContext,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency.asInstanceOf[ColumnarShuffleDependency[K, V, V]]

  private val blockManager = SparkEnv.get.blockManager

  private var stopping = false

  private var mapStatus: MapStatus = _

  val enableShuffleCompress: Boolean = OckColumnarShuffleManager.isCompress(ockConf.sparkConf)

  val cap: Int = ockConf.capacity
  val maxCapacityTotal: Int = ockConf.maxCapacityTotal
  val minCapacityTotal: Int = ockConf.minCapacityTotal

  private val jniWritter = new OckShuffleJniWriter()

  private var nativeSplitter: Long = 0

  private var splitResult: SplitResult = _

  private var partitionLengths: Array[Long] = _

  private var first: Boolean = true
  private var readTime: Long = 0L
  private var markTime: Long = 0L
  private var splitTime: Long = 0L
  private var changeTime: Long = 0L
  private var rowNum: Int = 0
  private var vbCnt: Int = 0

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    if (!records.hasNext) {
      partitionLengths = new Array[Long](dep.partitioner.numPartitions)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
      return
    }

    val startMake = System.currentTimeMillis()
    if (nativeSplitter == 0) {
      nativeSplitter = jniWritter.make(
        applicationId,
        dep.shuffleId,
        context.stageId(),
        context.stageAttemptNumber(),
        mapId.toInt,
        context.taskAttemptId(),
        dep.partitionInfo,
        cap,
        maxCapacityTotal,
        minCapacityTotal,
        enableShuffleCompress)
    }
    val makeTime = System.currentTimeMillis() - startMake

    while (records.hasNext) {
      vbCnt += 1
      if (first) {
        readTime = System.currentTimeMillis() - makeTime
        first = false
      } else {
        readTime += (System.currentTimeMillis() - markTime)
      }
      val cb = records.next()._2.asInstanceOf[ColumnarBatch]
      if (cb.numRows == 0 || cb.numCols == 0) {
        logInfo(s"Skip ColumnarBatch of ${cb.numRows} rows, ${cb.numCols} cols")
        System.out.println("Skip column")
        markTime = System.currentTimeMillis()
      } else {
        val startTime = System.currentTimeMillis()
        val input = transColBatchToOmniVecs(cb)
        val endTime = System.currentTimeMillis()
        changeTime += endTime - startTime
        for( col <- 0 until cb.numCols()) {
          dep.dataSize += input(col).getRealValueBufCapacityInBytes
          dep.dataSize += input(col).getRealNullBufCapacityInBytes
          dep.dataSize += input(col).getRealOffsetBufCapacityInBytes
        }
        val vb = new VecBatch(input, cb.numRows())
        if (rowNum == 0) {
          rowNum = cb.numRows()
        }
        jniWritter.split(nativeSplitter, vb.getNativeVectorBatch)
        dep.numInputRows.add(cb.numRows)
        writeMetrics.incRecordsWritten(1)
        markTime = System.currentTimeMillis()
        splitTime += markTime - endTime
      }
    }
    val flushStartTime = System.currentTimeMillis()
    splitResult = jniWritter.stop(nativeSplitter)

    val stopTime = (System.currentTimeMillis() - flushStartTime)
    dep.splitTime.add(splitTime)
    writeMetrics.incBytesWritten(splitResult.getTotalBytesWritten)
    writeMetrics.incWriteTime(splitResult.getTotalWriteTime)

    partitionLengths = splitResult.getPartitionLengths

    val blockManagerId = BlockManagerId.apply(blockManager.blockManagerId.executorId,
      blockManager.blockManagerId.host,
      blockManager.blockManagerId.port,
      Option.apply(OCKFunctions.getNodeId + "#" + context.taskAttemptId()))
    mapStatus = MapStatus(blockManagerId, partitionLengths, mapId)

    System.out.println("shuffle_write_tick makeTime " + makeTime + " readTime " + readTime + " splitTime "
      + splitTime + " changeTime " + changeTime + " stopTime " + stopTime + " rowNum " + dep.numInputRows.value + " vbCnt " + vbCnt)
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        None
      } else {
        stopping = true
        if (success) {
          NativeShuffle.shuffleStageSetShuffleId("Spark_"+applicationId, context.stageId(), handle.shuffleId)
          Option(mapStatus)
        } else {
          None
        }
      }
    } finally {
      if (nativeSplitter != 0) {
        jniWritter.close(nativeSplitter)
        nativeSplitter = 0
      }
    }
  }
}