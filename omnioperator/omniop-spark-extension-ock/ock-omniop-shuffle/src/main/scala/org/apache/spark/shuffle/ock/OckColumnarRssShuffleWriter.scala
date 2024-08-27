/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package org.apache.spark.shuffle.ock

import com.huawei.boostkit.spark.util.OmniAdaptorUtil.transColBatchToOmniVecs
import com.huawei.boostkit.spark.vectorized.SplitResult
import com.huawei.ock.spark.jni.{OckShuffleJniWriter, RssWriterId}
import com.huawei.ock.ucache.shuffle.NativeShuffle
import com.huawei.ock.ucache.shuffle.datatype.{RSSMapTaskInfo, RSSNodeInfo}
import nova.hetu.omniruntime.vector.VecBatch
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{OCKConf, OCKRemoteFunctions}
import org.apache.spark.{SparkEnv, TaskContext}

import scala.collection.mutable

class OckColumnarRssShuffleWriter[K, V](
    applicationId: String,
    ockConf: OCKConf,
    handle: BaseShuffleHandle[K, V, V],
    mapId: Long,
    context: TaskContext,
    writeMetrics: ShuffleWriteMetricsReporter,
    rssNodeInfo: RSSNodeInfo)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency.asInstanceOf[ColumnarShuffleDependency[K, V, V]]
  private val blockManager = SparkEnv.get.blockManager
  private val maxSingleRegionSize: Int = 2 * 1024 * 1024
  private var stopping = false

  private var mapStatus: MapStatus = _
  private var splitResult: SplitResult = _
  private val shuffleId = dep.shuffleId
  private val numPartitions = dep.partitioner.numPartitions
  private var partitionLengths: Array[Long] = _
  private val originMapId: Int = context.partitionId()

  private val taskName: String = "%s_%s_%d".format(applicationId, shuffleId, context.taskAttemptId())
  private var rssMapTaskInfo: RSSMapTaskInfo = _
  private val executorId = blockManager.blockManagerId.executorId
  private var nativeSplitter: Long = 0
  private var nativeWriterId: RssWriterId = new RssWriterId()
  val maxCapacityTotal: Int = ockConf.maxCapacityTotal
  val minCapacityTotal: Int = ockConf.minCapacityTotal
  val cap: Int = {
    val capacity = Math.min(Math.max(maxSingleRegionSize * numPartitions, minCapacityTotal), maxCapacityTotal)
    val regionCapacity = Math.min(Math.floorDiv(capacity, numPartitions), maxSingleRegionSize)
    regionCapacity
  }
  val enableShuffleCompress: Boolean = OckColumnarShuffleManager.isCompress(ockConf.sparkConf)
  private val jniWriter = new OckShuffleJniWriter()
  private var splitTime: Long = 0L
  private val shuffleIds: mutable.Set[Int] = mutable.Set()
  var isCompress: Boolean = OCKRemoteShuffleManager.isCompress(ockConf.sparkConf)

  def setLBStrategy(shuffleId: Int, rssNodeInfo: RSSNodeInfo): Unit = {
    if (!shuffleIds.contains(shuffleId)) {
      synchronized(if (!shuffleIds.contains(shuffleId)) {
        NativeShuffle.setShuffleLBStrategy(shuffleId, rssNodeInfo)
        shuffleIds.add(shuffleId)
      })
    }
  }

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    if (!records.hasNext) {
      partitionLengths = new Array[Long](dep.partitioner.numPartitions)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
      return
    }

    logDebug(s"Begin to write in shuffle. Task info: shuffleId ${handle.shuffleId}, mapID $mapId"
      + " Task id: " + context.taskAttemptId() + " host name: " + blockManager.blockManagerId.host
      + " stage attempt: " + context.stageAttemptNumber)

    setLBStrategy(shuffleId, rssNodeInfo)

    rssMapTaskInfo = new RSSMapTaskInfo(taskName, shuffleId, context.stageId(), context.stageAttemptNumber(),
      numPartitions, context.taskAttemptId().toInt, originMapId, executorId.toInt)

    if (nativeSplitter == 0) {
      nativeSplitter = jniWriter.make(
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
        enableShuffleCompress,
        true,
        nativeWriterId)
    }
    if (nativeWriterId.getId == -1) {
      logError(s"Get native rss writer failed.")
      return
    }
    NativeShuffle.rssRefreshWriter(nativeWriterId.getId, rssMapTaskInfo)

    while (records.hasNext) {
      val cb = records.next()._2.asInstanceOf[ColumnarBatch]
      if (cb.numRows == 0 || cb.numCols == 0) {
        logInfo(s"Skip ColumnarBatch of ${cb.numRows} rows, ${cb.numCols} cols")
        System.out.println("Skip column")
      } else {
        val input = transColBatchToOmniVecs(cb)
        val endTime = System.currentTimeMillis()
        for( col <- 0 until cb.numCols()) {
          dep.dataSize += input(col).getRealValueBufCapacityInBytes
          dep.dataSize += input(col).getRealNullBufCapacityInBytes
          dep.dataSize += input(col).getRealOffsetBufCapacityInBytes
        }
        val vb = new VecBatch(input, cb.numRows())
        jniWriter.split(nativeSplitter, vb.getNativeVectorBatch)
        vb.close()
        splitTime += System.currentTimeMillis() - endTime
        dep.numInputRows.add(cb.numRows)
        writeMetrics.incRecordsWritten(cb.numRows)
      }
    }
    splitResult = jniWriter.stop(nativeSplitter)

    dep.splitTime.add(splitTime)
    dep.spillTime.add(splitResult.getTotalSpillTime)
    dep.bytesSpilled.add(splitResult.getTotalBytesSpilled)
    writeMetrics.incBytesWritten(splitResult.getTotalBytesWritten)
    writeMetrics.incWriteTime(splitResult.getTotalWriteTime)

    partitionLengths = splitResult.getPartitionLengths

    val blockManagerId = BlockManagerId.apply(executorId,
      blockManager.blockManagerId.host,
      blockManager.blockManagerId.port,
      Option.apply(OCKRemoteFunctions.getNodeId + "#" + context.taskAttemptId()))
    mapStatus = MapStatus(blockManagerId, partitionLengths, mapId)

    logDebug(s"Begin to update the entry meta in shuffle write. Task info: shuffleId ${handle.shuffleId}, mapID $mapId")
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        logInfo("Unexpected branch.")
        return None
      }
      stopping = true
      if (success) {
        logDebug(s"Success to shuffle writer. Begin to clear writer. " +
          s"Task info: shuffleId ${handle.shuffleId} mapId $mapId originMapId $originMapId")

        if (nativeSplitter != 0 && nativeWriterId.getId >= 0) {
          NativeShuffle.updateTaskInfo(nativeWriterId.getId)
          NativeShuffle.rssDestroyNativeWriter(nativeWriterId.getId)
        } else {
          NativeShuffle.reportTaskNoData(shuffleId, context.stageId(), originMapId, context.taskAttemptId().toInt)
        }

        Some(mapStatus)
      } else {
        logInfo("map task " + context.taskAttemptId() + " lost.")
        None
      }
    } finally {
      if (nativeSplitter != 0) {
        jniWriter.close(nativeSplitter)
        nativeSplitter = 0
      }
    }
  }

  override def getPartitionLengths(): Array[Long] = {
    partitionLengths
  }
}