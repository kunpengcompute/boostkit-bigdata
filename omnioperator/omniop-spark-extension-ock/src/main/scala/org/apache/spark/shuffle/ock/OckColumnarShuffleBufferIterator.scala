/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.shuffle.ock

import com.huawei.ock.spark.jni.OckShuffleJniReader
import com.huawei.ock.ucache.shuffle.NativeShuffle
import com.huawei.ock.ucache.shuffle.datatype.{FetchError, FetchResult, MapTasksInfo}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.shuffle.ock.OckColumnarShuffleBufferIterator.getAndIncReaderSequence
import org.apache.spark.util.{OCKConf, OCKException}

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class OckColumnarShuffleBufferIterator[T](
                                ockConf: OCKConf,
                                appId: String,
                                shuffleId: Int,
                                readMetrics: ShuffleReadMetricsReporter,
                                startMapIndex: Int,
                                endMapIndex: Int,
                                startPartition: Int,
                                endPartition: Int,
                                numBuffers: Int,
                                bufferSize: Long,
                                mapTaskToHostInfo: MapTasksInfo,
                                typeIds: Array[Int],
                                context: TaskContext)
  extends Iterator[OckShuffleJniReader] with Logging {

    private var totalFetchNum = 0L
    private var blobMap: Map[Long, OckShuffleJniReader] = Map()

    private var usedBlobId = -1L
    final private val FETCH_ERROR = -1L;
    final private val FETCH_FINISH = 0L;

    private val taskContext = context
    private val sequenceId: String = "Spark_%s_%d_%d_%d_%d_%d_%d".format(appId, shuffleId, startMapIndex,
        endMapIndex, startPartition, endPartition, getAndIncReaderSequence())
    private var hasBlob: Boolean = false;

    initialize()

    private[this] def destroyMapTaskInfo(): Unit = {
        if (mapTaskToHostInfo.getNativeObjHandle != 0) {
            NativeShuffle.destroyMapTaskInfo(mapTaskToHostInfo.getNativeObjHandle)
            mapTaskToHostInfo.setNativeObjHandle(0)
        }
        blobMap.values.foreach(reader => {
            reader.doClose()
        })
    }

    private[this] def throwFetchException(fetchError: FetchError): Unit = {
        NativeShuffle.shuffleStreamReadStop(sequenceId)
        destroyMapTaskInfo()
        if (fetchError.getExecutorId() > 0) {
            logError("Fetch failed error occurred, mostly because ockd is killed in some stage, node id is: "
              + fetchError.getNodeId + " executor id is: " + fetchError.getExecutorId() + " sequenceId is " + sequenceId)
            NativeShuffle.markShuffleWorkerRemoved(appId,  fetchError.getNodeId.toInt)
            val blocksByAddress = OckColumnarShuffleBlockResolver.getMapSizes(shuffleId, startMapIndex, endMapIndex,
                startPartition, endPartition)
            OCKException.ThrowFetchFailed(appId, shuffleId, fetchError, blocksByAddress, taskContext)
        }

        val errorMessage = "Other error occurred, mostly because mf copy is failed in some stage, copy from node: "
            + fetchError.getNodeId + " sequenceId is " + sequenceId
        OCKException.ThrowOckException(errorMessage)
    }

    private[this] def initialize(): Unit = {
        // if reduce task fetch data is empty, will construct empty iterator
        if (mapTaskToHostInfo.recordNum() > 0) {
            val ret = NativeShuffle.shuffleStreamReadSizesGet(sequenceId, shuffleId, context.stageId(),
                context.stageAttemptNumber(), startMapIndex, endMapIndex, startPartition, endPartition, mapTaskToHostInfo)
            if (ret == FETCH_ERROR) {
                throwFetchException(NativeShuffle.shuffleStreamReaderGetError(sequenceId))
            }
            totalFetchNum = ret
        }

        // create buffers, or blobIds
        // use bagName, numBuffers and bufferSize to create buffers in low level
        if (totalFetchNum != 0) {
            NativeShuffle.shuffleStreamReadStart(sequenceId)
            hasBlob = true
        }

        logDebug("Initialize OCKColumnarShuffleBufferIterator sequenceId " + sequenceId + " blobNum " + totalFetchNum)
    }

    override def hasNext: Boolean = {
        if (!hasBlob && totalFetchNum != 0) {
            val dataSize: Int = NativeShuffle.shuffleStreamReadStop(sequenceId)
            if (OckColumnarShuffleManager.isCompress(ockConf.sparkConf) && dataSize > 0) {
                readMetrics.incRemoteBytesRead(dataSize)
            }
            destroyMapTaskInfo()
        }

        hasBlob
    }

    override def next(): OckShuffleJniReader = {
        logDebug(s"new next called, need to release last buffer and call next buffer")
        if (usedBlobId != -1L) {
            NativeShuffle.shuffleStreamReadGatherFlush(sequenceId, usedBlobId)
        }
        val startFetchWait = System.nanoTime()
        val result: FetchResult = NativeShuffle.shuffleStreamReadGatherOneBlob(sequenceId)
        val fetchWaitTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startFetchWait)
        readMetrics.incFetchWaitTime(fetchWaitTime)

        if (result.getRet == FETCH_ERROR) {
            throwFetchException(result.getError)
        } else if (result.getRet == FETCH_FINISH) {
            hasBlob = false
        }

        usedBlobId = result.getBlobId
        logDebug("Get info blobId " + result.getBlobId + " blobSize " + result.getDataSize + ", sequenceId "
          + sequenceId + " getRet " + result.getRet)
        if (result.getDataSize > 0) {
            if (!OckColumnarShuffleManager.isCompress(ockConf.sparkConf)) {
                readMetrics.incRemoteBytesRead(result.getDataSize)
            }
            if (blobMap.contains(result.getBlobId)) {
                val record = blobMap(result.getBlobId)
                record.upgradeValueLen(result.getDataSize)
                record
            } else {
                val record = new OckShuffleJniReader(result.getBlobId, result.getCapacity.toInt,
                    result.getAddress, result.getDataSize, typeIds)
                blobMap += (result.getBlobId -> record)
                record
            }
        } else {
            val errorMessage = "Get buffer capacity to read is zero, sequenceId is " + sequenceId
            OCKException.ThrowOckException(errorMessage)
            new OckShuffleJniReader(result.getBlobId, 0, result.getAddress, result.getDataSize, typeIds)
        }
    }
}

private object OckColumnarShuffleBufferIterator {
    var gReaderSequence : AtomicInteger = new AtomicInteger(0)

    def getAndIncReaderSequence(): Int = {
        gReaderSequence.getAndIncrement()
    }
}