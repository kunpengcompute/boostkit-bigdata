/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.shuffle.ock

import com.huawei.ock.spark.jni.OckShuffleJniReader
import org.apache.spark._
import org.apache.spark.executor.TempShuffleReadMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle.{FetchFailedException, ShuffleBlockResolver}
import org.apache.spark.storage.{BlockId, BlockManagerId}
import org.apache.spark.util.{OCKConf, OCKFunctions}

class OckColumnarShuffleBlockResolver(conf: SparkConf, ockConf: OCKConf)
  extends ShuffleBlockResolver with Logging {

  override def getBlockData(blockId: BlockId, dirs: Option[Array[String]]): ManagedBuffer = {
    null
  }

  /**
   * Remove shuffle temp memory data that contain the output data from one map.
   */
  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
  }

  override def stop(): Unit = {}
}

object OckColumnarShuffleBlockResolver extends Logging {
  def getShuffleData[T](ockConf: OCKConf,
                        appId: String,
                        shuffleId: Int,
                        readMetrics: TempShuffleReadMetrics,
                        startMapIndex: Int,
                        endMapIndex: Int,
                        startPartition: Int,
                        endPartition: Int,
                        numBuffers: Int,
                        bufferSize: Long,
                        typeIds: Array[Int],
                        context: TaskContext): Iterator[OckShuffleJniReader] = {
    val blocksByAddresses = getMapSizes(shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)

    new OckColumnarShuffleBufferIterator(ockConf, appId, shuffleId, readMetrics, startMapIndex, endMapIndex, startPartition, endPartition, numBuffers, bufferSize,
      OCKFunctions.parseBlocksByHost(blocksByAddresses), typeIds, context)
  }

  def CreateFetchFailedException(
                                  address: BlockManagerId,
                                  shuffleId: Int,
                                  mapId: Long,
                                  mapIndex: Int,
                                  reduceId: Int,
                                  message: String
                                ): FetchFailedException = {
    new FetchFailedException(address, shuffleId, mapId, mapIndex, reduceId, message)
  }

  def getMapSizes(
                   shuffleId: Int,
                   startMapIndex: Int,
                   endMapIndex: Int,
                   startPartition: Int,
                   endPartition: Int
                 ): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] = {
    val mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker
    mapOutputTracker.getMapSizesByExecutorId(shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
  }
}