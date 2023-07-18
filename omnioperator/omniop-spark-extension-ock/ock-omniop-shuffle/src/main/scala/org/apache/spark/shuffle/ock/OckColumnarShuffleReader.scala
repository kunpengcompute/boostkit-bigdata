/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.shuffle.ock

import com.huawei.boostkit.spark.ColumnarPluginConfig
import com.huawei.boostkit.spark.serialize.ColumnarBatchSerializer
import com.huawei.ock.spark.jni.OckShuffleJniReader
import com.huawei.ock.spark.serialize.{OckColumnarBatchSerializer, OckColumnarBatchSerializerInstance}
import nova.hetu.omniruntime.`type`.{DataType, DataTypeSerializer}
import org.apache.spark._
import org.apache.spark.executor.TempShuffleReadMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.JavaSerializerInstance
import org.apache.spark.shuffle.{BaseShuffleHandle, ColumnarShuffleDependency, ShuffleReader}
import org.apache.spark.sorter.OCKShuffleSorter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.util.{CompletionIterator, OCKConf, Utils}

/**
 * Fetches and reads the partitions in range [startPartition, endPartition) from a shuffle by
 * requesting them from other nodes' block stores.
 */
class OckColumnarShuffleReader[K, C](
                              appId: String,
                              handle: BaseShuffleHandle[K, _, C],
                              startMapIndex: Int,
                              endMapIndex: Int,
                              startPartition: Int,
                              endPartition: Int,
                              context: TaskContext,
                              conf: SparkConf,
                              ockConf: OCKConf,
                              readMetrics: TempShuffleReadMetrics)
  extends ShuffleReader[K, C] with Logging {
  logInfo(s"get OCKShuffleReader mapIndex $startMapIndex - $endMapIndex partition: $startPartition - $endPartition.")

  private val dep = handle.dependency.asInstanceOf[ColumnarShuffleDependency[K, C, C]]

  val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf

  private var recordsSize: Long = 0L
  // some input stream may exist header, must handle for it
  private var isInputStreamExistHeader: Boolean = false

  val shuffleSorterClass: String = ockConf.shuffleSorterClass

  val ockShuffleSorter: OCKShuffleSorter =
    Utils.classForName(shuffleSorterClass).newInstance.asInstanceOf[OCKShuffleSorter]

  val readBatchNumRows = classOf[ColumnarBatchSerializer].getDeclaredField("readBatchNumRows")
  val numOutputRows = classOf[ColumnarBatchSerializer].getDeclaredField("numOutputRows")
  readBatchNumRows.setAccessible(true)
  numOutputRows.setAccessible(true)

  private val serializerInstance = new OckColumnarBatchSerializer(
    readBatchNumRows.get(dep.serializer).asInstanceOf[SQLMetric],
    numOutputRows.get(dep.serializer).asInstanceOf[SQLMetric])
    .newInstance()
    .asInstanceOf[OckColumnarBatchSerializerInstance]

  /**
   * Read the combined key-values for this reduce task
   */
  override def read(): Iterator[Product2[K, C]] = {
    // Update the context task metrics for each record read.
    val vectorTypes: Array[DataType] = DataTypeSerializer.deserialize(dep.partitionInfo.getInputTypes)
    val typeIds: Array[Int] = vectorTypes.map {
      vecType => vecType.getId.ordinal
    }

    val gatherDataStart = System.currentTimeMillis()
    val records: Iterator[OckShuffleJniReader] = OckColumnarShuffleBlockResolver.getShuffleData(ockConf, appId,
      handle.shuffleId, readMetrics, startMapIndex, endMapIndex,
      startPartition, endPartition, 3, 0L, typeIds, context)
    val gatherDataEnd = System.currentTimeMillis()

    var aggregatedIter: Iterator[Product2[K, C]] = null
    var deserializeStart: Long = 0L
    var deserializeEnd: Long = 0L
    var combineBranchEnd: Long = 0L
    var branch: Int = 0

    if (ockConf.useSparkSerializer) {
      deserializeStart = System.currentTimeMillis()
      val readIter = records.flatMap { shuffleJniReader =>
        recordsSize += shuffleJniReader.getValueLen
        serializerInstance.deserializeReader(shuffleJniReader, vectorTypes,
          columnarConf.maxBatchSizeInBytes,
          columnarConf.maxRowCount).asKeyValueIterator
      }

      val recordIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
        readIter.map { record =>
          readMetrics.incRecordsRead(1)
          record
        },
        context.taskMetrics().mergeShuffleReadMetrics())

      // An interruptible iterator must be used here in order to support task cancellation
      val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, recordIter)

      deserializeEnd = System.currentTimeMillis()

      aggregatedIter = if (dep.aggregator.isDefined) {
        if (dep.mapSideCombine && ockConf.isMapSideCombineExt) {
          branch = 1
          // We are reading values that are already combined
          val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
          dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
        } else {
          branch = 2
          val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
          dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
        }
      } else {
        branch = 3
        interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
      }
      combineBranchEnd = System.currentTimeMillis()
    }
    context.taskMetrics().mergeShuffleReadMetrics()

    val result = dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        ockShuffleSorter.sort(context, keyOrd, dep.serializer, records, aggregatedIter)
      case None =>
        aggregatedIter
    }
    val sortEnd = System.currentTimeMillis()

    logInfo("Time cost for shuffle read partitionId: " + startPartition + "; gather data cost " + (gatherDataEnd - gatherDataStart)
      + "ms. data size: " + recordsSize + "Bytes. deserialize cost " + (deserializeEnd - deserializeStart) + "ms. combine branch: "
      + branch + ", cost: " + (combineBranchEnd - deserializeEnd) + "ms. " + "sort: " + (sortEnd - combineBranchEnd) + "ms.")

    new InterruptibleIterator[Product2[K, C]](context, result)
  }
}