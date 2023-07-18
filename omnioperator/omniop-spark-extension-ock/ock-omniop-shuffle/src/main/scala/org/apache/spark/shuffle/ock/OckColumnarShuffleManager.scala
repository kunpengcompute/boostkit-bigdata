/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.shuffle.ock

import com.huawei.ock.common.exception.ApplicationException
import com.huawei.ock.ucache.shuffle.NativeShuffle
import org.apache.spark._
import org.apache.spark.executor.TempShuffleReadMetrics
import org.apache.spark.internal.config.IO_COMPRESSION_CODEC
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.scheduler.OCKScheduler
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.ColumnarShuffleManager
import org.apache.spark.util.{OCKConf, OCKFunctions, Utils}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

class OckColumnarShuffleManager(conf: SparkConf) extends ColumnarShuffleManager with Logging {
  /**
   * A mapping from shuffle ids to the task ids of mappers producing output for those shuffles.
   */
  private[this] val numMapsForOCKShuffle = new ConcurrentHashMap[Int, Long]()
  private[this] val ockConf = new OCKConf(conf)


  val shuffleBlockResolver = new OckColumnarShuffleBlockResolver(conf, ockConf)

  var appId = ""
  var listenFlg: Boolean = false
  var isOckBroadcast: Boolean = ockConf.isOckBroadcast
  var heartBeatFlag = false
  val applicationDefaultAttemptId = "1";

  if (ockConf.excludeUnavailableNodes && ockConf.appId == "driver") {
    OCKScheduler.waitAndBlacklistUnavailableNode(conf)
  }

  OCKFunctions.shuffleInitialize(ockConf, isOckBroadcast)
  val isShuffleCompress: Boolean = conf.get(config.SHUFFLE_COMPRESS)
  val compressCodec: String = conf.get(IO_COMPRESSION_CODEC);
  OCKFunctions.setShuffleCompress(OckColumnarShuffleManager.isCompress(conf), compressCodec)

  /**
   * Obtains a [[ShuffleHandle]] to pass to tasks.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    appId = OCKFunctions.genAppId(conf.getAppId,  SparkContext.getActive.get.applicationAttemptId.getOrElse("1"))
    if (!listenFlg) {
      dependency.rdd.sparkContext.addSparkListener(new OCKShuffleStageListener(conf, appId, ockConf.removeShuffleDataAfterJobFinished))
      listenFlg = true
    }
    var tokenCode: String = ""
    if (isOckBroadcast) {
      tokenCode = OCKFunctions.getToken(ockConf.isIsolated)
      OckColumnarShuffleManager.registerShuffle(shuffleId, dependency.partitioner.numPartitions, conf, ockConf)
    } else {
      tokenCode = OckColumnarShuffleManager.registerShuffle(shuffleId, dependency.partitioner.numPartitions,
        conf, ockConf)
    }
    if (!heartBeatFlag && ockConf.appId == "driver") {
      heartBeatFlag = true
      OCKFunctions.tryStartHeartBeat(this, appId)
    }

    if (dependency.isInstanceOf[ColumnarShuffleDependency[_, _, _]]) {
      new OckColumnarShuffleHandle[K, V](
        shuffleId,
        dependency.asInstanceOf[ColumnarShuffleDependency[K, V, V]],
        tokenCode,
        SparkContext.getActive.get.applicationAttemptId.getOrElse("1"))
    } else {
      new OCKShuffleHandle(shuffleId, dependency, tokenCode,
        SparkContext.getActive.get.applicationAttemptId.getOrElse("1"))
    }
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    logInfo(s"Map task get writer. Task info: shuffleId ${handle.shuffleId} mapId $mapId")

    handle match {
      case ockColumnarShuffleHandle: OckColumnarShuffleHandle[K@unchecked, V@unchecked] =>
        appId = OCKFunctions.genAppId(ockConf.appId, handle.asInstanceOf[OckColumnarShuffleHandle[_, _]].appAttemptId)
        //when ock shuffle work with memory cache will remove numMapsForOCKShuffle
        OckColumnarShuffleManager.registerApp(appId, ockConf, handle.asInstanceOf[OckColumnarShuffleHandle[_, _]].secCode)
        new OckColumnarShuffleWriter(appId, ockConf, ockColumnarShuffleHandle, mapId, context, metrics)
      case ockShuffleHandle: OCKShuffleHandle[K@unchecked, V@unchecked, _] =>
        appId = OCKFunctions.genAppId(ockConf.appId, handle.asInstanceOf[OCKShuffleHandle[_, _, _]].appAttemptId)
        //when ock shuffle work with memory cache will remove numMapsForOCKShuffle
        OckColumnarShuffleManager.registerApp(appId, ockConf, handle.asInstanceOf[OCKShuffleHandle[_, _, _]].secCode)
        val serializerClass: String = ockConf.serializerClass
        val serializer: Serializer = Utils.classForName(serializerClass).newInstance().asInstanceOf[Serializer]
        new OCKShuffleWriter(appId, ockConf, ockShuffleHandle.asInstanceOf[BaseShuffleHandle[K, V, _]],
          serializer, mapId, context, metrics)
    }
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
      logInfo(s"Reduce task get reader. Task info: shuffleId ${handle.shuffleId} reduceId $startPartition - $endPartition ")

    if (handle.isInstanceOf[OckColumnarShuffleHandle[_, _]]) {
      appId = OCKFunctions.genAppId(ockConf.appId, handle.asInstanceOf[OckColumnarShuffleHandle[_, _]].appAttemptId)
      ShuffleManager.registerApp(appId, ockConf, handle.asInstanceOf[OckColumnarShuffleHandle[_, _]].secCode)
      new OckColumnarShuffleReader(appId, handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
        startMapIndex, endMapIndex, startPartition, endPartition, context, conf, ockConf, metrics.asInstanceOf[TempShuffleReadMetrics])
    } else {
      appId = OCKFunctions.genAppId(ockConf.appId, handle.asInstanceOf[OCKShuffleHandle[_, _, _]].appAttemptId)
      ShuffleManager.registerApp(appId, ockConf, handle.asInstanceOf[OCKShuffleHandle[_, _, _]].secCode)
      new OCKShuffleReader(appId, handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
        startMapIndex, endMapIndex, startPartition, endPartition, context, conf, ockConf, metrics.asInstanceOf[TempShuffleReadMetrics])
    }
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
      logInfo(s"Unregister shuffle. Task info: shuffleId $shuffleId")
    Option(numMapsForOCKShuffle.remove(shuffleId)).foreach { numMaps =>
      (0 until numMaps.toInt).foreach { mapId =>
        shuffleBlockResolver.removeDataByMap(shuffleId, mapId)
      }
    }
    true
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    logInfo("stop ShuffleManager")
    if (ockConf.appId == "driver") {
      if (SparkContext.getActive.isDefined) {
        appId = OCKFunctions.genAppId(conf.getAppId, SparkContext.getActive.get.applicationAttemptId.getOrElse(applicationDefaultAttemptId))
      }
      if (appId.nonEmpty) {
        OCKFunctions.tryStopHeartBeat(this, appId)
        OckColumnarShuffleManager.markComplete(ockConf, appId)
      }
    }
    shuffleBlockResolver.stop()
  }
}

private[spark] object OckColumnarShuffleManager extends Logging {

  var externalShuffleServiceFlag :AtomicBoolean = new AtomicBoolean(false)
  var isWR: AtomicBoolean = new AtomicBoolean(false)

  def registerShuffle(
      shuffleId: Int,
      numPartitions: Int,
      conf: SparkConf,
      ockConf: OCKConf): String = {
    val appId = OCKFunctions.genAppId(conf.getAppId, SparkContext.getActive.get.applicationAttemptId.getOrElse("1"))
    val bagPartName = OCKFunctions.concatBagPartName(appId, shuffleId)
    NativeShuffle.shuffleBagBatchCreate(appId, bagPartName, numPartitions, ockConf.priority, 0)

    if (!externalShuffleServiceFlag.get()) {
      try {
        val blockManagerClass = Class.forName("org.apache.spark.storage.BlockManager")
        val externalShuffleServiceEnabledField = blockManagerClass.getDeclaredField("externalShuffleServiceEnabled")
        externalShuffleServiceEnabledField.setAccessible(true)
        externalShuffleServiceEnabledField.set(SparkEnv.get.blockManager, true)
        logInfo("success to change externalShuffleServiceEnabled in block manager to " +
          SparkEnv.get.blockManager.externalShuffleServiceEnabled)
        externalShuffleServiceFlag.set(true)
      } catch {
        case _: Exception =>
          logWarning("failed to change externalShuffleServiceEnabled in block manager," +
            " maybe ockd could not be able to recover in shuffle process")
      }
      conf.set(config.SHUFFLE_SERVICE_ENABLED, true)
    }
    //  generate token code. Need 32bytes.
    OCKFunctions.getToken(ockConf.isIsolated)
  }

  def registerApp(appId: String, ockConf: OCKConf, secCode: String): Unit = {
    if (!isWR.get()) {
      synchronized(if (!isWR.get()) {
        val nodeId = NativeShuffle.registerShuffleApp(appId, ockConf.removeShuffleDataAfterJobFinished, secCode)
        isWR.set(true)
        OCKFunctions.setNodeId(nodeId)
      })
    }
  }

  def markComplete(ockConf: OCKConf, appId: String): Unit = {
    try {
      NativeShuffle.markApplicationCompleted(appId)
    } catch {
      case ex: ApplicationException =>
        logError("Failed to mark application completed")
    }
  }

  def isCompress(conf: SparkConf): Boolean = {
    conf.get(config.SHUFFLE_COMPRESS)
  }
}