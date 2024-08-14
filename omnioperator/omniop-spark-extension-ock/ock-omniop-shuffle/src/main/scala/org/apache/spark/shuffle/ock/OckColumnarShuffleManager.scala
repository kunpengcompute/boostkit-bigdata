/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.shuffle.ock

import com.huawei.ock.common.exception.ApplicationException
import com.huawei.ock.ucache.shuffle.NativeShuffle
import com.huawei.ock.ucache.shuffle.datatype.ArgOption
import org.apache.spark._
import org.apache.spark.executor.TempShuffleReadMetrics
import org.apache.spark.internal.config.IO_COMPRESSION_CODEC
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.scheduler.OCKScheduler
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.ock.spark_3_x.OCKRemoteShuffleHandle
import org.apache.spark.shuffle.sort.ColumnarShuffleManager
import org.apache.spark.util.{OCKConf, OCKRemoteFunctions, Utils}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

class OckColumnarShuffleManager(conf: SparkConf) extends ColumnarShuffleManager with Logging {
  /**
   * A mapping from shuffle ids to the task ids of mappers producing output for those shuffles.
   */
  private[this] val numMapsForOCKShuffle = new ConcurrentHashMap[Int, Long]()
  private[this] val ockConf = new OCKConf(conf)

  override val shuffleBlockResolver = new OckColumnarShuffleBlockResolver(conf, ockConf)
  private val shuffleBlockResolver4RSS = new OCKRemoteShuffleBlockResolver(ockConf)

  var appId = ""
  private val listenFlg: AtomicBoolean = new AtomicBoolean(false)
  private val setFlag: AtomicBoolean = new AtomicBoolean(false)
  var shuffleMode: String = conf.get(OckColumnarDefines.ShuffleModeKey, ShuffleMode.ESS)
  private val functions = new OckColumnarFunctions(shuffleMode, ockConf)
  private val serializerClass: String = ockConf.serializerClass
  val serializer: Serializer = Utils.classForName(serializerClass).newInstance().asInstanceOf[Serializer]

  if (ockConf.excludeUnavailableNodes && ockConf.appId == "driver") {
    OCKScheduler.waitAndBlacklistUnavailableNode(conf)
  }

  functions.shuffleInitialize()
  functions.setShuffleCompress(OckColumnarShuffleManager.isCompress(conf), conf.get(IO_COMPRESSION_CODEC))

  /**
   * Obtains a [[ShuffleHandle]] to pass to tasks.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    appId = functions.genAppId(None)
    if (!listenFlg.get()) {
      synchronized(if (!listenFlg.get()) {
        if (ShuffleMode.RSS == shuffleMode) {
          OCKRemoteFunctions.setLBStrategy(ockConf.lbStrategy, ockConf.lbInitRSSNum)
          dependency.rdd.sparkContext.addSparkListener(new OCKRemoteShuffleStageListener(appId, ockConf.removeShuffleDataAfterJobFinished))
        } else {
          dependency.rdd.sparkContext.addSparkListener(new OCKShuffleStageListener(conf, appId, ockConf.removeShuffleDataAfterJobFinished))
        }
        listenFlg.set(true)
      })
    }
    val tokenCode: String = OckColumnarShuffleManager.registerShuffle(shuffleId, shuffleMode, dependency, conf, ockConf)

    if (dependency.isInstanceOf[ColumnarShuffleDependency[K, V, C]]) {
      val ockColumnarShuffleHandle = new OckColumnarShuffleHandle[K, V](
        shuffleId,
        dependency.asInstanceOf[ColumnarShuffleDependency[K, V, V]],
        tokenCode,
        SparkContext.getActive.get.applicationAttemptId.getOrElse("1"))
      if (ShuffleMode.RSS == shuffleMode) {
        ockColumnarShuffleHandle.setBagsInfo(ShuffleRemoteManager.bagsInfo)
          .setNodeInfo(ShuffleRemoteManager.rssNodeInfo)
          .setRepInfo(ShuffleRemoteManager.repInfo)
          .setDriverRpc(ShuffleRemoteManager.driverRpcAddress)
      } else {
        ockColumnarShuffleHandle.setDriverRpc(ShuffleManager.driverRpcAddress)
      }

      ockColumnarShuffleHandle
    } else {
      if (ShuffleMode.RSS == shuffleMode) {
        new spark_3_x.OCKRemoteShuffleHandle(shuffleId, dependency, tokenCode, SparkContext.getActive.get.applicationAttemptId.getOrElse("1"),
          ShuffleRemoteManager.bagsInfo, ShuffleRemoteManager.repInfo, ShuffleRemoteManager.rssNodeInfo, ShuffleRemoteManager.driverRpcAddress)
      } else {
        new OCKShuffleHandle(shuffleId, dependency, tokenCode, SparkContext.getActive.get.applicationAttemptId.getOrElse("1"),
          ShuffleManager.driverRpcAddress)
      }
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
        appId = functions.genAppId(Some(handle.asInstanceOf[OckColumnarShuffleHandle[_, _]].appAttemptId))
        //when ock shuffle work with memory cache will remove numMapsForOCKShuffle
        OckColumnarShuffleManager.registerApp(appId, ockConf, shuffleMode, ockColumnarShuffleHandle, context, isWriter = true)
        if (ShuffleMode.RSS == shuffleMode) {
          new OckColumnarRssShuffleWriter(appId, ockConf, ockColumnarShuffleHandle, mapId, context, metrics, ockColumnarShuffleHandle.rssNodeInfo)
        } else {
          new OckColumnarShuffleWriter(appId, ockConf, ockColumnarShuffleHandle, mapId, context, metrics)
        }
      case ockShuffleHandle: OCKShuffleHandle[K@unchecked, V@unchecked, _] =>
        appId = functions.genAppId(Some(handle.asInstanceOf[OCKShuffleHandle[_, _, _]].appAttemptId))
        NativeShuffle.linkToDriverRPC(ockShuffleHandle.driverRpc)
        //when ock shuffle work with memory cache will remove numMapsForOCKShuffle
        ShuffleManager.registerApp(appId, ockConf, handle.asInstanceOf[OCKShuffleHandle[_, _, _]].secCode)
        new OCKShuffleWriter(appId, ockConf, ockShuffleHandle.asInstanceOf[BaseShuffleHandle[K, V, _]],
          serializer, mapId, context, metrics)
      case ockRemoteHandle: OCKRemoteShuffleHandle[K@unchecked, V@unchecked, _] =>
        val shuffleHandle = handle.asInstanceOf[spark_3_x.OCKRemoteShuffleHandle[_, _, _]]
        if (setFlag.compareAndSet(false, true)) {
          NativeShuffle.shuffleArgsOption(ArgOption.DRIVER_RPC_ADDRESS,
            "%s:%d".format(shuffleHandle.driverRpc.getIp, shuffleHandle.driverRpc.getPort).getBytes())
        }

        NativeShuffle.linkToDriverRPC(shuffleHandle.driverRpc)

        appId = functions.genAppId(Some(shuffleHandle.appAttemptId))
        val strategyName = ockConf.lbStrategy
        val bagNamePrefixWithStage: String = "%s_%d_%d_%d".format(appId, handle.shuffleId, context.stageId(), context.stageAttemptNumber())
        ShuffleRemoteManager.registerShuffleStage(appId, strategyName, bagNamePrefixWithStage, ockConf,
          shuffleHandle.secCode, shuffleHandle.getBagsInfo, shuffleHandle.rssRepInfo)
        new OCKRemoteShuffleWriter(appId, ockConf, shuffleBlockResolver4RSS, ockRemoteHandle.asInstanceOf[BaseShuffleHandle[K, V, _]],
          serializer, mapId, context, metrics, shuffleHandle.rssNodeInfo, shuffleHandle.driverRpc)
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

    handle match {
      case ockColumnarShuffleHandle: OckColumnarShuffleHandle[K@unchecked, _] =>
        appId = functions.genAppId(Some(handle.asInstanceOf[OckColumnarShuffleHandle[_, _]].appAttemptId))
        if (ShuffleMode.RSS == shuffleMode) {
          shuffleBlockResolver4RSS.releaseShuffleManager(handle.shuffleId)
        }
        OckColumnarShuffleManager.registerApp(appId, ockConf, shuffleMode, ockColumnarShuffleHandle, context, isWriter = false)
        new OckColumnarShuffleReader(appId, ockColumnarShuffleHandle.asInstanceOf[BaseShuffleHandle[K, _, C]],
          startMapIndex, endMapIndex, startPartition, endPartition, context, conf, ockConf, metrics.asInstanceOf[TempShuffleReadMetrics])
      case ockShuffleHandle: OCKShuffleHandle[K@unchecked, _, _] =>
        appId = functions.genAppId(Some(handle.asInstanceOf[OCKShuffleHandle[_, _, _]].appAttemptId))
        NativeShuffle.linkToDriverRPC(ockShuffleHandle.driverRpc)
        //when ock shuffle work with memory cache will remove numMapsForOCKShuffle
        ShuffleManager.registerApp(appId, ockConf, handle.asInstanceOf[OCKShuffleHandle[_, _, _]].secCode)
        new OCKShuffleReader(appId, ockShuffleHandle.asInstanceOf[BaseShuffleHandle[K, _, C]],
          startMapIndex, endMapIndex, startPartition, endPartition, context, conf, ockConf, metrics.asInstanceOf[TempShuffleReadMetrics])
      case ockRemoteHandle: OCKRemoteShuffleHandle[K@unchecked, _, C@unchecked] =>
        shuffleBlockResolver4RSS.releaseShuffleManager(handle.shuffleId)

        val shuffleHandle = handle.asInstanceOf[spark_3_x.OCKRemoteShuffleHandle[_, _, _]]
        if (setFlag.compareAndSet(false, true)) {
          NativeShuffle.shuffleArgsOption(ArgOption.DRIVER_RPC_ADDRESS, "%s:%d".format(shuffleHandle.driverRpc.getIp,
            shuffleHandle.driverRpc.getPort).getBytes())
        }

        NativeShuffle.linkToDriverRPC(shuffleHandle.driverRpc)
        appId = functions.genAppId(Some(shuffleHandle.appAttemptId))
        val strategyName = ockConf.lbStrategy
        ShuffleRemoteManager.registerApp(appId, strategyName, ockConf, shuffleHandle.secCode, shuffleHandle.rssRepInfo)

        new OCKRemoteShuffleReader(appId, ockRemoteHandle.asInstanceOf[BaseShuffleHandle[K, _, C]],
          startMapIndex, endMapIndex, startPartition, endPartition, context, conf, ockConf,
          metrics.asInstanceOf[TempShuffleReadMetrics])
    }
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
      logInfo(s"Unregister shuffle. Task info: shuffleId $shuffleId")
    Option(numMapsForOCKShuffle.remove(shuffleId)).foreach { numMaps =>
      (0 until numMaps.toInt).foreach { mapId =>
        if (ShuffleMode.RSS == shuffleMode) {
          shuffleBlockResolver4RSS.removeDataByMap(shuffleId, mapId)
        } else {
          shuffleBlockResolver.removeDataByMap(shuffleId, mapId)
        }
      }
    }
    true
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    logInfo("stop ShuffleManager")
    if (ockConf.appId == "driver") {
      if (SparkContext.getActive.isDefined) {
        appId = functions.genAppId(None)
      }
      if (appId.nonEmpty) {
        OckColumnarShuffleManager.markComplete(appId)
      }
    }
    if (ShuffleMode.RSS == shuffleMode) {
      shuffleBlockResolver4RSS.stop()
    } else {
      shuffleBlockResolver.stop()
    }
  }
}

private[spark] object OckColumnarShuffleManager extends Logging {
  private[this] val setFlag :AtomicBoolean = new AtomicBoolean(false)

  private def registerShuffle[K, V, C](
      shuffleId: Int,
      shuffleMode: String,
      dependency: ShuffleDependency[K, V, C],
      conf: SparkConf,
      ockConf: OCKConf): String = {
    var tokenCode: String = ""
    if (ShuffleMode.RSS == shuffleMode) {
      tokenCode = OCKRemoteFunctions.getToken(ockConf.isIsolated)
      ShuffleRemoteManager.registerShuffle(shuffleId, dependency.partitioner.numPartitions, conf, ockConf)
    } else {
      tokenCode = ShuffleManager.registerShuffle(shuffleId, dependency.partitioner.numPartitions, conf, ockConf)
    }
    tokenCode
  }

  private def registerApp(
      appId: String,
      ockConf: OCKConf,
      shuffleMode: String,
      handle: OckColumnarShuffleHandle[_, _],
      context: TaskContext,
      isWriter: Boolean
    ): Unit = {
    if (ShuffleMode.RSS == shuffleMode) {
      if (setFlag.compareAndSet(false, true)) {
        NativeShuffle.shuffleArgsOption(ArgOption.DRIVER_RPC_ADDRESS, "%s:%d".format(handle.getDriverRpc.getIp,
          handle.getDriverRpc.getPort).getBytes())
      }
      NativeShuffle.linkToDriverRPC(handle.getDriverRpc)
      val strategyName = ockConf.lbStrategy
      if (isWriter) {
        val bagNamePrefixWithStage: String = "%s_%d_%d_%d".format(appId, handle.shuffleId, context.stageId(), context.stageAttemptNumber())
        ShuffleRemoteManager.registerShuffleStage(appId, strategyName, bagNamePrefixWithStage, ockConf,
          handle.secCode, handle.getBagsInfo, handle.rssRepInfo)
      } else {
        ShuffleRemoteManager.registerApp(appId, strategyName, ockConf, handle.secCode, handle.rssRepInfo)
      }
    } else {
      NativeShuffle.linkToDriverRPC(handle.getDriverRpc)
      ShuffleManager.registerApp(appId, ockConf, handle.secCode)
    }
  }

  private def markComplete(appId: String): Unit = {
    try {
      NativeShuffle.markApplicationCompleted(appId)
    } catch {
      case ex: ApplicationException =>
        logError("Failed to mark application completed for " + ex.getMessage)
    }
  }

  def isCompress(conf: SparkConf): Boolean = {
    conf.get(config.SHUFFLE_COMPRESS)
  }
}