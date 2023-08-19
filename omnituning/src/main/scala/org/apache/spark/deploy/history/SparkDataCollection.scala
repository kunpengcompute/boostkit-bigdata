/*
 * Copyright (C) 2020-2023. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.history

import com.huawei.boostkit.omnituning.models.AppResult
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.status.api.v1
import org.apache.spark.util.kvstore.{InMemoryStore, KVStore}
import org.apache.spark.internal.config.Status.ASYNC_TRACKING_ENABLED
import org.apache.spark.scheduler.ReplayListenerBus
import org.apache.spark.status.{AppStatusStore, CustomAppStatusListener, ElementTrackingStore}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.OpenHashSet

import java.io.InputStream
import java.util.{ ArrayList => JArrayList, HashSet => JHashSet, List => JList, Set => JSet }
import scala.collection.mutable

class SparkDataCollection {

  private val conf = new SparkConf

  var environmentInfo: v1.ApplicationEnvironmentInfo = null
  var executorSummary: Seq[v1.ExecutorSummary] = null
  var jobData: Seq[v1.JobData] = null
  var stageData: Seq[v1.StageData] = null
  var appInfo: v1.ApplicationInfo = null

  private var executorIdToMaxUtilizedMemory: mutable.HashMap[String, Long] = mutable.HashMap.empty

  val logger: Logger = Logger.getLogger(SparkDataCollection.getClass)

  def replayEventLogs(in: InputStream, sourceName: String): Unit = {
    val store: KVStore = createInMemoryStore()
    val replayConf: SparkConf = conf.clone().set(ASYNC_TRACKING_ENABLED, false)
    val trackingStore: ElementTrackingStore = new ElementTrackingStore(store, replayConf)
    val replayBus: ReplayListenerBus = new ReplayListenerBus()
    val listener: CustomAppStatusListener = new CustomAppStatusListener(trackingStore, replayConf, false)
    replayBus.addListener(listener)

    try {
      replayBus.replay(in, sourceName, true)
      trackingStore.close(false)
    } catch {
      case e: Exception =>
        Utils.tryLogNonFatalError {
          trackingStore.close()
        }
        throw e
    }
    logger.info("Replay of logs complete")
    executorIdToMaxUtilizedMemory = listener.executorIdToMaxUtilizedMemory
    val appStatusStore: AppStatusStore = new AppStatusStore(store)
    appInfo = appStatusStore.applicationInfo()
    environmentInfo = appStatusStore.environmentInfo()
    executorSummary = appStatusStore.executorList(false)
    jobData = appStatusStore.jobsList(null)
    stageData = appStatusStore.stageList(null)
    logger.info("executors = " + executorSummary.size)
    appStatusStore.close()
  }

  def getAppResult(workload: String): AppResult = {
    SparkApplicationDataExtractor.extractAppResultFromAppStatusStore(appInfo, workload, environmentInfo, jobData)
  }

  private def createInMemoryStore(): KVStore = {
    val store = new InMemoryStore()
    store
  }
}

object SparkDataCollection {

  def stringToSet(str: String): JSet[String] = {
    val set = new JHashSet[String]()
    str.split(",").foreach { case t: String => set.add(t)}
    set
  }

  def toJList[T](seq: Seq[T]): JList[T] = {
    val list = new JArrayList[T]()
    seq.foreach { case (item: T) => list.add(item)}
    list
  }

  def addIntSetToJSet(set: OpenHashSet[Int], jset: JSet[Integer]): Unit = {
    val it = set.iterator
    while (it.hasNext) {
      jset.add(it.next())
    }
  }

  def addIntSetToJSet(set: mutable.HashSet[Int], jset: JSet[Integer]): Unit = {
    val it = set.iterator
    while (it.hasNext) {
      jset.add(it.next())
    }
  }
}
