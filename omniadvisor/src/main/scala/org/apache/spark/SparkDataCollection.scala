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

package org.apache.spark

import com.huawei.boostkit.omniadvisor.models.AppResult
import org.apache.spark.internal.config.Status.ASYNC_TRACKING_ENABLED
import org.apache.spark.scheduler.ReplayListenerBus
import org.apache.spark.sql.execution.ui.{SQLAppStatusListener, SQLAppStatusStore, SQLExecutionUIData, SparkPlanGraph}
import org.apache.spark.status.api.v1
import org.apache.spark.status.{AppStatusListener, AppStatusStore, ElementTrackingStore}
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.{InMemoryStore, KVStore}
import org.slf4j.{Logger, LoggerFactory}

import java.io.InputStream
import scala.collection.mutable

class SparkDataCollection {
  val LOG: Logger = LoggerFactory.getLogger(classOf[SparkDataCollection])

  private val conf = new SparkConf

  var environmentInfo: v1.ApplicationEnvironmentInfo = _
  var jobsList: Seq[v1.JobData] = _
  var appInfo: v1.ApplicationInfo = _
  var sqlExecutionsList: Seq[SQLExecutionUIData] = _
  var sqlGraphMap: mutable.Map[Long, SparkPlanGraph] = _

  def replayEventLogs(in: InputStream, sourceName: String): Unit = {

    val store: KVStore = createInMemoryStore()
    val replayConf: SparkConf = conf.clone().set(ASYNC_TRACKING_ENABLED, false)
    val trackingStore: ElementTrackingStore = new ElementTrackingStore(store, replayConf)
    val replayBus: ReplayListenerBus = new ReplayListenerBus()
    val listener = new AppStatusListener(trackingStore, replayConf, false)
    val sqlListener = new SQLAppStatusListener(replayConf, trackingStore, false)
    replayBus.addListener(listener)
    replayBus.addListener(sqlListener)

    try {
      replayBus.replay(in, sourceName, maybeTruncated = true)
      trackingStore.close(false)
    } catch {
      case e: Exception =>
        Utils.tryLogNonFatalError {
          trackingStore.close()
        }
        throw e
    }
    LOG.info("Replay of logs complete")
    val appStatusStore: AppStatusStore = new AppStatusStore(store)
    appInfo = appStatusStore.applicationInfo()
    environmentInfo = appStatusStore.environmentInfo()
    jobsList = appStatusStore.jobsList(null)

    val sqlAppStatusStore: SQLAppStatusStore = new SQLAppStatusStore(store)
    sqlExecutionsList = sqlAppStatusStore.executionsList()
    sqlGraphMap = mutable.HashMap.empty[Long, SparkPlanGraph]
    sqlExecutionsList.foreach { sqlExecution =>
      try {
        val planGraph = sqlAppStatusStore.planGraph(sqlExecution.executionId)
        sqlGraphMap.put(sqlExecution.executionId, planGraph)
      } catch {
        case e: Exception =>
          LOG.warn(s"Get PlanGraph for SQLExecution [${sqlExecution.executionId}] in ${appInfo.id}")
      }
      sqlGraphMap.put(sqlExecution.executionId, sqlAppStatusStore.planGraph(sqlExecution.executionId))
    }

    appStatusStore.close()
  }

  def getAppResult(workload: String): AppResult = {
    SparkApplicationDataExtractor.extractAppResultFromAppStatusStore(appInfo, workload, environmentInfo, jobsList, sqlExecutionsList, sqlGraphMap)
  }

  private def createInMemoryStore(): KVStore = {
    val store = new InMemoryStore()
    store
  }
}
