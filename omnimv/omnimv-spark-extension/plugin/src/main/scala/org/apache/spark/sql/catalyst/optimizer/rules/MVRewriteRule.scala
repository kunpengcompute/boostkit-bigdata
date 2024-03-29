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

package org.apache.spark.sql.catalyst.optimizer.rules

import com.fasterxml.jackson.annotation.JsonIgnore
import com.huawei.boostkit.spark.conf.OmniMVPluginConfig
import com.huawei.boostkit.spark.util.{RewriteHelper, RewriteLogger, ViewMetadata}
import com.huawei.boostkit.spark.util.ViewMetadata._
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.{FullOuter, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable, OptimizedCreateHiveTableAsSelectCommand}
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.kvstore.KVIndex

class MVRewriteRule(session: SparkSession)
    extends Rule[LogicalPlan] with RewriteHelper with RewriteLogger {
  private var cannotRewritePlans: Set[LogicalPlan] = Set[LogicalPlan]()

  private val omniMVConf: OmniMVPluginConfig = OmniMVPluginConfig.getConf

  private val joinRule = new MaterializedViewJoinRule(session)
  private val outJoinRule = new MaterializedViewOutJoinRule(session)
  private val aggregateRule = new MaterializedViewAggregateRule(session)
  private val outJoinAggregateRule = new MaterializedViewOutJoinAggregateRule(session)

  override def apply(logicalPlan: LogicalPlan): LogicalPlan = {
    if (!omniMVConf.enableOmniMV || cannotRewritePlans.contains(logicalPlan)) {
      return logicalPlan
    }
    RewriteHelper.disableSqlLog()
    try {
      logicalPlan match {
        case _: CreateHiveTableAsSelectCommand =>
          tryRewritePlan(logicalPlan)
        case _: OptimizedCreateHiveTableAsSelectCommand =>
          tryRewritePlan(logicalPlan)
        case _: InsertIntoHadoopFsRelationCommand =>
          tryRewritePlan(logicalPlan)
        case _: InsertIntoHiveTable =>
          tryRewritePlan(logicalPlan)
        case _: Command =>
          logicalPlan
        case _ =>
          tryRewritePlan(logicalPlan)
      }
    } catch {
      case e: Throwable =>
        logError(s"Failed to rewrite plan with mv.")
        logicalPlan
    } finally {
      RewriteHelper.enableSqlLog()
    }
  }

  def tryRewritePlan(plan: LogicalPlan): LogicalPlan = {
    val usingMvInfos = mutable.Set.empty[(String, String)]
    RewriteTime.clear()
    val rewriteStartSecond = System.currentTimeMillis()

    if (ViewMetadata.status == ViewMetadata.STATUS_LOADING) {
      return plan
    }
    // init viewMetadata by full queryPlan
    RewriteTime.withTimeStat("viewMetadata") {
      ViewMetadata.init(session, Some(plan))
    }

    // automatic wash out
    if (OmniMVPluginConfig.getConf.enableAutoWashOut) {
      val autoCheckInterval: Long = RewriteHelper.secondsToMillisecond(
        OmniMVPluginConfig.getConf.autoCheckWashOutTimeInterval)
      val autoWashOutTime: Long = ViewMetadata.autoWashOutTimestamp.getOrElse(0)
      if ((System.currentTimeMillis() - autoWashOutTime) >= autoCheckInterval) {
        automaticWashOutCheck()
      }
    }

    var res = RewriteHelper.optimizePlan(plan)
    val queryTables = extractTablesOnly(res).toSet
    val candidateViewPlans = RewriteTime.withTimeStat("getApplicableMaterializations") {
      getApplicableMaterializations(queryTables)
          .filter(x => !OmniMVPluginConfig.isMVInUpdate(x._2))
    }

    if (candidateViewPlans.isEmpty) {
      logDetail(s"no candidateViewPlans")
    } else {
      for (candidateViewPlan <- candidateViewPlans) {
        res = res.transformDown {
          case r =>
            if (RewriteHelper.containsMV(r)) {
              r
            } else {
              r match {
                case p: Project =>
                  if (containsOuterJoin(p)) {
                    outJoinRule.perform(Some(p), p.child, usingMvInfos, candidateViewPlan)
                  } else {
                    joinRule.perform(Some(p), p.child, usingMvInfos, candidateViewPlan)
                  }
                case a: Aggregate =>
                  var rewritedPlan = if (containsOuterJoin(a)) {
                    outJoinAggregateRule.perform(None, a, usingMvInfos, candidateViewPlan)
                  } else {
                    aggregateRule.perform(None, a, usingMvInfos, candidateViewPlan)
                  }
                  // below agg may be join/filter can be rewrite
                  if (rewritedPlan == a && !a.child.isInstanceOf[Project]) {
                    val child = Project(
                      RewriteHelper.extractAllAttrsFromExpression(
                        a.aggregateExpressions).toSeq, a.child)
                    val rewritedChild = if (containsOuterJoin(a)) {
                      outJoinRule.perform(Some(child), child.child, usingMvInfos, candidateViewPlan)
                    } else {
                      joinRule.perform(Some(child), child.child, usingMvInfos, candidateViewPlan)
                    }
                    if (rewritedChild != child) {
                      val projectChild = rewritedChild.asInstanceOf[Project]
                      rewritedPlan = a.copy(child = Project(
                        projectChild.projectList ++ projectChild.child.output, projectChild.child))
                    }
                  }
                  rewritedPlan
                case p => p
              }
            }
        }
      }
    }

    RewriteTime.queue.add(("load_mv.nums", ViewMetadata.viewToTablePlan.size()))
    if (usingMvInfos.nonEmpty) {
      RewriteTime.withTimeStat("checkAttrsValid") {
        if (!RewriteHelper.checkAttrsValid(res)) {
          RewriteTime.statFromStartTime("total", rewriteStartSecond)
          logBasedOnLevel(RewriteTime.stat())
          return plan
        }
      }
      val sql = session.sparkContext.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION)
      val mvs = usingMvInfos.mkString(";").replaceAll("`", "")
      val costSecond = (System.currentTimeMillis() - rewriteStartSecond).toString
      val log = ("logicalPlan MVRewrite success," +
          "using materialized view:[%s],cost %s milliseconds,")
          .format(mvs, costSecond)
      logBasedOnLevel(log)
      session.sparkContext.listenerBus.post(SparkListenerMVRewriteSuccess(sql, mvs))
    } else {
      res = plan
      cannotRewritePlans += res
    }
    RewriteTime.statFromStartTime("total", rewriteStartSecond)
    logBasedOnLevel(RewriteTime.stat())
    res
  }

  def containsOuterJoin(plan: LogicalPlan): Boolean = {
    plan.foreach {
      case j: Join =>
        j.joinType match {
          case LeftOuter => return true
          case RightOuter => return true
          case FullOuter => return true
          case LeftSemi => return true
          case LeftAnti => return true
          case _ =>
        }
      case _ =>
    }
    false
  }

  private def automaticWashOutCheck(): Unit = {
    val timeInterval = OmniMVPluginConfig.getConf.autoWashOutTimeInterval
    val threshold = System.currentTimeMillis() - RewriteHelper.daysToMillisecond(timeInterval)
    val viewQuantity = OmniMVPluginConfig.getConf.automaticWashOutMinimumViewQuantity

    loadViewCount()
    loadWashOutTimestamp()

    if (ViewMetadata.viewCnt.size() >= viewQuantity &&
        (ViewMetadata.washOutTimestamp.isEmpty ||
            (ViewMetadata.washOutTimestamp.get <= threshold))) {
      ViewMetadata.spark.sql("WASH OUT MATERIALIZED VIEW")
      logInfo("WASH OUT MATERIALIZED VIEW BY AUTOMATICALLY.")
      ViewMetadata.autoWashOutTimestamp = Some(System.currentTimeMillis())
    }
  }
}

@DeveloperApi
case class SparkListenerMVRewriteSuccess(sql: String, usingMvs: String) extends SparkListenerEvent {
  @JsonIgnore
  @KVIndex
  def id: String = (System.currentTimeMillis() + "%s%s".format(sql, usingMvs).hashCode).toString
}

class MVRewriteSuccessListener(
    kvStore: ElementTrackingStore) extends SparkListener with RewriteLogger {

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case _: SparkListenerMVRewriteSuccess =>
        kvStore.write(event)
      case _ =>
    }
  }
}

object RewriteTime {
  val timeStat: mutable.Map[String, Long] = mutable.HashMap[String, Long]()
  val queue = new LinkedBlockingQueue[(String, Long)]()

  def statFromStartTime(key: String, startTime: Long): Unit = {
    queue.add((key, System.currentTimeMillis() - startTime))
  }

  def clear(): Unit = {
    timeStat.clear()
    queue.clear()
  }

  def withTimeStat[T](key: String)(f: => T): T = {
    val startTime = System.currentTimeMillis()
    try {
      f
    } finally {
      statFromStartTime(key, startTime)
    }
  }

  def stat(): String = {
    queue.forEach { infos =>
      val (key, time) = infos
      if (key.endsWith(".C")) {
        timeStat += (key -> Math.max(timeStat.getOrElse(key, 0L), time))
      } else {
        timeStat += (key -> (timeStat.getOrElse(key, 0L) + time))
      }
    }
    s"plugin cost:${timeStat.toSeq.sortWith((a, b) => a._2 > b._2).toString()}"
  }
}
