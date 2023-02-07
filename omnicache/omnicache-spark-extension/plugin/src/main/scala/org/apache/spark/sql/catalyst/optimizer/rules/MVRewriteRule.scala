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
import com.huawei.boostkit.spark.conf.OmniCachePluginConfig
import com.huawei.boostkit.spark.util.{RewriteHelper, RewriteLogger, ViewMetadata}
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.{FullOuter, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.{ExplainCommand, OmniCacheCreateMvCommand}
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.kvstore.KVIndex

class MVRewriteRule(session: SparkSession) extends Rule[LogicalPlan] with RewriteHelper {
  var cannotRewritePlans: Set[LogicalPlan] = Set[LogicalPlan]()

  val omniCacheConf: OmniCachePluginConfig = OmniCachePluginConfig.getConf

  val joinRule = new MaterializedViewJoinRule(session)
  val outJoinRule = new MaterializedViewOutJoinRule(session)
  val aggregateRule = new MaterializedViewAggregateRule(session)
  val outJoinAggregateRule = new MaterializedViewOutJoinAggregateRule(session)

  override def apply(logicalPlan: LogicalPlan): LogicalPlan = {
    if (!omniCacheConf.enableOmniCache || cannotRewritePlans.contains(logicalPlan)) {
      return logicalPlan
    }
    try {
      logicalPlan match {
        case _: OmniCacheCreateMvCommand | ExplainCommand(_, _) =>
          logicalPlan
        case _ =>
          tryRewritePlan(logicalPlan)
      }
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to rewrite plan with mv,errmsg: ${e.getMessage}")
        logicalPlan
    }
  }

  def tryRewritePlan(plan: LogicalPlan): LogicalPlan = {
    val usingMvs = mutable.Set.empty[String]
    RewriteTime.clear()
    val rewriteStartSecond = System.currentTimeMillis()

    if (ViewMetadata.status == ViewMetadata.STATUS_LOADING) {
      return plan
    }
    // init viewMetadata by full queryPlan
    RewriteTime.withTimeStat("viewMetadata") {
      ViewMetadata.init(session, Some(plan))
    }

    var res = RewriteHelper.optimizePlan(plan)
    val queryTables = extractTablesOnly(res).toSet
    val candidateViewPlans = RewriteTime.withTimeStat("getApplicableMaterializations") {
      getApplicableMaterializations(queryTables)
          .filter(x => !OmniCachePluginConfig.isMVInUpdate(x._2))
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
                    outJoinRule.perform(Some(p), p.child, usingMvs, candidateViewPlan)
                  } else {
                    joinRule.perform(Some(p), p.child, usingMvs, candidateViewPlan)
                  }
                case a: Aggregate =>
                  var rewritedPlan = if (containsOuterJoin(a)) {
                    outJoinAggregateRule.perform(None, a, usingMvs, candidateViewPlan)
                  } else {
                    aggregateRule.perform(None, a, usingMvs, candidateViewPlan)
                  }
                  // below agg may be join/filter can be rewrite
                  if (rewritedPlan == a && !a.child.isInstanceOf[Project]) {
                    val child = Project(
                      RewriteHelper.extractAllAttrsFromExpression(
                        a.aggregateExpressions).toSeq, a.child)
                    val rewritedChild = if (containsOuterJoin(a)) {
                      outJoinRule.perform(Some(child), child.child, usingMvs, candidateViewPlan)
                    } else {
                      joinRule.perform(Some(child), child.child, usingMvs, candidateViewPlan)
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
    if (usingMvs.nonEmpty) {
      RewriteTime.withTimeStat("checkAttrsValid") {
        if (!RewriteHelper.checkAttrsValid(res)) {
          RewriteTime.statFromStartTime("total", rewriteStartSecond)
          logBasedOnLevel(RewriteTime.stat())
          return plan
        }
      }
      val sql = session.sparkContext.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION)
      val mvs = usingMvs.mkString(";").replaceAll("`", "")
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
          case _: LeftOuter.type => return true
          case _: RightOuter.type => return true
          case _: FullOuter.type => return true
          case _: LeftSemi.type => return true
          case _: LeftAnti.type => return true
          case _ =>
        }
      case _ =>
    }
    false
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
