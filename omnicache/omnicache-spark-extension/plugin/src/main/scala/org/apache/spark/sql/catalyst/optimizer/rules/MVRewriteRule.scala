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

import com.huawei.boostkit.spark.conf.OmniCachePluginConfig
import com.huawei.boostkit.spark.util.RewriteHelper
import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.OmniCacheCreateMvCommand

class MVRewriteRule(session: SparkSession) extends Rule[LogicalPlan] with Logging {
  val omniCacheConf: OmniCachePluginConfig = OmniCachePluginConfig.getConf

  val joinRule = new MaterializedViewJoinRule(session)
  val aggregateRule = new MaterializedViewAggregateRule(session)

  override def apply(logicalPlan: LogicalPlan): LogicalPlan = {
    if (!omniCacheConf.enableOmniCache) {
      return logicalPlan
    }
    try {
      logicalPlan match {
        case _: OmniCacheCreateMvCommand =>
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
    val res = plan.transformDown {
      case p: Project =>
        joinRule.perform(Some(p), p.child, usingMvs)
      case a: Aggregate =>
        var rewritedPlan = aggregateRule.perform(None, a, usingMvs)
        // below agg may be join/filter can be rewrite
        if (rewritedPlan == a) {
          val child = Project(
            RewriteHelper.extractAllAttrsFromExpression(a.aggregateExpressions).toSeq, a.child)
          val rewritedChild = joinRule.perform(Some(child), child.child, usingMvs)
          if (rewritedChild != child) {
            rewritedPlan = a.copy(child = rewritedChild)
          }
        }
        rewritedPlan
      case p => p
    }
    if (usingMvs.nonEmpty) {
      val sql = session.sparkContext.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION)
      val mvs = usingMvs.mkString(";").replaceAll("`", "")
      val log = "logicalPlan MVRewrite success,using materialized view:[%s],original sql:%s"
          .format(mvs, sql)
      logWarning(log)
      session.sparkContext.listenerBus.post(SparkListenerMVRewriteSuccess(sql, mvs))
    }
    res
  }
}

case class SparkListenerMVRewriteSuccess(sql: String, usingMvs: String) extends SparkListenerEvent {
}
