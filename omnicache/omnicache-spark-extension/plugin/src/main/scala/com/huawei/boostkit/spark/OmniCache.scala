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

package com.huawei.boostkit.spark

import com.huawei.boostkit.spark.util.RewriteLogger

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.optimizer.OmniCacheOptimizer
import org.apache.spark.sql.catalyst.parser.OmniCacheExtensionSqlParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

class OmniCache extends (SparkSessionExtensions => Unit) with RewriteLogger {
  override def apply(extensions: SparkSessionExtensions): Unit = {

    // OmniCache internal parser
    extensions.injectParser { case (spark, parser) =>
      new OmniCacheExtensionSqlParser(spark, parser)
    }
    // OmniCache optimizer rules
    extensions.injectPostHocResolutionRule { (session: SparkSession) =>
      OmniCacheOptimizerRule(session)
    }
  }
}

case class OmniCacheOptimizerRule(session: SparkSession) extends Rule[LogicalPlan] {
  self =>

  var notAdded = true

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (notAdded) {
      self.synchronized {
        if (notAdded) {
          notAdded = false
          val sessionState = session.sessionState
          val field = sessionState.getClass.getDeclaredField("optimizer")
          field.setAccessible(true)
          field.set(sessionState,
            OmniCacheOptimizer(session, sessionState.optimizer))
        }
      }
    }
    plan
  }
}
