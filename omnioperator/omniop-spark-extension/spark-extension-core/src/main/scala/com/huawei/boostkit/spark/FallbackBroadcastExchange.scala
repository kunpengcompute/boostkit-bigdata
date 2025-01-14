/*
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

import com.huawei.boostkit.spark.util.PhysicalPlanSelector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.{ColumnarBroadcastExchangeExec, SparkPlan}

case class FallbackBroadcastExchange(session: SparkSession) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = PhysicalPlanSelector.maybe(session, plan) {

    val enableColumnarBroadcastExchange: Boolean =
      ColumnarPluginConfig.getSessionConf.enableColumnarBroadcastExchange

    plan.foreach {
      case exec: BroadcastExchangeExec =>
        if (!enableColumnarBroadcastExchange) {
          TransformHints.tagNotTransformable(exec)
          logInfo(s"BroadcastExchange falls back for disable ColumnarBroadcastExchange")
        } else {
          // check whether support ColumnarBroadcastExchangeExec or not
          try {
            new ColumnarBroadcastExchangeExec(
              exec.mode,
              exec.child
            ).buildCheck()
          } catch {
            case t: Throwable =>
              TransformHints.tagNotTransformable(exec)
              logDebug(s"BroadcastExchange falls back for ColumnarBroadcastExchangeExec: ${t}")
          }
        }
      case _ =>
    }
    plan
  }
}
