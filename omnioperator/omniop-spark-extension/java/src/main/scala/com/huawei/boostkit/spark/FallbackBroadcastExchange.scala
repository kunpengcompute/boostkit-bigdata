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
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarBroadcastExchangeExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ColumnarBroadcastHashJoinExec}

case class FallbackBroadcastExchange(session: SparkSession) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = PhysicalPlanSelector.maybe(session, plan) {

    val SQLconf = session.sessionState.conf
    val enableColumnarBroadcastExchange: Boolean =
      SQLconf.getConfString("spark.omni.sql.columnar.broadcastexchange", "true").toBoolean
    val enableColumnarBroadcastJoin: Boolean = SQLconf
      .getConfString("spark.omni.sql.columnar.broadcastJoin", "true")
      .toBoolean
    val useColumnarBroadcastExchange: Boolean = enableColumnarBroadcastExchange &&
      enableColumnarBroadcastJoin

    plan.foreach {
      // Falls back for BroadcastNestedLoopJoinExec
      case bnlj: BroadcastNestedLoopJoinExec =>
        val buildSidePlan = bnlj.buildSide match {
          case BuildLeft => bnlj.left
          case BuildRight => bnlj.right
        }

        buildSidePlan match {
          case exec: BroadcastExchangeExec =>
            TransformHints.tagNotTransformable(exec)
            logInfo(s"BroadcastExchange falls back for\n${bnlj}")
          case _ =>
        }

        // Falls back Broadcast when disable ColumnarBroadcastExchange or unsupport BroadcastHashJoin
      case bhj: BroadcastHashJoinExec =>
        val buildSidePlan = bhj.buildSide match {
          case BuildLeft => bhj.left
          case BuildRight => bhj.right
        }

        buildSidePlan match {
          case exec: BroadcastExchangeExec =>
            // fallback if disable ColumnarBroadcastExchange
            if (!useColumnarBroadcastExchange) {
              TransformHints.tagNotTransformable(exec)
              TransformHints.tagNotTransformable(bhj)
              logInfo(s"BroadcastExchange falls back for disable ColumnarBroadcastExchange")
            } else {
              // check whether support ColumnarBroadcastHashJoinExec or not
              try {
                ColumnarBroadcastHashJoinExec(
                  bhj.leftKeys,
                  bhj.rightKeys,
                  bhj.joinType,
                  bhj.buildSide,
                  bhj.condition,
                  bhj.left,
                  bhj.right,
                  bhj.isNullAwareAntiJoin
                ).buildCheck()
              } catch {
                case t: Throwable =>
                  TransformHints.tagNotTransformable(exec)
                  TransformHints.tagNotTransformable(bhj)
                  logInfo(s"BroadcastExchange falls back for ColumnarBroadcastHashJoinExec: ${t}")
              }

              // check whether support ColumnarBroadcastExchangeExec or not
              try {
                new ColumnarBroadcastExchangeExec(
                  exec.mode,
                  exec.child
                ).buildCheck()
              } catch {
                case t: Throwable =>
                  TransformHints.tagNotTransformable(exec)
                  TransformHints.tagNotTransformable(bhj)
                  logInfo(s"BroadcastExchange falls back for ColumnarBroadcastExchangeExec: ${t}")
              }
            }

          case _ =>
        }

      case exec: BroadcastExchangeExec =>
        if (!useColumnarBroadcastExchange) {
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
              logInfo(s"BroadcastExchange falls back for ColumnarBroadcastExchangeExec: ${t}")
          }
        }
      case _ =>
    }
    plan
  }
}
