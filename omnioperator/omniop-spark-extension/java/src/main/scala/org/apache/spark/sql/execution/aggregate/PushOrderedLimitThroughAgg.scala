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

package org.apache.spark.sql.execution.aggregate;

import com.huawei.boostkit.spark.ColumnarPluginConfig
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{LocalLimitExec, SortExec, SparkPlan, TakeOrderedAndProjectExec, TopNSortExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

object PushOrderedLimitThroughAgg extends Rule[SparkPlan] with PredicateHelper {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!ColumnarPluginConfig.getConf.pushOrderedLimitThroughAggEnable) {
      return plan
    }
    val columnarConf: ColumnarPluginConfig = ColumnarPluginConfig.getSessionConf
    val enableColumnarTopNSort: Boolean = columnarConf.enableColumnarTopNSort

    plan.transform {
      case orderAndProject @ TakeOrderedAndProjectExec(limit, sortOrder, projectList, orderAndProjectChild) => {
        orderAndProjectChild match {
          case finalAgg @ HashAggregateExec(_, _, _, _, _, _, _, _, finalAggChild) =>
            finalAggChild match {
              case shuffleExchange @ ShuffleExchangeExec(_, shuffleExchangeChild, _) =>
                shuffleExchangeChild match {
                  case partialAgg @ HashAggregateExec(_, _, _, partialAggGroupingExpressions, _, _, _, _, _) =>
                    val validSortOrder = sortOrder.takeWhile { order =>
                      partialAggGroupingExpressions.exists(attr => order.child.references.exists(ref => ref.name == attr.name))
                    }
                    if(validSortOrder.nonEmpty) {
                      val newTopNSort = if (enableColumnarTopNSort) {
                        TopNSortExec(limit, strictTopN = false, validSortOrder.take(0), validSortOrder, global = false, child = partialAgg);
                      } else {
                        val newSortExec = SortExec(
                          validSortOrder,
                          global = false,
                          child = partialAgg
                        )
                        LocalLimitExec(limit, child = newSortExec)
                      }
                      session.sparkContext.setLocalProperty("pushOrderedLimitThroughAggApplied", "true");
                      TakeOrderedAndProjectExec(
                        limit, sortOrder, projectList,
                        child = HashAggregateExec(
                          finalAgg.requiredChildDistributionExpressions,
                          finalAgg.isStreaming,
                          finalAgg.numShufflePartitions,
                          finalAgg.groupingExpressions,
                          finalAgg.aggregateExpressions,
                          finalAgg.aggregateAttributes,
                          finalAgg.initialInputBufferOffset,
                          finalAgg.resultExpressions,
                          child = ShuffleExchangeExec(
                            shuffleExchange.outputPartitioning,
                            child = newTopNSort,
                            shuffleExchange.shuffleOrigin
                          )
                        )
                      )
                    } else {
                      orderAndProject
                    }

                  case _ => orderAndProject
                }
              case _ => orderAndProject
            }
          case _ => orderAndProject
        }
      }
    }
  }
}
