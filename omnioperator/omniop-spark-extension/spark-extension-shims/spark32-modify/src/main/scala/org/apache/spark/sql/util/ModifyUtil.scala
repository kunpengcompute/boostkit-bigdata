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

package org.apache.spark.sql.util

import com.huawei.boostkit.spark.util.ModifyUtilAdaptor
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.{ExprId, Expression, PromotePrecision}
import org.apache.spark.sql.catalyst.optimizer.{CombineJoinedAggregates, MergeSubqueryFilters}
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.{ColumnarToRowExec, OmniColumnarToRowExec, SparkPlan}
import org.apache.spark.sql.types.DataType
import com.google.gson.JsonObject

object ModifyUtil extends Logging {

  private def rewriteToOmniJsonExpressionAdapter(expr: Expression,
                                                 exprsIndexMap: Map[ExprId, Int],
                                                 returnDatatype: DataType,
                                                 func: (Expression, Map[ExprId, Int], DataType) => JsonObject): JsonObject = {
    expr match {
      case promotePrecision: PromotePrecision =>
        func(promotePrecision.child, exprsIndexMap, promotePrecision.child.dataType)
      case _ =>
        null
    }
  }

  private def preReplaceSparkPlanAdapter(plan: SparkPlan, func: (SparkPlan => SparkPlan)): SparkPlan = {
    plan match {
      case _ => null
    }
  }

  private def postReplaceSparkPlanAdapter(plan: SparkPlan, func: (SparkPlan => SparkPlan)): SparkPlan = {
    plan match {
      case r: SparkPlan
        if !r.isInstanceOf[QueryStageExec] && !r.supportsColumnar && r.children.exists(c =>
          c.isInstanceOf[ColumnarToRowExec]) =>
        val children = r.children.map {
          case c: ColumnarToRowExec =>
            val child = func(c.child)
            OmniColumnarToRowExec(child)
          case other =>
            func(other)
        }
        r.withNewChildren(children)
      case _ => null
    }
  }

  private def injectRuleAdapter(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule(_ => MergeSubqueryFilters)
    extensions.injectOptimizerRule(_ => CombineJoinedAggregates)
  }

  def registerFunc(): Unit = {
    ModifyUtilAdaptor.configRewriteJsonFunc(rewriteToOmniJsonExpressionAdapter)
    ModifyUtilAdaptor.configPreReplacePlanFunc(preReplaceSparkPlanAdapter)
    ModifyUtilAdaptor.configPostReplacePlanFunc(postReplaceSparkPlanAdapter)
    ModifyUtilAdaptor.configInjectRuleFunc(injectRuleAdapter)
  }
}
