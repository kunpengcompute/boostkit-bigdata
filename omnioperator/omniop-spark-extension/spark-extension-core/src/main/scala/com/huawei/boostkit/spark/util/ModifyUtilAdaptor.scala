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

package com.huawei.boostkit.spark.util

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.{ExprId, Expression}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DataType
import com.google.gson.JsonObject

object ModifyUtilAdaptor {
  private var rewriteJsonFunc: (Expression, Map[ExprId, Int], DataType, (Expression, Map[ExprId, Int], DataType) => JsonObject) => JsonObject = _

  private var preReplacePlanFunc: (SparkPlan, SparkPlan => SparkPlan) => SparkPlan = _

  private var postReplacePlanFunc: (SparkPlan, SparkPlan => SparkPlan) => SparkPlan = _

  private var injectRuleFunc: SparkSessionExtensions => Unit = _

  def configRewriteJsonFunc(func: (Expression, Map[ExprId, Int], DataType, (Expression, Map[ExprId, Int], DataType) => JsonObject) => JsonObject): Unit = {
    rewriteJsonFunc = func
  }

  def configPreReplacePlanFunc(func: (SparkPlan, SparkPlan => SparkPlan) => SparkPlan): Unit = {
    preReplacePlanFunc = func
  }

  def configPostReplacePlanFunc(func: (SparkPlan, SparkPlan => SparkPlan) => SparkPlan): Unit = {
    postReplacePlanFunc = func
  }

  def configInjectRuleFunc(func: SparkSessionExtensions => Unit): Unit = {
    injectRuleFunc = func
  }

  val registration: Unit = {
    val ModifyUtilClazz = Thread.currentThread().getContextClassLoader.loadClass("org.apache.spark.sql.util.ModifyUtil")
    val method = ModifyUtilClazz.getMethod("registerFunc")
    method.invoke(null)
  }

  def rewriteToOmniJsonExpression(expr: Expression,
                                  exprsIndexMap: Map[ExprId, Int],
                                  returnDatatype: DataType,
                                  func: (Expression, Map[ExprId, Int], DataType) => JsonObject): JsonObject = {
    rewriteJsonFunc(expr, exprsIndexMap, returnDatatype, func)
  }

  def preReplaceSparkPlanFunc(plan: SparkPlan, func: SparkPlan => SparkPlan): SparkPlan = {
    preReplacePlanFunc(plan, func)
  }

  def postReplaceSparkPlanFunc(plan: SparkPlan, func: SparkPlan => SparkPlan): SparkPlan = {
    postReplacePlanFunc(plan, func)
  }

  def injectRule(extensions: SparkSessionExtensions): Unit = {
    injectRuleFunc(extensions)
  }
}
