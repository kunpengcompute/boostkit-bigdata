/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
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

package org.apache.spark.sql.execution.ndp

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec

// filter in aggregate could be push down through aggregate, separate filter and aggregate
case class AggExeInfo(
    aggregateExpressions: Seq[AggregateFunction],
    groupingExpressions: Seq[NamedExpression],
    output: Seq[Attribute])

// include aggregate filter
case class FilterExeInfo(filter: Expression, output: Seq[Attribute])

case class LimitExeInfo(limit: Int)

case class PushDownInfo(
    filterExecutions: Seq[FilterExeInfo],
    aggExecutions: Seq[AggExeInfo],
    limitExecution: Option[LimitExeInfo],
    fpuHosts: scala.collection.Map[String, String])

trait NdpSupport extends SparkPlan {

  val filterExeInfos = new ListBuffer[FilterExeInfo]()
  val aggExeInfos = new ListBuffer[AggExeInfo]()
  var limitExeInfo: Option[LimitExeInfo] = None
  var fpuHosts: scala.collection.Map[String, String] = _

  def pushDownFilter(filter: FilterExeInfo): Unit = {
    filterExeInfos += filter
  }

  def pushDownAgg(agg: AggExeInfo): Unit = {
    aggExeInfos += agg
  }

  def pushDownLimit(limit: LimitExeInfo): Unit = {
    limitExeInfo = Some(limit)
  }

  def pushDown(n: NdpSupport): Unit = {
    filterExeInfos ++= n.filterExeInfos
    aggExeInfos ++= n.aggExeInfos
    limitExeInfo = n.limitExeInfo
  }

  def fpuHosts(fpu: scala.collection.Map[String, String]): Unit = {
    fpuHosts = fpu
  }

  def ndpOperators: PushDownInfo = {
    PushDownInfo(filterExeInfos, aggExeInfos, limitExeInfo, fpuHosts)
  }

  def isPushDown: Boolean = filterExeInfos.nonEmpty ||
    aggExeInfos.nonEmpty ||
    limitExeInfo.nonEmpty
}

object NdpSupport {
  def toAggExecution(agg: BaseAggregateExec): AggExeInfo = {
    AggExeInfo(agg.aggregateExpressions.map(_.aggregateFunction),
      agg.groupingExpressions, agg.output)
  }
}
