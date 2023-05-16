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

import org.apache.spark.sql.NdpUtils.stripEnd


import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.util.CharVarcharUtils.getRawTypeString
import org.apache.spark.sql.execution.{FilterExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String



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
  val allFilterExecInfo = new ListBuffer[FilterExec]()
  var zkRate: Double = 1.0

  def partialPushDownFilter(filter: FilterExec): Unit = {
    allFilterExecInfo += filter
  }

  def partialPushDownFilterList(filters: ListBuffer[FilterExec]): Unit = {
    allFilterExecInfo ++= filters
  }

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

  def pushZkRate(pRate: Double): Unit = {
      zkRate = pRate
  }
}

object NdpSupport {
  def toAggExecution(agg: BaseAggregateExec): AggExeInfo = {
    AggExeInfo(agg.aggregateExpressions.map(_.aggregateFunction),
      agg.groupingExpressions, agg.output)
  }

  def filterStripEnd(filter: Expression): Expression = {
    val f = filter.transform {
      case greaterThan @ GreaterThan(left: Attribute, right: Literal) if isCharType(left) =>
        GreaterThan(left, Literal(UTF8String.fromString(stripEnd(right.value.toString, " ")), right.dataType))
      case greaterThan @ GreaterThan(left: Literal, right: Attribute) if isCharType(right) =>
        GreaterThan(Literal(UTF8String.fromString(stripEnd(left.value.toString, " ")), left.dataType), right)
      case greaterThanOrEqual @ GreaterThanOrEqual(left: Attribute, right: Literal) if isCharType(left) =>
        GreaterThanOrEqual(left, Literal(UTF8String.fromString(stripEnd(right.value.toString, " ")), right.dataType))
      case greaterThanOrEqual @ GreaterThanOrEqual(left: Literal, right: Attribute) if isCharType(right) =>
        GreaterThanOrEqual(Literal(UTF8String.fromString(stripEnd(left.value.toString, " ")), left.dataType), right)
      case lessThan @ LessThan(left: Attribute, right: Literal) if isCharType(left) =>
        LessThan(left, Literal(UTF8String.fromString(stripEnd(right.value.toString, " ")), right.dataType))
      case lessThan @ LessThan(left: Literal, right: Attribute) if isCharType(right) =>
        LessThan(Literal(UTF8String.fromString(stripEnd(left.value.toString, " ")), left.dataType), right)
      case lessThanOrEqual @ LessThanOrEqual(left: Attribute, right: Literal) if isCharType(left) =>
        LessThanOrEqual(left, Literal(UTF8String.fromString(stripEnd(right.value.toString, " ")), right.dataType))
      case lessThanOrEqual @ LessThanOrEqual(left: Literal, right: Attribute) if isCharType(right) =>
        LessThanOrEqual(Literal(UTF8String.fromString(stripEnd(left.value.toString, " ")), left.dataType), right)
      case equalto @ EqualTo(left: Attribute, right: Literal) if isCharType(left) =>
        EqualTo(left, Literal(UTF8String.fromString(stripEnd(right.value.toString, " ")), right.dataType))
      case equalto @ EqualTo(left: Literal, right: Attribute) if isCharType(right) =>
        EqualTo(Literal(UTF8String.fromString(stripEnd(left.value.toString, " ")), left.dataType), right)
      case in @ In(value: Attribute, list: Seq[Literal]) if isCharType(value) =>
        In(value, list.map(literal => Literal(UTF8String.fromString(stripEnd(literal.value.toString, " ")), literal.dataType)))
    }
    f
  }

  def isCharType(value: Attribute): Boolean = {
    value.dataType.isInstanceOf[StringType] && getRawTypeString(value.metadata).isDefined && getRawTypeString(value.metadata).get.startsWith("char")
  }
}