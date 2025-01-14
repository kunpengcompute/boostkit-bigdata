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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Partial}
import org.apache.spark.sql.catalyst.optimizer.NormalizeFloatingNumbers
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LeafNode, Statistics}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.ShimUtil
import org.apache.spark.util.Utils

object ExtendedAggUtils {
  def normalizeGroupingExpressions(groupingExpressions: Seq[NamedExpression]) = {
    groupingExpressions.map { e =>
      NormalizeFloatingNumbers.normalize(e) match {
        case n: NamedExpression => n
        case other => Alias(other, e.name)(exprId = e.exprId)
      }
    }
  }

  def planPartialAggregateWithoutDistinct(
                                           groupingExpressions: Seq[NamedExpression],
                                           aggregateExpressions: Seq[AggregateExpression],
                                           resultExpressions: Seq[NamedExpression],
                                           child: SparkPlan): SparkPlan = {
    val completeAggregateExpressions = aggregateExpressions.map(_.copy(mode = Complete))
    createAggregate(
      requiredChildDistributionExpressions = None,
      groupingExpressions = groupingExpressions.map(_.toAttribute),
      aggregateExpressions = completeAggregateExpressions,
      aggregateAttributes = completeAggregateExpressions.map(_.resultAttribute),
      initialInputBufferOffset = groupingExpressions.length,
      resultExpressions = resultExpressions,
      child = child)
  }

  private def createAggregate(
                               requiredChildDistributionExpressions: Option[Seq[Expression]] = None,
                               isStreaming: Boolean = false,
                               groupingExpressions: Seq[NamedExpression] = Nil,
                               aggregateExpressions: Seq[AggregateExpression] = Nil,
                               aggregateAttributes: Seq[Attribute] = Nil,
                               initialInputBufferOffset: Int = 0,
                               resultExpressions: Seq[NamedExpression] = Nil,
                               child: SparkPlan): SparkPlan = {
    val useHash = ShimUtil.supportsHashAggregate(
      aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes))

    if (useHash) {
      HashAggregateExec(
        requiredChildDistributionExpressions = requiredChildDistributionExpressions,
        isStreaming = isStreaming,
        numShufflePartitions = None,
        groupingExpressions = groupingExpressions,
        aggregateExpressions = mayRemoveAggFilters(aggregateExpressions),
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = initialInputBufferOffset,
        resultExpressions = resultExpressions,
        child = child)
    } else {
      val objectHashEnabled = child.conf.useObjectHashAggregation
      val useObjectHash = ShimUtil.supportsObjectHashAggregate(aggregateExpressions)

      if (objectHashEnabled && useObjectHash) {
        ObjectHashAggregateExec(
          requiredChildDistributionExpressions = requiredChildDistributionExpressions,
          isStreaming = isStreaming,
          numShufflePartitions = None,
          groupingExpressions = groupingExpressions,
          aggregateExpressions = mayRemoveAggFilters(aggregateExpressions),
          aggregateAttributes = aggregateAttributes,
          initialInputBufferOffset = initialInputBufferOffset,
          resultExpressions = resultExpressions,
          child = child)
      } else {
        SortAggregateExec(
          requiredChildDistributionExpressions = requiredChildDistributionExpressions,
          isStreaming = isStreaming,
          numShufflePartitions = None,
          groupingExpressions = groupingExpressions,
          aggregateExpressions = mayRemoveAggFilters(aggregateExpressions),
          aggregateAttributes = aggregateAttributes,
          initialInputBufferOffset = initialInputBufferOffset,
          resultExpressions = resultExpressions,
          child = child)
      }
    }
  }

  private def mayRemoveAggFilters(exprs: Seq[AggregateExpression]): Seq[AggregateExpression] = {
    exprs.map { ae =>
      if (ae.filter.isDefined) {
        ae.mode match {
          case Partial | Complete => ae
          case _ => ae.copy(filter = None)
        }
      } else {
        ae
      }
    }
  }
}

case class DummyLogicalPlan() extends LeafNode {
  override def output: Seq[Attribute] = Nil

  override def computeStats(): Statistics = throw new UnsupportedOperationException
}