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

package org.apache.spark.sql.execution.window;

import com.huawei.boostkit.spark.ColumnarPluginConfig
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FilterExec, SortExec, SparkPlan, TopNSortExec}

object TopNPushDownForWindow extends Rule[SparkPlan] with PredicateHelper {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!ColumnarPluginConfig.getConf.topNPushDownForWindowEnable) {
      return plan
    }

    plan.transform {
      case f @ FilterExec(condition,
        w @ WindowExec(Seq(windowExpression), _, orderSpec, sort: SortExec))
        if orderSpec.nonEmpty && isTopNExpression(windowExpression) =>
        var topn = Int.MaxValue
        val nonTopNConditions = splitConjunctivePredicates(condition).filter {
          case LessThan(e: NamedExpression, IntegerLiteral(n))
            if e.exprId == windowExpression.exprId =>
            topn = Math.min(topn, n - 1)
            false
          case LessThanOrEqual(e: NamedExpression, IntegerLiteral(n))
            if e.exprId == windowExpression.exprId =>
            topn = Math.min(topn, n)
            false
          case GreaterThan(IntegerLiteral(n), e: NamedExpression)
            if e.exprId == windowExpression.exprId =>
            topn = Math.min(topn, n - 1)
            false
          case GreaterThanOrEqual(IntegerLiteral(n), e: NamedExpression)
            if e.exprId == windowExpression.exprId =>
            topn = Math.min(topn, n)
            false
          case EqualTo(e: NamedExpression, IntegerLiteral(n))
            if n == 1 && e.exprId == windowExpression.exprId =>
            topn = 1
            false
          case EqualTo(IntegerLiteral(n), e: NamedExpression)
            if n == 1 && e.exprId == windowExpression.exprId =>
            topn = 1
            false
          case _ => true
        }

        // topn <= SQLConf.get.topNPushDownForWindowThreshold 100.
        if (topn> 0 && topn <= ColumnarPluginConfig.getConf.topNPushDownForWindowThreshold) {
          val strictTopN = isStrictTopN(windowExpression)
          val topNSortExec = TopNSortExec(
            topn, strictTopN, w.partitionSpec, w.orderSpec, sort.global, sort.child)
          val newCondition = if (nonTopNConditions.isEmpty) {
            Literal.TrueLiteral
          } else {
            nonTopNConditions.reduce(And)
          }
          FilterExec(newCondition, w.copy(child = topNSortExec))
        } else {
          f
        }
    }
  }

  private def isTopNExpression(e: Expression): Boolean = e match {
    case Alias(child, _) => isTopNExpression(child)
    case WindowExpression(windowFunction, _)
      if windowFunction.isInstanceOf[Rank] => true
    case _ => false
  }

  private def isStrictTopN(e: Expression): Boolean = e match {
    case Alias(child, _) => isStrictTopN(child)
    case WindowExpression(windowFunction, _) => windowFunction.isInstanceOf[RowNumber]
  }
}
