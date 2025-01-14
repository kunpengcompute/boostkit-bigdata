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

package org.apache.spark.sql.catalyst.optimizer

import com.huawei.boostkit.spark.ColumnarPluginConfig

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

import scala.collection.mutable.ArrayBuffer

/**
 * Rewrite the SelfJoin resulting in duplicate rows used for IN predicate to aggregation.
 * For IN predicate, duplicate rows does not have any value. It will be overhead.
 * <p>
 * Ex: TPCDS Q95: following CTE is used only in IN predicates for only one column comparison
 * ({@code ws_order_number}). This results in exponential increase in Joined rows with too many
 * duplicate rows.
 * <pre>
 * WITH ws_wh AS
 * (
 *        SELECT ws1.ws_order_number
 *        FROM   web_sales ws1,
 *               web_sales ws2
 *        WHERE  ws1.ws_order_number = ws2.ws_order_number
 *        AND    ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
 * </pre>
 * <p>
 * Could be optimized as below:
 * <pre>
 * WITH ws_wh AS
 *     (SELECT ws_order_number
 *       FROM  web_sales
 *       GROUP BY ws_order_number
 *       HAVING COUNT(DISTINCT ws_warehouse_sk) > 1)
 * </pre>
 * Optimized CTE scans table only once and results in unique rows.
 */
object RewriteSelfJoinInInPredicate extends Rule[LogicalPlan] with PredicateHelper {

  def rewrite(plan: LogicalPlan): LogicalPlan =
    plan.transform {
      case f: Filter =>
        f transformExpressions {
          case in @ InSubquery(_, listQuery @ ListQueryShim(Project(projectList,
          Join(left, right, Inner, Some(joinCond), _)), _, _, _, _))
            if left.canonicalized ne right.canonicalized =>
            val attrMapping = AttributeMap(right.output.zip(left.output))
            val subCondExprs = splitConjunctivePredicates(joinCond transform {
              case attr: Attribute => attrMapping.getOrElse(attr, attr)
            })
            val equalJoinAttrs = ArrayBuffer[Attribute]()
            val nonEqualJoinAttrs = ArrayBuffer[NamedExpression]()
            var hasComplexCond = false
            subCondExprs map {
              case EqualTo(attr1: Attribute, attr2: Attribute) if attr1.semanticEquals(attr2) =>
                equalJoinAttrs += attr1

              case Not(EqualTo(attr1: Attribute, attr2: Attribute))
                if attr1.semanticEquals(attr2) =>
                nonEqualJoinAttrs +=
                  Alias(Count(attr1).toAggregateExpression(), "cnt_" + attr1.name)()

              case _ => hasComplexCond = true
            }

            val newProjectList = projectList map {
              case attr: Attribute => attrMapping.getOrElse(attr, attr)
              case Alias(attr: Attribute, name) => Alias(attrMapping.getOrElse(attr, attr), name)()
              case attr => attr
            }

            if (!hasComplexCond &&
              AttributeSet(newProjectList).subsetOf(AttributeSet(equalJoinAttrs))) {
              val aggPlan =
                Aggregate(equalJoinAttrs, (equalJoinAttrs ++ nonEqualJoinAttrs), left)
              val filterPlan =
                if (nonEqualJoinAttrs.isEmpty) {
                  Project(newProjectList, aggPlan)
                } else {
                  Project(newProjectList,
                    Filter(buildBalancedPredicate(nonEqualJoinAttrs.map(
                      expr => GreaterThan(expr.toAttribute, Literal(0L))), And),
                      aggPlan
                    )
                  )
                }

              in.copy(query = listQuery.copy(plan = filterPlan))
            } else {
              in
            }
        }
    }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!ColumnarPluginConfig.getSessionConf.enableRewriteSelfJoinInInPredicate) {
      plan
    } else {
      rewrite(plan)
    }
  }

  override protected def buildBalancedPredicate(
                              expressions: Seq[Expression], op: (Expression, Expression) => Expression): Expression = {
    assert(expressions.nonEmpty)
    var currentResult = expressions
    while (currentResult.size != 1) {
      var i = 0
      val nextResult = new Array[Expression](currentResult.size / 2 + currentResult.size % 2)
      while (i < currentResult.size) {
        nextResult(i / 2) = if (i + 1 == currentResult.size) {
          currentResult(i)
        } else {
          op(currentResult(i), currentResult(i + 1))
        }
        i += 2
      }
      currentResult = nextResult
    }
    currentResult.head
  }
}