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
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{And, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.planning.ExtractFiltersAndInnerJoins
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.util.sideBySide

/**
 * Move all cartesian products to the root of the plan
 */
object DelayCartesianProduct extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * Extract cliques from the input plans.
   * A cliques is a sub-tree(sub-plan) which doesn't have any join with other sub-plan.
   * The input plans are picked from left to right
   * , until we can't find join condition in the remaining plans.
   * The same logic is applied to the remaining plans, until all plans are picked.
   * This function can produce a left-deep tree or a bushy tree.
   *
   * @param input      a list of LogicalPlans to inner join and the type of inner join.
   * @param conditions a list of condition for join.
   */
  private def extractCliques(input: Seq[(LogicalPlan, InnerLike)], conditions: Seq[Expression])
  : Seq[(LogicalPlan, InnerLike)] = {
    if (input.size == 1) {
      input
    } else {
      val (leftPlan, leftInnerJoinType) :: linearSeq = input
      // discover the initial join that contains at least one join condition
      val conditionalOption = linearSeq.find { planJoinPair =>
        val plan = planJoinPair._1
        val refs = leftPlan.outputSet ++ plan.outputSet
        conditions
          .filterNot(l => l.references.nonEmpty && canEvaluate(l, leftPlan))
          .filterNot(r => r.references.nonEmpty && canEvaluate(r, plan))
          .exists(_.references.subsetOf(refs))
      }

      if (conditionalOption.isEmpty) {
        Seq((leftPlan, leftInnerJoinType)) ++ extractCliques(linearSeq, conditions)
      } else {
        val (rightPlan, rightInnerJoinType) = conditionalOption.get

        val joinedRefs = leftPlan.outputSet ++ rightPlan.outputSet
        val (joinConditions, otherConditions) = conditions.partition(
          e => e.references.subsetOf(joinedRefs) && canEvaluateWithinJoin(e))
        val joined = Join(leftPlan, rightPlan, rightInnerJoinType,
          joinConditions.reduceLeftOption(And), JoinHint.NONE)

        // must not make reference to the same logical plan
        extractCliques(Seq((joined, Inner))
          ++ linearSeq.filterNot(_._1 eq rightPlan), otherConditions)
      }
    }
  }

  /**
   * Link cliques by cartesian product
   *
   * @param input
   * @return
   */
  private def linkCliques(input: Seq[(LogicalPlan, InnerLike)])
  : LogicalPlan = {
    if (input.length == 1) {
      input.head._1
    } else if (input.length == 2) {
      val ((left, innerJoinType1), (right, innerJoinType2)) = (input(0), input(1))
      val joinType = resetJoinType(innerJoinType1, innerJoinType2)
      Join(left, right, joinType, None, JoinHint.NONE)
    } else {
      val (left, innerJoinType1) :: (right, innerJoinType2) :: rest = input
      val joinType = resetJoinType(innerJoinType1, innerJoinType2)
      linkCliques(Seq((Join(left, right, joinType, None, JoinHint.NONE), joinType)) ++ rest)
    }
  }

  /**
   * This is to reset the join type before reordering.
   *
   * @param leftJoinType
   * @param rightJoinType
   * @return
   */
  private def resetJoinType(leftJoinType: InnerLike, rightJoinType: InnerLike): InnerLike = {
    (leftJoinType, rightJoinType) match {
      case (_, Cross) | (Cross, _) => Cross
      case _ => Inner
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!ColumnarPluginConfig.getSessionConf.enableDelayCartesianProduct) {
      return plan
    }

    // Reorder joins only when there are cartesian products.
    var existCartesianProduct = false
    plan foreach {
      case Join(_, _, _: InnerLike, None, _) => existCartesianProduct = true
      case _ =>
    }

    if (existCartesianProduct) {
      plan.transform {
        case originalPlan@ExtractFiltersAndInnerJoins(input, conditions)
          if input.size > 2 && conditions.nonEmpty =>
          val cliques = extractCliques(input, conditions)
          val reorderedPlan = linkCliques(cliques)

          reorderedPlan match {
            // Generate a bushy tree after reordering.
            case ExtractFiltersAndInnerJoinsForBushy(_, joinConditions) =>
              val primalConditions = conditions.flatMap(splitConjunctivePredicates)
              val reorderedConditions = joinConditions.flatMap(splitConjunctivePredicates).toSet
              val missingConditions = primalConditions.filterNot(reorderedConditions.contains)
              if (missingConditions.nonEmpty) {
                val comparedPlans =
                  sideBySide(originalPlan.treeString, reorderedPlan.treeString).mkString("\n")
                logWarning("There are missing conditions after reordering, falling back to the "
                  + s"original plan. == Comparing two plans ===\n$comparedPlans")
                originalPlan
              } else {
                reorderedPlan
              }
            case _ => throw new AnalysisException(
              s"There is no join node in the plan, this should not happen: $reorderedPlan")
          }
      }
    } else {
      plan
    }
  }
}



private object ExtractFiltersAndInnerJoinsForBushy extends PredicateHelper {

  /**
   * This function works for both left-deep and bushy trees.
   *
   * @param plan
   * @param parentJoinType
   * @return
   */
  def flattenJoin(plan: LogicalPlan, parentJoinType: InnerLike = Inner)
  : (Seq[(LogicalPlan, InnerLike)], Seq[Expression]) = plan match {
    case Join(left, right, joinType: InnerLike, cond, _) =>
      val (lPlans, lConds) = flattenJoin(left, joinType)
      val (rPlans, rConds) = flattenJoin(right, joinType)
      (lPlans ++ rPlans, lConds ++ rConds ++ cond.toSeq)

    case Filter(filterCondition, j@Join(_, _, _: InnerLike, _, _)) =>
      val (plans, conditions) = flattenJoin(j)
      (plans, conditions ++ splitConjunctivePredicates(filterCondition))

    case _ => (Seq((plan, parentJoinType)), Seq())
  }

  def unapply(plan: LogicalPlan): Option[(Seq[(LogicalPlan, InnerLike)], Seq[Expression])] = {
    plan match {
      case f@Filter(_, Join(_, _, _: InnerLike, _, _)) =>
        Some(flattenJoin(f))
      case j@Join(_, _, _, _, _) =>
        Some(flattenJoin(j))
      case _ => None
    }
  }
}