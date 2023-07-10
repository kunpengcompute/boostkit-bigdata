/*
 * Copyright (C) 2022-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

import scala.annotation.tailrec
import scala.collection.mutable

import com.huawei.boostkit.spark.ColumnarPluginConfig

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualNullSafe, EqualTo, Expression, IsNotNull, PredicateHelper}
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

/**
 * Firstly, Heuristic reorder join need to execute small joins with filters
 * , which can reduce intermediate results
 */
object HeuristicJoinReorder extends Rule[LogicalPlan]
  with PredicateHelper with JoinSelectionHelper {

  /**
   * Join a list of plans together and push down the conditions into them.
   * The joined plan are picked from left to right, thus the final result is a left-deep tree.
   *
   * @param input      a list of LogicalPlans to inner join and the type of inner join.
   * @param conditions a list of condition for join.
   */
  @tailrec
  final def createReorderJoin(input: Seq[(LogicalPlan, InnerLike)], conditions: Seq[Expression])
  : LogicalPlan = {
    assert(input.size >= 2)
    if (input.size == 2) {
      val (joinConditions, others) = conditions.partition(canEvaluateWithinJoin)
      val ((leftPlan, leftJoinType), (rightPlan, rightJoinType)) = (input(0), input(1))
      val innerJoinType = (leftJoinType, rightJoinType) match {
        case (Inner, Inner) => Inner
        case (_, _) => Cross
      }
      // Set the join node ordered so that we don't need to transform them again.
      val orderJoin = OrderedJoin(leftPlan, rightPlan, innerJoinType, joinConditions.reduceLeftOption(And))
      if (others.nonEmpty) {
        Filter(others.reduceLeft(And), orderJoin)
      } else {
        orderJoin
      }
    } else {
      val (left, _) :: rest = input.toList
      val candidates = rest.filter { planJoinPair =>
        val plan = planJoinPair._1
        // 1. it has join conditions with the left node
        // 2. it has a filter
        // 3. it can be broadcast
        val isEqualJoinCondition = conditions.flatMap {
          case EqualTo(l, r) if l.references.isEmpty || r.references.isEmpty => None
          case EqualNullSafe(l, r) if l.references.isEmpty || r.references.isEmpty => None
          case e@EqualTo(l, r) if canEvaluate(l, left) && canEvaluate(r, plan) => Some(e)
          case e@EqualTo(l, r) if canEvaluate(l, plan) && canEvaluate(r, left) => Some(e)
          case e@EqualNullSafe(l, r) if canEvaluate(l, left) && canEvaluate(r, plan) => Some(e)
          case e@EqualNullSafe(l, r) if canEvaluate(l, plan) && canEvaluate(r, left) => Some(e)
          case _ => None
        }.nonEmpty

        val hasFilter = plan match {
          case f: Filter if hasValuableCondition(f.condition) => true
          case Project(_, f: Filter) if hasValuableCondition(f.condition) => true
          case _ => false
        }

        isEqualJoinCondition && hasFilter
      }
      val (right, innerJoinType) = if (candidates.nonEmpty) {
        candidates.minBy(_._1.stats.sizeInBytes)
      } else {
        rest.head
      }

      val joinedRefs = left.outputSet ++ right.outputSet
      val selectedJoinConditions = mutable.HashSet.empty[Expression]
      val (joinConditions, others) = conditions.partition { e =>
        // If there are semantically equal conditions, they should come from two different joins.
        // So we should not put them into one join.
        if (!selectedJoinConditions.contains(e.canonicalized) && e.references.subsetOf(joinedRefs)
          && canEvaluateWithinJoin(e)) {
          selectedJoinConditions.add(e.canonicalized)
          true
        } else {
          false
        }
      }
      // Set the join node ordered so that we don't need to transform them again.
      val joined = OrderedJoin(left, right, innerJoinType, joinConditions.reduceLeftOption(And))

      // should not have reference to same logical plan
      createReorderJoin(Seq((joined, Inner)) ++ rest.filterNot(_._1 eq right), others)
    }
  }

  private def hasValuableCondition(condition: Expression): Boolean = {
    val conditions = splitConjunctivePredicates(condition)
    !conditions.forall(_.isInstanceOf[IsNotNull])
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (ColumnarPluginConfig.getSessionConf.enableHeuristicJoinReorder) {
      val newPlan = plan.transform {
        case p@ExtractFiltersAndInnerJoinsByIgnoreProjects(input, conditions)
          if input.size > 2 && conditions.nonEmpty =>
          val reordered = createReorderJoin(input, conditions)
          if (p.sameOutput(reordered)) {
            reordered
          } else {
            // Reordering the joins have changed the order of the columns.
            // Inject a projection to make sure we restore to the expected ordering.
            Project(p.output, reordered)
          }
      }

      // After reordering is finished, convert OrderedJoin back to Join
      val result = newPlan.transformDown {
        case OrderedJoin(left, right, jt, cond) => Join(left, right, jt, cond, JoinHint.NONE)
      }
      if (!result.resolved) {
        // In some special cases related to subqueries, we find that after reordering,
        val comparedPlans = sideBySide(plan.treeString, result.treeString).mkString("\n")
        logWarning("The structural integrity of the plan is broken, falling back to the " +
          s"original plan. == Comparing two plans ===\n$comparedPlans")
        plan
      } else {
        result
      }
    } else {
      plan
    }
  }
}

/**
 * This is different from [[ExtractFiltersAndInnerJoins]] in that it can collect filters and
 * inner joins by ignoring projects on top of joins, which are produced by column pruning.
 */
private object ExtractFiltersAndInnerJoinsByIgnoreProjects extends PredicateHelper {

  /**
   * Flatten all inner joins, which are next to each other.
   * Return a list of logical plans to be joined with a boolean for each plan indicating if it
   * was involved in an explicit cross join. Also returns the entire list of join conditions for
   * the left-deep tree.
   */
  def flattenJoin(plan: LogicalPlan, parentJoinType: InnerLike = Inner)
  : (Seq[(LogicalPlan, InnerLike)], Seq[Expression]) = plan match {
    case Join(left, right, joinType: InnerLike, cond, hint) if hint == JoinHint.NONE =>
      val (plans, conditions) = flattenJoin(left, joinType)
      (plans ++ Seq((right, joinType)), conditions ++
        cond.toSeq.flatMap(splitConjunctivePredicates))
    case Filter(filterCondition, j@Join(_, _, _: InnerLike, _, hint)) if hint == JoinHint.NONE =>
      val (plans, conditions) = flattenJoin(j)
      (plans, conditions ++ splitConjunctivePredicates(filterCondition))
    case Project(projectList, child)
      if projectList.forall(_.isInstanceOf[Attribute]) => flattenJoin(child)

    case _ => (Seq((plan, parentJoinType)), Seq.empty)
  }

  def unapply(plan: LogicalPlan): Option[(Seq[(LogicalPlan, InnerLike)], Seq[Expression])]
  = plan match {
    case f@Filter(_, Join(_, _, _: InnerLike, _, _)) =>
      Some(flattenJoin(f))
    case j@Join(_, _, _, _, hint) if hint == JoinHint.NONE =>
      Some(flattenJoin(j))
    case _ => None
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