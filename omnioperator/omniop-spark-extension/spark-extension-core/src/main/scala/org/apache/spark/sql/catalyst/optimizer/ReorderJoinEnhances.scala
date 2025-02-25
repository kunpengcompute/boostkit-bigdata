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

import scala.annotation.tailrec
import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ExtractFiltersAndInnerJoins
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.TreePattern._

/**
 * Reorder the joins and push all the conditions into join, so that the bottom ones have at least
 * one condition.
 *
 * The order of joins will not be changed if all of them already have at least one condition.
 *
 * If star schema detection is enabled, reorder the star join plans based on heuristics.
 */
object ReorderJoinEnhances extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Join a list of plans together and push down the conditions into them.
   *
   * The joined plan are picked from left to right, prefer those has at least one join condition.
   *
   * @param input a list of LogicalPlans to inner join and the type of inner join.
   * @param conditions a list of condition for join.
   */
  @tailrec
  final def createOrderedJoin(
      input: Seq[(LogicalPlan, InnerLike)],
      conditions: Seq[Expression]): LogicalPlan = {
    assert(input.size >= 2)
    if (input.size == 2) {
      val (joinConditions, others) = conditions.partition(canEvaluateWithinJoin)
      val ((left, leftJoinType), (right, rightJoinType)) = (input(0), input(1))
      val innerJoinType = (leftJoinType, rightJoinType) match {
        case (Inner, Inner) => Inner
        case (_, _) => Cross
      }
      val join = Join(left, right, innerJoinType,
        joinConditions.reduceLeftOption(And), JoinHint.NONE)
      if (others.nonEmpty) {
        Filter(others.reduceLeft(And), join)
      } else {
        join
      }
    } else {
      val (left, _) :: rest = input.toList
      // collect all possible joins
      val candidateJoins = rest.filter { planJoinPair =>
        val candidateRight = planJoinPair._1

        // check two plan have equi-join
        val joinKeysForTwoPlans = conditions.flatMap {
          case EqualTo(l, r) if l.references.isEmpty || r.references.isEmpty => None
          case EqualTo(l, r) if canEvaluate(l, left) && canEvaluate(r, candidateRight) => Some((l, r))
          case EqualTo(l, r) if canEvaluate(l, candidateRight) && canEvaluate(r, left) => Some((r, l))
          // Replace null with default value for joining key, then those rows with null in it could
          // be joined together
          case EqualNullSafe(l, r) if l.references.isEmpty || r.references.isEmpty => None
          case EqualNullSafe(l, r) if canEvaluate(l, left) && canEvaluate(r, candidateRight) =>
            Seq((Coalesce(Seq(l, Literal.default(l.dataType))),
              Coalesce(Seq(r, Literal.default(r.dataType)))),
              (IsNull(l), IsNull(r))
            )  // (coalesce(l, default) = coalesce(r, default)) and (isnull(l) = isnull(r))
          case EqualNullSafe(l, r) if canEvaluate(l, candidateRight) && canEvaluate(r, left) =>
            Seq((Coalesce(Seq(r, Literal.default(r.dataType))),
              Coalesce(Seq(l, Literal.default(l.dataType)))),
              (IsNull(r), IsNull(l))
            )  // Same as above with left/right reversed.
          case _ => None
        }

        // check candidateRight have filter condition
        val filterMatched = candidateRight match {
          case Filter(filterCondition, _) if checkFilterCondition(filterCondition) => true
          case Project(_, Filter(filterCondition, _)) if checkFilterCondition(filterCondition) => true
          case _ => false
        }
        joinKeysForTwoPlans.nonEmpty && filterMatched
      }
      // pick the min size one if candidateJoins left
      val (right, innerJoinType) = if (candidateJoins.nonEmpty) {
        candidateJoins.minBy(_._1.stats.sizeInBytes)
      } else {
        rest.head
      }

      val joinedRefs = left.outputSet ++ right.outputSet
      val dupJoinConditionChecks = mutable.HashSet.empty[Expression]
      val (joinConditions, others) = conditions.partition { e =>
        if (!dupJoinConditionChecks.contains(e.canonicalized) && e.references.subsetOf(joinedRefs)
          && canEvaluateWithinJoin(e)) {
          dupJoinConditionChecks.add(e.canonicalized)
          true
        } else {
          false
        }
      }

      val joined = Join(left, right, innerJoinType,
        joinConditions.reduceLeftOption(And), JoinHint.NONE)
      // should not have reference to same logical plan
      createOrderedJoin(Seq((joined, Inner)) ++ rest.filterNot(_._1 eq right), others)
    }
  }


  private def checkFilterCondition(condition: Expression): Boolean = {
    val conditions = splitConjunctivePredicates(condition)
    conf.constraintPropagationEnabled && !conditions.forall(_.isInstanceOf[IsNotNull])
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    ColumnarPluginConfig.getSessionConf.enableJoinReorderEnhance &&
    _.containsPattern(INNER_LIKE_JOIN)) {
    case p @ ExtractFiltersAndInnerJoinsEnhances(input, conditions)
        if input.size > 2 && conditions.nonEmpty =>
      val reordered = if (conf.starSchemaDetection && !conf.cboEnabled) {
        val starJoinPlan = StarSchemaDetection.reorderStarJoins(input, conditions)
        if (starJoinPlan.nonEmpty) {
          val rest = input.filterNot(starJoinPlan.contains(_))
          createOrderedJoin(starJoinPlan ++ rest, conditions)
        } else {
          createOrderedJoin(input, conditions)
        }
      } else {
        createOrderedJoin(input, conditions)
      }

      if (p.sameOutput(reordered)) {
        reordered
      } else {
        // Reordering the joins have changed the order of the columns.
        // Inject a projection to make sure we restore to the expected ordering.
        Project(p.output, reordered)
      }
  }
}

/**
 * A pattern that collects the filter and inner joins.
 *
 *          Filter
 *            |
 *        inner Join
 *          /    \            ---->      (Seq(plan0, plan1, plan2), conditions)
 *      Filter   plan2
 *        |
 *  inner join
 *      /    \
 *   plan0    plan1
 *
 * Note: This pattern currently only works for left-deep trees.
 */
object ExtractFiltersAndInnerJoinsEnhances extends PredicateHelper {

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
    case Filter(filterCondition, j @ Join(_, _, _: InnerLike, _, hint)) if hint == JoinHint.NONE =>
      val (plans, conditions) = flattenJoin(j)
      (plans, conditions ++ splitConjunctivePredicates(filterCondition))
    case Project(projectList, child)
      if projectList.forall(_.isInstanceOf[Attribute]) => flattenJoin(child)

    case _ => (Seq((plan, parentJoinType)), Seq.empty)
  }

  def unapply(plan: LogicalPlan)
  : Option[(Seq[(LogicalPlan, InnerLike)], Seq[Expression])]
  = plan match {
    case f @ Filter(filterCondition, j @ Join(_, _, joinType: InnerLike, _, hint))
      if hint == JoinHint.NONE =>
      Some(flattenJoin(f))
    case j @ Join(_, _, joinType, _, hint) if hint == JoinHint.NONE =>
      Some(flattenJoin(j))
    case _ => None
  }
}
