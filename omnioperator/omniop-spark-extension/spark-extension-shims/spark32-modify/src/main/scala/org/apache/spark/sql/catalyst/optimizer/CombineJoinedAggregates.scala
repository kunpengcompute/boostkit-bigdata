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

package org.apache.spark.sql.catalyst.optimizer
import com.huawei.boostkit.spark.ColumnarPluginConfig
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
 * This rule eliminates the [[Join]] if all the join side are [[Aggregate]]s by combine these
 * [[Aggregate]]s. This rule also support the nested [[Join]], as long as all the join sides for
 * every [[Join]] are [[Aggregate]]s.
 *
 * Note: this rule doesn't support following cases:
 * 1. The [[Aggregate]]s to be merged if at least one of them does not have a predicate or
 *    has low predicate selectivity.
 * 2. The upstream node of these [[Aggregate]]s to be merged exists [[Join]].
 */
object CombineJoinedAggregates extends Rule[LogicalPlan] with MergeScalarSubqueriesHelper {

  private def isSupportedJoinType(joinType: JoinType): Boolean =
    Seq(Inner, Cross, LeftOuter, RightOuter, FullOuter).contains(joinType)

  private def maxTreeNodeNumOfPredicate: Int = 10

  private def isCheapPredicate(e: Expression): Boolean = {
    !e.containsAnyPattern(PYTHON_UDF, SCALA_UDF, JSON_TO_STRUCT, LIKE_FAMLIY, DYNAMIC_PRUNING_SUBQUERY
      , DYNAMIC_PRUNING_EXPRESSION, HIGH_ORDER_FUNCTION, IN_SUBQUERY, IN, INSET, EXISTS_SUBQUERY)
  }

private def checkCondition(leftCondition: Expression, rightCondition: Expression): Boolean = {
    val normalizedLeft = normalizeExpression(leftCondition)
    val normalizedRight = normalizeExpression(rightCondition)
    if (normalizedLeft.isDefined && normalizedRight.isDefined) {
      (normalizedLeft.get, normalizedRight.get) match {
        case (a GreaterThan b, c LessThan d) if a.semanticEquals(c) =>
          isGreaterOrEqualTo(b, d, a.dataType)
        case (a LessThan b, c GreaterThan d) if a.semanticEquals(c) =>
          isGreaterOrEqualTo(d, b, a.dataType)
        case (a GreaterThanOrEqual b, c LessThan d) if a.semanticEquals(c) =>
          isGreaterOrEqualTo(b, d, a.dataType)
        case (a LessThan b, c GreaterThanOrEqual d) if a.semanticEquals(c) =>
          isGreaterOrEqualTo(d, b, a.dataType)
        case (a GreaterThan b, c LessThanOrEqual d) if a.semanticEquals(c) =>
          isGreaterOrEqualTo(b, d, a.dataType)
        case (a LessThanOrEqual b, c GreaterThan d) if a.semanticEquals(c) =>
          isGreaterOrEqualTo(d, b, a.dataType)
        case (a EqualTo b, Not(c EqualTo d)) if a.semanticEquals(c) =>
          isEqualTo(b, d, a.dataType)
        case _ => false
      }
    } else {
      false
    }
  }

  private def normalizeExpression(expr: Expression): Option[Expression] = {
    expr match {
      case gt @ GreaterThan(_, r) if r.foldable =>
        Some(gt)
      case l GreaterThan r if l.foldable =>
        Some(LessThanOrEqual(r, l))
      case lt @ LessThan(_, r) if r.foldable =>
        Some(lt)
      case l LessThan r if l.foldable =>
        Some(GreaterThanOrEqual(r, l))
      case gte @ GreaterThanOrEqual(_, r) if r.foldable =>
        Some(gte)
      case l GreaterThanOrEqual r if l.foldable =>
        Some(LessThan(r, l))
      case lte @ LessThanOrEqual(_, r) if r.foldable =>
        Some(lte)
      case l LessThanOrEqual r if l.foldable =>
        Some(GreaterThan(r, l))
      case eq @ EqualTo(_, r) if r.foldable =>
        Some(eq)
      case l EqualTo r if l.foldable =>
        Some(EqualTo(r, l))
      case not @ Not(EqualTo(l, r)) if r.foldable =>
        Some(not)
      case Not(l EqualTo r) if l.foldable =>
        Some(Not(EqualTo(r, l)))
      case _ => None
    }
  }

  private def isGreaterOrEqualTo(
      left: Expression, right: Expression, dataType: DataType): Boolean = dataType match {
    case ShortType => left.eval().asInstanceOf[Short] >= right.eval().asInstanceOf[Short]
    case IntegerType => left.eval().asInstanceOf[Int] >= right.eval().asInstanceOf[Int]
    case LongType => left.eval().asInstanceOf[Long] >= right.eval().asInstanceOf[Long]
    case FloatType => left.eval().asInstanceOf[Float] >= right.eval().asInstanceOf[Float]
    case DoubleType => left.eval().asInstanceOf[Double] >= right.eval().asInstanceOf[Double]
    case DecimalType.Fixed(_, _) =>
      left.eval().asInstanceOf[Decimal] >= right.eval().asInstanceOf[Decimal]
    case _ => false
  }

  private def isEqualTo(
      left: Expression, right: Expression, dataType: DataType): Boolean = dataType match {
    case ShortType => left.eval().asInstanceOf[Short] == right.eval().asInstanceOf[Short]
    case IntegerType => left.eval().asInstanceOf[Int] == right.eval().asInstanceOf[Int]
    case LongType => left.eval().asInstanceOf[Long] == right.eval().asInstanceOf[Long]
    case FloatType => left.eval().asInstanceOf[Float] == right.eval().asInstanceOf[Float]
    case DoubleType => left.eval().asInstanceOf[Double] == right.eval().asInstanceOf[Double]
    case DecimalType.Fixed(_, _) =>
      left.eval().asInstanceOf[Decimal] == right.eval().asInstanceOf[Decimal]
    case _ => false
  }

  def countPredicatesInExpressions(expression: Expression): Int = {
      expression match {
        // If the expression is a predicate, count it
        case predicate: Predicate => 1
        // If the expression is a complex expression, recursively count predicates in its children
        case complexExpression =>
          complexExpression.children.map(countPredicatesInExpressions).sum
      }
  }

  def normalizeJoinExpression(expr: Expression): Expression = expr match {
	case BinaryComparison(left, right) =>
      val sortedChildren = Seq(left, right).sortBy(_.toString)
	  expr.withNewChildren(sortedChildren)
	case _ => expr.transform {
	case a: AttributeReference => UnresolvedAttribute(a.name)
	}
   }

  def extendedNormalizeExpression(expr: Expression): Expression = {
   expr.transformUp {
    // Normalize attributes by name, ignoring exprId
    case attr: AttributeReference =>
      // You can adjust the normalization based on what aspects of the attributes are significant for your comparison
      // Here, we're focusing on the name and data type, but excluding metadata and other identifiers
      AttributeReference(attr.name, attr.dataType, attr.nullable)(exprId = NamedExpression.newExprId, qualifier = attr.qualifier)

    // Unwrap aliases to compare the underlying expressions directly
    case Alias(child, _) => child

    case Cast(child, dataType,_,_) =>
      // Normalize child and retain the cast's target data type
      Cast(extendedNormalizeExpression(child), dataType)

    // Handle commutative operations by sorting their children
    case b: BinaryOperator if b.isInstanceOf[Add] || b.isInstanceOf[Multiply] =>
      val sortedChildren = b.children.sortBy(_.toString())
      b.withNewChildren(sortedChildren)

    // Further transformations can be added here to handle other specific cases as needed
    }
  }

  // Function to compare two join conditions after normalization
  def isJoinConditionEqual(condition1: Option[Expression], condition2: Option[Expression]): Boolean = {
    (condition1, condition2) match {
      case (Some(expr1), Some(expr2)) =>

        // Check join condition
        val pattern = "#\\d+"
        val result1 = expr1.toString().replaceAll(pattern, "")
        val result2 = expr2.toString().replaceAll(pattern, "")
        return result1 == result2

      case (None, None) => true // Both conditions are None
      case _ => false // One condition is None and the other is not
    }
  }

  // Function to check if two joins are the same
  def areJoinsEqual(join1: Join, join2: Join): Boolean = {
    // Check join type
    if (join1.joinType != join2.joinType) return false

    if (!isJoinConditionEqual(join1.condition, join2.condition)) return false

    // Joins are equal
    true
  }

  // class to hold Expression with boolean flag
  case class ExpressionHolder(val expression: Expression, val propagate: Boolean)
  /**
   * Try to merge two `Aggregate`s by traverse down recursively.
   *
   * @return The optional tuple as follows:
   *         1. the merged plan
   *         2. the attribute mapping from the old to the merged version
   *         3. optional filters of both plans that need to be propagated and merged in an
   *         ancestor `Aggregate` node if possible.
   */
  private def mergePlan(
      left: LogicalPlan,
      right: LogicalPlan): Option[(LogicalPlan, AttributeMap[Attribute], Seq[ExpressionHolder])] = {
    (left, right) match {
      case (la: Aggregate, ra: Aggregate) =>
        mergePlan(la.child, ra.child).map { case (newChild, outputMap, filters) =>
          val rightAggregateExprs = ra.aggregateExpressions.map(mapAttributes(_, outputMap))

          // Filter the sequence to include only those entries where the propagate is true
          val filtersToBePropagated: Seq[ExpressionHolder] = filters.filter(_.propagate)
          val mergedAggregateExprs = if (filtersToBePropagated.length == 2) {
            Seq(
              (la.aggregateExpressions, filtersToBePropagated.head.expression),
              (rightAggregateExprs, filtersToBePropagated.last.expression)
            ).flatMap { case (aggregateExpressions, propagatedFilter) =>
              aggregateExpressions.map { ne =>
                ne.transform {
                  case ae @ AggregateExpression(_, _, _, filterOpt, _) =>
                    val newFilter = filterOpt.map { filter =>
                      //And(propagatedFilter, filter)
                      filter
                    }.orElse(Some(propagatedFilter))
                    ae.copy(filter = newFilter)
                }.asInstanceOf[NamedExpression]
              }
            }
          } else {
            la.aggregateExpressions ++ rightAggregateExprs
          }

          (Aggregate(Seq.empty, mergedAggregateExprs, newChild), AttributeMap.empty, Seq.empty)
        }
      case (lp: Project, rp: Project) =>
        val mergedProjectList = ArrayBuffer[NamedExpression](lp.projectList: _*)

        mergePlan(lp.child, rp.child).map { case (newChild, outputMap, filters) =>
          val allFilterReferences = filters.flatMap(_.expression.references)
          val newOutputMap = AttributeMap((rp.projectList ++ allFilterReferences).map { ne =>
            val mapped = mapAttributes(ne, outputMap)

            val withoutAlias = mapped match {
              case Alias(child, _) => child
              case e => e
            }

            val outputAttr = mergedProjectList.find {
              case Alias(child, _) => child semanticEquals withoutAlias
              case e => e semanticEquals withoutAlias
            }.getOrElse {
              mergedProjectList += mapped
              mapped
            }.toAttribute
            ne.toAttribute -> outputAttr
          })

          (Project(mergedProjectList.toSeq, newChild), newOutputMap, filters)
        }
      case (lf: Filter, rf: Filter)
        if isCheapPredicate(lf.condition) && isCheapPredicate(rf.condition) =>

        val pattern = "#\\d+"
        // Replace the matched pattern with an empty string
        val result1 = lf.condition.toString().replaceAll(pattern, "")
        val result2 = rf.condition.toString().replaceAll(pattern, "")

        if (result1 == result2 || lf.condition == rf.condition || checkCondition(lf.condition, rf.condition)) {
          // If both conditions are the same, proceed with one of them.
          mergePlan(lf.child, rf.child).map { case (newChild, outputMap, filters) =>
          (Filter(lf.condition, newChild), outputMap, Seq(ExpressionHolder(lf.condition, false)))
          }
        } else {
          mergePlan(lf.child, rf.child).map {
            case (newChild, outputMap, filters) =>
              val mappedRightCondition = mapAttributes(rf.condition, outputMap)
              val (newLeftCondition, newRightCondition) = if (filters.length == 2) {
                (And(lf.condition, filters.head.expression), And(mappedRightCondition, filters.last.expression))
              } else {
                (lf.condition, mappedRightCondition)
              }
            val newCondition = Or(newLeftCondition, newRightCondition)
            (Filter(newCondition, newChild), outputMap, Seq(ExpressionHolder(newLeftCondition,true), ExpressionHolder(newRightCondition,true)))
          }
        }
      case (lj: Join, rj: Join) =>
        if (areJoinsEqual(lj, rj)) {
         mergePlan(lj.left, rj.left).flatMap { case (newLeft, leftOutputMap, leftFilters) =>
          mergePlan(lj.right, rj.right).map { case (newRight, rightOutputMap, rightFilters) =>
           val newJoin = Join(newLeft, newRight, lj.joinType, lj.condition, lj.hint)
           val mergedOutputMap = leftOutputMap ++ rightOutputMap
           val mergedFilters = leftFilters ++ rightFilters
           (newJoin, mergedOutputMap, mergedFilters)
          }
        }
       } else {
         None
       }
      case (ll: LeafNode, rl: LeafNode) =>
         checkIdenticalPlans(rl, ll).map { outputMap =>
            (ll, outputMap, Seq.empty)
         }
      case (ls: SerializeFromObject, rs: SerializeFromObject) =>
        checkIdenticalPlans(rs, ls).map { outputMap =>
          (ls, outputMap, Seq.empty)
        }
      case _ => None
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!ColumnarPluginConfig.getSessionConf.combineJoinedAggregatesEnabled) return plan
    //apply rule on children first then itself
    plan.transformUpWithPruning(_.containsAnyPattern(JOIN, AGGREGATE)) {
      case j @ Join(left: Aggregate, right: Aggregate, joinType, None, _)
        if isSupportedJoinType(joinType) &&
          left.groupingExpressions.isEmpty && right.groupingExpressions.isEmpty =>
        val mergedAggregate = mergePlan(left, right)
        mergedAggregate.map(_._1).getOrElse(j)
    }
  }
}