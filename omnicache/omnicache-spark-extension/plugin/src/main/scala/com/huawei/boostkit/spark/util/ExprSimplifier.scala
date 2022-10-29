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

package com.huawei.boostkit.spark.util

import com.google.common.collect.{ArrayListMultimap, BoundType, Multimap, Range => GuavaRange}
import com.huawei.boostkit.spark.util.ExprOptUtil._
import java.util
import org.apache.calcite.util.{Pair, RangeUtil}
import scala.collection.{mutable, JavaConverters}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, OneRowRelation}
import org.apache.spark.sql.types.{BooleanType, DataType, NullType}

case class ExprSimplifier(unknownAsFalse: Boolean,
    pulledUpPredicates: Set[Expression]) {

  def simplify(condition: Expression): Expression = condition match {
    case and@And(_, _) =>
      simplifyAnd(and)
    case or@Or(_, _) =>
      simplifyOrs(or)
    case IsNull(_) | IsNotNull(_) |
         EqualNullSafe(_, _) | Not(EqualNullSafe(_, _)) =>
      simplifyIs(condition)
    case not@Not(_) =>
      simplifyNot(not)
    case EqualTo(_, _) | LessThan(_, _) |
         LessThanOrEqual(_, _) | GreaterThan(_, _) |
         GreaterThanOrEqual(_, _) =>
      simplifyComparison(condition.asInstanceOf[BinaryComparison])
    case caseWhen@CaseWhen(_, _) =>
      simplifyCase(caseWhen)
    case _ => condition
  }

  private def simplifyCase(caseWhen: CaseWhen): Expression = {
    val newBranches: mutable.Buffer[(Expression, Expression)] = caseWhen.branches.toBuffer
    var newElseValue: Option[Expression] = caseWhen.elseValue
    val break1 = Breaks
    val toBeDeleted: ArrayBuffer[Int] = ArrayBuffer()
    break1.breakable {
      for (i <- caseWhen.branches.indices) {
        val when = caseWhen.branches(i)._1
        val then = caseWhen.branches(i)._2
        if (isAlwaysTrue(when)) {
          newElseValue = Some(then)
          for (j <- i until newBranches.size) {
            // j should not used in the following statement
            newBranches.remove(i)
          }
          break1.break()
        } else if (isAlwaysFalse(when) || isNull(when)) {
          // Predicate is always FALSE or NULL. Skip predicate and value.
          toBeDeleted.+=(i)
        }
      }
    }
    for (i <- toBeDeleted.size - 1 until(-1, -1)) {
      newBranches.remove(toBeDeleted(i))
    }
    if (newBranches.size == 0) {
      val res = newElseValue.get
      if (isNull(res) && unknownAsFalse) {
        return Literal.FalseLiteral
      } else {
        return res
      }
    }

    val break2 = Breaks
    break2.breakable {
      if (caseWhen.dataType.isInstanceOf[BooleanType]) {
        // Optimize CASE where every branch returns constant true or constant false.

        // 1) Possible simplification if unknown is treated as false:
        //   CASE
        //   WHEN p1 THEN TRUE
        //   WHEN p2 THEN TRUE
        //   ELSE FALSE
        //   END
        // can be rewritten to: (p1 or p2)
        if (unknownAsFalse) {
          val terms: mutable.Buffer[Expression] = mutable.Buffer()
          var canSimplify = true
          for ((when, then) <- newBranches) {
            if (!isAlwaysTrue(then)) {
              canSimplify = false
            }
            terms.+=(when)
          }
          if (!isAlwaysFalse(newElseValue.get)) {
            canSimplify = false
          }
          if (canSimplify) {
            return composeDisjunctions(terms, false)
          }
        }

        // 2) Another simplification
        //   CASE
        //   WHEN p1 THEN TRUE
        //   WHEN p2 THEN FALSE
        //   WHEN p3 THEN TRUE
        //   ELSE FALSE
        //   END
        // can be rewrite to: (p1 or (p3 and not p2))
        // if p1...pn cannot be nullable
        // then condition must always be True or False
        for ((when, then) <- newBranches) {
          if (when.nullable) {
            break2.break()
          }
          if (!isAlwaysFalse(then) && !isAlwaysTrue(then)
              && (!unknownAsFalse || !isNull(then))) {
            break2.break()
          }
        }
        val terms: mutable.Buffer[Expression] = mutable.Buffer()
        val notTerms: mutable.Buffer[Expression] = mutable.Buffer()
        for ((when, then) <- newBranches) {
          if (isAlwaysTrue(then)) {
            terms.+=(andNot(when, notTerms))
            notTerms.clear()
          } else {
            notTerms.+=(when)
          }
        }
        if (notTerms.size > 0) {
          terms.++=(negateBufferTerms(notTerms))
        }
        return composeDisjunctions(terms, false)
      }
    }
    if (newBranches.size == caseWhen.branches.size) {
      return caseWhen
    }
    CaseWhen(newBranches, newElseValue)
  }

  private def simplifyComparison(condition: BinaryComparison): Expression = {
    val c0 = simplify(condition.left)
    val c1 = simplify(condition.right)
    // Simplify "x <op> x"
    if (c0.sql.equals(c1.sql)
        && (unknownAsFalse
        || (!c0.nullable
        && !c1.nullable))) {
      if (condition.isInstanceOf[EqualTo]
          || condition.isInstanceOf[GreaterThanOrEqual]
          || condition.isInstanceOf[LessThanOrEqual]) {
        return simplify(IsNotNull(c0))
      } else {
        return Literal.FalseLiteral
      }
    }

    // Simplify "<literal1> <op> <literal2>"
    // For example, "1 = 2" becomes FALSE;
    // "1 != 1" becomes FALSE;
    // "1 != NULL" becomes UNKNOWN (or FALSE if unknownAsFalse);
    // "1 != '1'" is unchanged because the types are not the same.
    if (c0.isInstanceOf[Literal] && c1.isInstanceOf[Literal]) {
      val l0 = c0.asInstanceOf[Literal]
      val l1 = c1.asInstanceOf[Literal]
      if (l0.dataType.typeName.equals(l1.dataType.typeName)) {
        return RangeUtil.compareLiteral(l0, l1, unknownAsFalse, condition)
      }
    }

    val res = condition match {
      case EqualTo(_, _) =>
        EqualTo(c0, c1)
      case GreaterThan(_, _) =>
        GreaterThan(c0, c1)
      case GreaterThanOrEqual(_, _) =>
        GreaterThanOrEqual(c0, c1)
      case LessThan(_, _) =>
        LessThan(c0, c1)
      case LessThanOrEqual(_, _) =>
        LessThanOrEqual(c0, c1)
      case _ => condition
    }
    RangeUtil.simplifyUsingPredicates(res, JavaConverters.setAsJavaSet(pulledUpPredicates))
  }

  private def simplifyAnd(condition: And): Expression = {
    val terms: mutable.Buffer[Expression] = ArrayBuffer()
    val notTerms: mutable.Buffer[Expression] = ArrayBuffer()
    decomposeConjunctions(condition, terms, notTerms)
    simplifySeq(terms)
    simplifySeq(notTerms)
    if (unknownAsFalse) {
      simplifyAnd2ForUnknownAsFalse(terms, notTerms)
    } else {
      simplifyAnd2(terms, notTerms)
    }
  }

  private def simplifyAnd2(terms: mutable.Buffer[Expression],
      notTerms: mutable.Buffer[Expression]): Expression = {
    for (term <- terms) {
      if (isAlwaysFalse(term)) {
        return Literal.FalseLiteral
      }
    }
    if (terms.isEmpty && notTerms.isEmpty) {
      return Literal.TrueLiteral
    }

    for (notTerm <- notTerms) {
      val terms2 = conjunctions(notTerm)
      if (containsAllSql(terms.toSet, terms2.toSet)) {
        return Literal.FalseLiteral
      }
    }
    // Add the NOT disjunctions back in.
    for (notTerm <- notTerms) {
      terms.+=(simplify(Not(notTerm)))
    }
    composeConjunctions(terms, false)
  }

  private def simplifyAnd2ForUnknownAsFalse(terms: mutable.Buffer[Expression],
      notTerms: mutable.Buffer[Expression]): Expression = {
    for (term <- terms) {
      if (isAlwaysFalse(term)) {
        return Literal.FalseLiteral
      }
    }
    if (terms.isEmpty && notTerms.isEmpty) {
      return Literal.TrueLiteral
    }
    if (terms.size == 1 && notTerms.isEmpty) {
      return simplify(terms.head)
    }

    // Try to simplify the expression
    val equalityTerms: Multimap[String, (String, Expression)] = ArrayListMultimap.create()
    val rangeTerms: util.Map[String, Pair[GuavaRange[Comparable[_]], util.List[Expression]]] =
      new util.HashMap[String, Pair[GuavaRange[Comparable[_]], util.List[Expression]]]
    val equalityConstantTerms: mutable.HashMap[String, String] = mutable.HashMap()
    val negatedTerms: mutable.HashSet[Expression] = mutable.HashSet()
    val nullOperands: mutable.HashSet[Expression] = mutable.HashSet()
    val notNullOperands: mutable.HashSet[Expression] = mutable.HashSet()
    val comparedOperands: mutable.HashSet[Expression] = mutable.HashSet()
    val orsOperands: mutable.ListBuffer[Expression] = mutable.ListBuffer()

    // Add the predicates from the source to the range terms.
    for (predicate <- pulledUpPredicates) {
      val comparison = createComparison(predicate)
      if (comparison.isDefined) {
        val v0 = RangeUtil.getComparableValueAs(comparison.get.literal)
        if (v0 != null) {
          val res = RangeUtil.processRange(
            JavaConverters.bufferAsJavaList(terms),
            rangeTerms,
            predicate, comparison.get.ref, v0, comparison.get.kind)
          if (res != null) {
            return res
          }
        }
      }
    }

    val breaks1 = new Breaks
    val toBeDelTerm: ArrayBuffer[Expression] = ArrayBuffer()
    for (i <- terms.indices) {
      val term = terms(i)
      breaks1.breakable {
        if (!term.deterministic) {
          breaks1.break()
        }

        if (term.isInstanceOf[EqualTo] ||
            term.isInstanceOf[LessThan] ||
            term.isInstanceOf[LessThanOrEqual] ||
            term.isInstanceOf[GreaterThan] ||
            term.isInstanceOf[GreaterThanOrEqual]) {
          val left = term.asInstanceOf[BinaryComparison].left
          val right = term.asInstanceOf[BinaryComparison].right
          comparedOperands.add(left)
          // if it is a cast, we include the inner reference
          if (left.isInstanceOf[Cast]) {
            comparedOperands.add(left.asInstanceOf[Cast].child)
          }
          comparedOperands.add(right)
          if (right.isInstanceOf[Cast]) {
            comparedOperands.add(right.asInstanceOf[Cast].child)
          }
          val comparison = createComparison(term)
          // Check for comparison with null values
          if (comparison.isDefined && comparison.get.literal.value == null) {
            return Literal.FalseLiteral
          }
          // Check for equality on different constants. If the same ref or CAST(ref)
          // is equal to different constants, this condition cannot be satisfied,
          // and hence it can be evaluated to FALSE
          if (term.isInstanceOf[EqualTo]) {
            if (comparison.isDefined) {
              val literal = comparison.get.literal.sql
              val prevLiteral = equalityConstantTerms.put(comparison.get.ref.sql, literal)
              if (prevLiteral.isDefined && !prevLiteral.get.equals(literal)) {
                return Literal.FalseLiteral
              }
            } else if (isReference(left, true) && isReference(right, true)) {
              equalityTerms.put(left.sql, (right.sql, term))
            }
          }
          // Assume the expression a > 5 is part of a Filter condition.
          // Then we can derive the negated term: a <= 5.
          // But as the comparison is string based and thus operands order dependent,
          // we should also add the inverted negated term: 5 >= a.
          // Observe that for creating the inverted term we invert the list of operands.
          val negatedTerm = ExprOptUtil.simpleNegate(term)
          if (negatedTerm != term) {
            negatedTerms.add(negatedTerm)
            val invertNegatedTerm = invert(negatedTerm)
            if (invertNegatedTerm.isDefined) {
              negatedTerms.add(invertNegatedTerm.get)
            }
          }
          // Remove terms that are implied by predicates on the input,
          // or weaken terms that are partially implied.
          // E.g. given predicate "x >= 5" and term "x between 3 and 10"
          // we weaken to term to "x between 5 and 10".
          val term2 = RangeUtil
              .simplifyUsingPredicates(term, JavaConverters.setAsJavaSet(pulledUpPredicates))
          if (!term2.sql.equals(term.sql)) {
            terms.remove(i)
            terms.insert(i, term2)
          }
          // Range
          if (comparison.isDefined) {
            val constant = RangeUtil.getComparableValueAs(comparison.get.literal)
            if (constant != null) {
              val res = RangeUtil.processRange(
                JavaConverters.bufferAsJavaList(terms),
                rangeTerms,
                term, comparison.get.ref, constant, comparison.get.kind)
              if (res != null) {
                return res
              }
            }
          }
        } else if (term.isInstanceOf[In]) {
          comparedOperands.add(term.asInstanceOf[In].value)
        } else if (term.isInstanceOf[IsNotNull]) {
          notNullOperands.add(term.asInstanceOf[IsNotNull].child)
          toBeDelTerm.+=(term)
        } else if (term.isInstanceOf[IsNull]) {
          nullOperands.add(term.asInstanceOf[IsNull].child)
        } else if (term.isInstanceOf[Or]) {
          orsOperands.+=(term)
        }
      }
    }
    for (i <- toBeDelTerm.indices) {
      terms.-=(toBeDelTerm(i))
    }
    // disjoint
    // If one column should be null and is in a comparison predicate,
    // it is not satisfiable.
    // Example. IS NULL(x) AND x < 5  - not satisfiable
    if (containsSql(nullOperands.toSet, comparedOperands.toSet)) {
      return Literal.FalseLiteral
    }
    // Check for equality of two refs wrt equality with constants
    // Example #1. x=5 AND y=5 AND x=y : x=5 AND y=5
    // Example #2. x=5 AND y=6 AND x=y - not satisfiable
    val equalitySets = JavaConverters.asScalaSet(equalityTerms.keySet())
    val out = new Breaks
    val inner = new Breaks
    for (ref1 <- equalitySets) {
      out.breakable {
        val literal1 = equalityConstantTerms.get(ref1)
        if (literal1.isEmpty) {
          out.break()
        }
        val references = JavaConverters.collectionAsScalaIterable(equalityTerms.get(ref1))
        for (r <- references) {
          inner.breakable {
            val literal2 = equalityConstantTerms.get(r._1)
            if (literal2.isEmpty) {
              inner.break()
            }
            if (!literal1.equals(literal2)) {
              // If an expression is equal to two different constants,
              // it is not satisfiable
              return Literal.FalseLiteral
            }
            // Otherwise we can remove the term, as we already know that
            // the expression is equal to two constants
            terms.-=(r._2)
          }
        }
      }
    }
    // Remove not necessary IS NOT NULL expressions.
    //
    // Example. IS NOT NULL(x) AND x < 5  : x < 5
    for (notNullOperand <- notNullOperands) {
      if (!containsSql(comparedOperands.toSet, notNullOperand)) {
        terms.+=(IsNotNull(notNullOperand))
      }
    }
    // If one of the not-disjunctions is a disjunction that is wholly
    // contained in the disjunctions list, the expression is not
    // satisfiable.
    //
    // Example #1. x AND y AND z AND NOT (x AND y)  - not satisfiable
    // Example #2. x AND y AND NOT (x AND y)        - not satisfiable
    // Example #3. x AND y AND NOT (x AND y AND z)  - may be satisfiable
    val breaks2 = new Breaks
    for (notTerm <- notTerms) {
      breaks2.breakable {
        if (!notTerm.deterministic) {
          breaks2.break()
        }
        val terms2Set: mutable.Buffer[Expression] = conjunctions(notTerm)
        if (containsAllSql(terms.toSet, terms2Set.toSet)) {
          return Literal.FalseLiteral
        }
      }
    }
    // Simplify the range to a point.
    val itr = rangeTerms.values().iterator()
    while (itr.hasNext) {
      val value = itr.next()
      val range = value.left
      if (range.hasLowerBound && range.hasUpperBound
          && range.upperEndpoint().equals(range.lowerEndpoint())
          && range.upperBoundType() == range.lowerBoundType()
          && range.upperBoundType() == BoundType.CLOSED
          && value.right.size() > 1) {
        val comparison = createComparison(value.right.get(0))
        if (comparison.isDefined) {
          val equalExpr = EqualTo(comparison.get.ref, comparison.get.literal)
          RangeUtil.removeExpression(JavaConverters.bufferAsJavaList(terms), value.right)
          terms.+=(equalExpr)
        }
      }
    }
    // Add the NOT disjunctions back in.
    for (notTerm <- notTerms) {
      terms.+=(simplify(Not(notTerm)))
    }
    // The negated terms: only deterministic expressions
    if (containsSql(terms.toSet, negatedTerms.toSet)) {
      return Literal.FalseLiteral
    }
    // simplify And-Ors situation.
    val breaks3 = new Breaks
    for (orOp <- orsOperands) {
      breaks3.breakable {
        val ors = decomposeDisjunctions(orOp).toSet
        for (term <- terms) {
          // Excluding self-simplification
          if (!term.eq(orOp)) {
            // Simplification between a OrExpression and a OrExpression.
            if (term.isInstanceOf[Or]) {
              if (containsAllSql(ors, decomposeDisjunctions(term).toSet)) {
                terms.-=(orOp)
                breaks3.break()
              }
            } else if (containsSql(ors, term)) {
              // Simplification between a otherExpression and a OrExpression.
              terms.-=(orOp)
              breaks3.break()
            }
          }
        }
      }
    }
    composeConjunctions(terms, false)
  }

  private def simplifyNot(condition: Expression): Expression = {
    val child = condition.asInstanceOf[Not].child
    child match {
      case Not(child) =>
        return simplify(child)
      case And(left: Expression, right: Expression) =>
        return simplify(Or(simplify(Not(left)), simplify(Not(right))))
      case Or(left: Expression, right: Expression) =>
        return simplify(And(simplify(Not(left)), simplify(Not(right))))
      case _ =>
    }
    val negateExpr1 = ExprOptUtil.negateNullSafe(child)
    if (negateExpr1 != child) {
      return simplify(negateExpr1)
    }
    val negateExpr2 = ExprOptUtil.negate(child)
    if (negateExpr2 != child) {
      return simplify(negateExpr2)
    }
    condition
  }

  private def simplifySeq(terms: mutable.Buffer[Expression]): Unit = {
    for (i <- terms.indices) {
      val simplifiedTerm = ExprSimplifier(false, pulledUpPredicates).simplify(terms(i))
      terms.remove(i)
      terms.insert(i, simplifiedTerm)
    }
  }

  private def simplifyOrs(condition: Expression): Expression = {
    val terms: mutable.Buffer[Expression] = decomposeDisjunctions(condition).toBuffer
    val toBeDelTerms: ArrayBuffer[Expression] = ArrayBuffer()
    val brk = Breaks
    for (i <- terms.indices) {
      brk.breakable {
        val term = terms(i)
        val simplifiedTerm: Expression = simplify(term)
        simplifiedTerm match {
          case literal@Literal(_, dataType: DataType) =>
            if (!dataType.isInstanceOf[NullType] && literal.value != null) {
              if (dataType.isInstanceOf[BooleanType]
                  && literal.value.asInstanceOf[Boolean]) {
                return simplifiedTerm
              } else {
                toBeDelTerms.+=(term)
                brk.break()
              }
            }
          case _ =>
        }
        terms.remove(i)
        terms.insert(i, simplifiedTerm)
      }
    }
    for (i <- toBeDelTerms.indices) {
      terms.-=(toBeDelTerms(i))
    }
    composeDisjunctions(terms, false)
  }

  def simplifyIs(condition: Expression): Expression = {
    val child = condition.children.head
    condition match {
      // IS_NULL
      case _: IsNull =>
        if (!child.nullable) {
          return Literal.FalseLiteral
        }
      // IS_NOT_NULL
      case _: IsNotNull =>
        val simplified = simplifyIsNotNull(child)
        if (simplified.isDefined) {
          return simplified.get
        }
      // IS_TRUE,IS_NOT_FALSE
      case _@EqualNullSafe(c, Literal.TrueLiteral) =>
        if (!c.nullable) {
          return simplify(c)
        }
      case _@Not(EqualNullSafe(c, Literal.FalseLiteral)) =>
        if (!c.nullable) {
          return simplify(c)
        }
      // IS_FALSE,IS_NOT_TRUE
      case _@EqualNullSafe(c, Literal.FalseLiteral) =>
        if (!c.nullable) {
          return simplify(Not(c))
        }
      case _@Not(EqualNullSafe(c, Literal.TrueLiteral)) =>
        if (!c.nullable) {
          return simplify(Not(c))
        }
      case _ =>
    }

    // child.nullable
    child match {
      // NOT
      case _: Not =>
        return simplify(ExprOptUtil.negateNullSafe(child))
      case _ =>
    }

    // simply child and rebuild as condition
    val child2 = simplify(child)
    if (child2 != child) {
      val condition2 = condition match {
        case c@IsNull(_) =>
          c.copy(child = child2)
        case c@IsNotNull(_) =>
          c.copy(child = child2)
        case c@EqualNullSafe(_, Literal.TrueLiteral) =>
          c.copy(left = child2)
        case c@Not(c1@EqualNullSafe(_, Literal.FalseLiteral)) =>
          Not(c1.copy(left = child2))
        case c@EqualNullSafe(_, Literal.FalseLiteral) =>
          c.copy(left = child2)
        case c@Not(c1@EqualNullSafe(_, Literal.TrueLiteral)) =>
          Not(c1.copy(left = child2))
        case c =>
          throw new RuntimeException("unSupport type is predicate simplify :%s".format(c))
      }
      return condition2
    }

    // cannot be simplified
    condition
  }

  def simplifyIsNotNull(condition: Expression): Option[Expression] = {
    if (!condition.nullable) {
      return Some(Literal.TrueLiteral)
    }
    condition match {
      case c: Literal =>
        return Some(Literal(!c.nullable))
      case Not(_) | EqualTo(_, _) |
           LessThan(_, _) | LessThanOrEqual(_, _) |
           GreaterThan(_, _) | GreaterThanOrEqual(_, _) |
           Like(_, _, _) | Add(_, _, _) | Subtract(_, _, _) |
           Multiply(_, _, _) | Divide(_, _, _) | Cast(_, _, _) |
           StringTrim(_, _) | StringTrimLeft(_, _) | StringTrimRight(_, _) |
           Ceil(_) | Floor(_) | Extract(_, _, _) | Greatest(_) | Least(_) |
           TimeAdd(_, _, _)
      =>
        var children = Seq.empty[Expression]
        condition.children.foreach { child =>
          val child2 = simplifyIsNotNull(child)
          if (child2.isEmpty) {
            children :+= IsNotNull(child)
          } else if (ExprOptUtil.isAlwaysFalse(child2.get)) {
            return Some(Literal.FalseLiteral)
          } else {
            child2.get match {
              case Literal.FalseLiteral =>
                return Some(Literal.FalseLiteral)
              case _ =>
                children :+= child2.get
            }
          }
        }
        return Some(ExprOptUtil.composeConjunctions(children, nullOnEmpty = false))
      case _ =>
    }
    None
  }
}

object ExprSimplifier extends PredicateHelper {
  // Spark native simplification rules to be executed before this simplification
  val frontRules = Seq(SimplifyCasts, ConstantFolding, UnwrapCastInBinaryComparison, ColumnPruning)

  // simplify condition with pulledUpPredicates.
  def simplify(logicalPlan: LogicalPlan): LogicalPlan = {
    val originPredicates: mutable.ArrayBuffer[Expression] = ArrayBuffer()
    val normalizeLogicalPlan = RewriteHelper.normalizePlan(logicalPlan)
    normalizeLogicalPlan foreach {
      case Filter(condition, _) =>
        originPredicates ++= splitConjunctivePredicates(condition)
      case Join(_, _, _, condition, _) if condition.isDefined =>
        originPredicates ++= splitConjunctivePredicates(condition.get)
      case _ =>
    }
    val inferredPlan = InferFiltersFromConstraints.apply(normalizeLogicalPlan)
    val inferredPredicates: mutable.ArrayBuffer[Expression] = mutable.ArrayBuffer()
    inferredPlan foreach {
      case Filter(condition, _) =>
        inferredPredicates ++= splitConjunctivePredicates(condition)
      case Join(_, _, _, condition, _) if condition.isDefined =>
        inferredPredicates ++= splitConjunctivePredicates(condition.get)
      case _ =>
    }
    val pulledUpPredicates: Set[Expression] = inferredPredicates.toSet -- originPredicates.toSet
    // front Spark native optimize
    var optPlan: LogicalPlan = normalizeLogicalPlan
    for (rule <- frontRules) {
      optPlan = rule.apply(optPlan)
    }
    optPlan transform {
      case Filter(condition: Expression, child: LogicalPlan) =>
        val simplifyExpr = ExprSimplifier(true, pulledUpPredicates).simplify(condition)
        Filter(simplifyExpr, child)
      case Join(left, right, joinType, condition, hint) if condition.isDefined =>
        val simplifyExpr = ExprSimplifier(true, pulledUpPredicates).simplify(condition.get)
        Join(left, right, joinType, Some(simplifyExpr), hint)
      case other@_ =>
        other
    }
  }

  // simplify condition without pulledUpPredicates.
  def simplify(expr: Expression): Expression = {
    val fakePlan = simplify(Filter(expr, OneRowRelation()))
    RewriteHelper.canonicalize(fakePlan.asInstanceOf[Filter].condition)
  }
}
