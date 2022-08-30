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

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{BooleanType, DataType}

object ExprOptUtil {

  /**
   * Returns a condition decomposed by AND.
   */
  def conjunctions(expr: Expression): mutable.Buffer[Expression] = {
    val buf: mutable.Buffer[Expression] = mutable.Buffer()
    decomposeConjunctions(expr, buf)
    buf
  }

  /**
   * Returns a condition decomposed by OR.
   */
  def disjunctions(expr: Expression): mutable.Buffer[Expression] = {
    decomposeDisjunctions(expr).toBuffer
  }

  /**
   * Decompose a predicate into a list of expressions that are AND'ed together,
   * and a list of expressions that are preceded by NOT. For example,
   * a AND NOT b AND NOT (c and d) AND TRUE AND NOT FALSE returns
   * rexList = [a], notList = [b, c AND d].
   */
  def decomposeConjunctions(expr: Expression,
      terms: mutable.Buffer[Expression],
      notTerms: mutable.Buffer[Expression]): Unit = {
    if (expr == null) {
      return
    }
    expr match {
      case And(left: Expression, right: Expression) =>
        decomposeConjunctions(left, terms, notTerms)
        decomposeConjunctions(right, terms, notTerms)
      case Not(child: Expression) =>
        child match {
          case or@Or(_, _) =>
            val ors = decomposeDisjunctions(or)
            for (expr <- ors) {
              expr match {
                case Not(child: Expression) =>
                  terms.+=(child)
                case _ =>
                  notTerms.+=(expr)
              }
            }
          case _ =>
            notTerms.+=(child)
        }
      case literal@Literal(_, _) =>
        if (literal.value != null && literal.dataType == BooleanType
            && literal.value.asInstanceOf[Boolean]) {
          return
        }
        terms.+=(literal)
      case _ =>
        terms.+=(expr)
    }
  }

  /**
   * Decomposes a predicate into a list of expressions that are AND'ed together.
   */
  def decomposeConjunctions(expr: Expression,
      buf: mutable.Buffer[Expression]): Unit = {
    if (expr == null) {
      return
    }
    if (expr.isInstanceOf[And]) {
      decomposeConjunctions(expr.asInstanceOf[And].left, buf)
      decomposeConjunctions(expr.asInstanceOf[And].right, buf)
    } else {
      buf.+=(expr)
    }
  }

  def composeConjunctions(terms: Seq[Expression], nullOnEmpty: Boolean): Expression = {
    makeAnd(terms, nullOnEmpty)
  }

  /**
   * Returns a condition decomposed by OR.
   * For example disjunctions(FALSE) returns the empty list.
   */
  def decomposeDisjunctions(expr: Expression): Seq[Expression] = {
    var res: Seq[Expression] = Seq()
    if (expr == null || isAlwaysFalse(expr)) {
      return Seq()
    }
    expr match {
      case or: Or =>
        res = res ++ decomposeDisjunctions(or.left)
        res = res ++ decomposeDisjunctions(or.right)
      case _ =>
        res = res ++ Seq(expr)
    }
    res
  }

  /**
   * Converts a collection of expressions into an OR,
   * optionally returning null if the list is empty.
   */
  def composeDisjunctions(terms: Seq[Expression], nullOnEmpty: Boolean): Expression = {
    makeOr(terms, nullOnEmpty)
  }

  def makeOr(terms: Seq[Expression], nullOnEmpty: Boolean): Expression = {
    if (terms.isEmpty) {
      if (nullOnEmpty) null else Literal(false, BooleanType)
    } else if (terms.size == 1) {
      terms.head
    } else if (terms.size == 2) {
      val itr = terms.iterator
      val left = itr.next()
      val right = itr.next()
      simplifierMakeOr(left, right)
    } else {
      // Simulates the Spark primordial mode.
      val half = (terms.size - 1) / 2
      val left = makeOr(terms.slice(0, half + 1), nullOnEmpty)
      val right = makeOr(terms.slice(half + 1, terms.size), nullOnEmpty)
      simplifierMakeOr(left, right)
    }
  }

  def makeAnd(terms: Seq[Expression], nullOnEmpty: Boolean): Expression = {
    if (terms.isEmpty) {
      if (nullOnEmpty) null else Literal(true, BooleanType)
    } else if (terms.size == 1) {
      terms.head
    } else if (terms.size == 2) {
      val itr = terms.iterator
      val left = itr.next()
      val right = itr.next()
      simplifyMakeAnd(left, right)
    } else {
      // Simulates the Spark primordial mode.
      val half = (terms.size - 1) / 2
      val left = makeAnd(terms.slice(0, half + 1), nullOnEmpty)
      val right = makeAnd(terms.slice(half + 1, terms.size), nullOnEmpty)
      simplifyMakeAnd(left, right)
    }
  }

  private def simplifyMakeAnd(left: Expression, right: Expression): Expression = {
    if (isLiteralFalse(left) || isLiteralFalse(right)) {
      Literal.FalseLiteral
    } else if (isLiteralTrue(left)) {
      right
    } else if (isLiteralTrue(right)) {
      left
    } else {
      And(left, right)
    }
  }

  private def simplifierMakeOr(left: Expression, right: Expression): Expression = {
    if (isLiteralTrue(left) || isLiteralTrue(right)) {
      Literal.TrueLiteral
    } else if (isLiteralFalse(left)) {
      right
    } else if (isLiteralFalse(right)) {
      left
    } else {
      Or(left, right)
    }
  }

  def isLiteralFalse(e: Expression): Boolean = {
    e.isInstanceOf[Literal] && e.sql.equals("false")
  }

  def isLiteralTrue(e: Expression): Boolean = {
    e.isInstanceOf[Literal] && e.sql.equals("true")
  }

  /**
   * @return Whether the expression.sql in {@code srcTerms}
   *         contains all expression.sql in {@code dstTerms}
   */
  def containsAllSql(srcTerms: Set[Expression], dstTerms: Set[Expression]): Boolean = {
    if (dstTerms.isEmpty || srcTerms.isEmpty) {
      return false
    }
    var sql: mutable.Buffer[String] = mutable.Buffer()
    for (srcTerm <- srcTerms) {
      sql.+=(srcTerm.sql)
    }
    val sqlSet = sql.toSet
    for (dstTerm <- dstTerms) {
      if (!sqlSet.contains(dstTerm.sql)) {
        return false
      }
    }
    true
  }

  /**
   * @return Whether the expression.sql in {@code srcTerms}
   *         contains at least one expression.sql in {@code dstTerms}
   */
  def containsSql(srcTerms: Set[Expression], dstTerms: Set[Expression]): Boolean = {
    if (dstTerms.isEmpty || srcTerms.isEmpty) {
      return false
    }
    var sql: mutable.Buffer[String] = mutable.Buffer()
    for (srcTerm <- srcTerms) {
      sql.+=(srcTerm.sql)
    }
    val sqlSet = sql.toSet
    for (dstTerm <- dstTerms) {
      if (sqlSet.contains(dstTerm.sql)) {
        return true
      }
    }
    false
  }

  /**
   * Returns whether the SQL statements of the srcTerms contains the SQL statement of the dstTerm.
   */
  def containsSql(srcTerms: Set[Expression], dstTerm: Expression): Boolean = {
    var sql: mutable.Buffer[String] = mutable.Buffer()
    for (srcTerm <- srcTerms) {
      sql.+=(srcTerm.sql)
    }
    val sqlSet = sql.toSet
    sqlSet.contains(dstTerm.sql)
  }

  /**
   * Returns the kind that you get if you negate this kind.
   * To conform to null semantics, null value should not be compared.
   * For simplification purposes,
   * except for the case where the NOT needs to be nested outside.
   */
  def negateNullSafe(expr: Expression): Expression = expr match {
    case Not(equalTo@EqualTo(_, _)) =>
      equalTo
    case IsNotNull(child: Expression) =>
      IsNull(child)
    case IsNull(child: Expression) =>
      IsNotNull(child)
    case LessThan(left: Expression, right: Expression) =>
      GreaterThanOrEqual(left, right)
    case GreaterThan(left: Expression, right: Expression) =>
      LessThanOrEqual(left, right)
    case LessThanOrEqual(left: Expression, right: Expression) =>
      GreaterThan(left, right)
    case GreaterThanOrEqual(left: Expression, right: Expression) =>
      LessThan(left, right)
    case EqualTo(left, Literal.TrueLiteral) =>
      EqualTo(left, Literal.FalseLiteral)
    case EqualTo(left, Literal.FalseLiteral) =>
      EqualTo(left, Literal.TrueLiteral)
    case Not(EqualTo(left, Literal.TrueLiteral)) =>
      Not(EqualTo(left, Literal.FalseLiteral))
    case Not(EqualTo(left, Literal.FalseLiteral)) =>
      Not(EqualTo(left, Literal.TrueLiteral))
    case Literal.TrueLiteral =>
      Literal.FalseLiteral
    case Literal.FalseLiteral =>
      Literal.TrueLiteral
    case _ =>
      expr
  }

  /** Returns the kind that you get if you apply NOT to this kind.
   *
   * <p>For example, {@code IS_NOT_NULL.negate()} returns {@link #IS_NULL}.
   *
   * <p>For {@link #IS_TRUE}, {@link #IS_FALSE}, {@link #IS_NOT_TRUE},
   * {@link #IS_NOT_FALSE}, nullable inputs need to be treated carefully.
   *
   * <p>{@code NOT(IS_TRUE(null))} = {@code NOT(false)} = {@code true},
   * while {@code IS_FALSE(null)} = {@code false},
   * so {@code NOT(IS_TRUE(X))} should be {@code IS_NOT_TRUE(X)}.
   * On the other hand,
   * {@code IS_TRUE(NOT(null))} = {@code IS_TRUE(null)} = {@code false}.
   *
   * <p>This is why negate() != negateNullSafe() for these operators.
   *
   * For simplification purposes,
   * except for the case where the NOT needs to be nested outside.
   */
  def negate(expr: Expression): Expression = expr match {
    case IsNotNull(child: Expression) =>
      IsNull(child)
    case IsNull(child: Expression) =>
      IsNotNull(child)
    case Not(ens@EqualNullSafe(_, Literal.TrueLiteral)) =>
      ens
    case Not(ens@EqualNullSafe(_, Literal.FalseLiteral)) =>
      ens
    case Literal.TrueLiteral =>
      Literal.FalseLiteral
    case Literal.FalseLiteral =>
      Literal.TrueLiteral
    case _ =>
      expr
  }

  /**
   * For And Predicate Simplification
   */
  def simpleNegate(expr: Expression): Expression = expr match {
    case equalTo: EqualTo =>
      Not(equalTo)
    case Not(equalTo@EqualTo(_, _)) =>
      equalTo
    case LessThan(left: Expression, right: Expression) =>
      GreaterThanOrEqual(left, right)
    case GreaterThan(left: Expression, right: Expression) =>
      LessThanOrEqual(left, right)
    case LessThanOrEqual(left: Expression, right: Expression) =>
      GreaterThan(left, right)
    case GreaterThanOrEqual(left: Expression, right: Expression) =>
      LessThan(left, right)
    case _ =>
      expr
  }

  def invert(expr: Expression): Option[Expression] = {
    expr match {
      case Not(EqualTo(left: Expression, right: Expression)) =>
        Some(Not(EqualTo(right, left)))
      case EqualTo(left: Expression, right: Expression) =>
        Some(EqualTo(right, left))
      case LessThan(left: Expression, right: Expression) =>
        Some(GreaterThan(right, left))
      case LessThanOrEqual(left: Expression, right: Expression) =>
        Some(GreaterThanOrEqual(right, left))
      case GreaterThan(left: Expression, right: Expression) =>
        Some(LessThan(right, left))
      case GreaterThanOrEqual(left: Expression, right: Expression) =>
        Some(LessThanOrEqual(right, left))
      case _ => None
    }
  }

  def createComparison(expr: Expression): Option[Comparison] = {
    if (expr.isInstanceOf[EqualTo] ||
        expr.isInstanceOf[LessThan] ||
        expr.isInstanceOf[LessThanOrEqual] ||
        expr.isInstanceOf[GreaterThan] ||
        expr.isInstanceOf[GreaterThanOrEqual]) {
      createComparison2(expr.asInstanceOf[BinaryComparison], expr)
    } else {
      None
    }
  }

  private def createComparison2(expr: BinaryComparison, kind: Expression): Option[Comparison] = {
    val left = expr.left
    val right = expr.right
    if (left.isInstanceOf[AttributeReference] && right.isInstanceOf[Literal]) {
      return Some(Comparison(left.asInstanceOf[AttributeReference],
        kind, right.asInstanceOf[Literal]))
    } else if (right.isInstanceOf[AttributeReference] && left.isInstanceOf[Literal]) {
      val invertKind = invert(kind)
      if (invertKind.isDefined) {
        return Some(Comparison(right.asInstanceOf[AttributeReference],
          invertKind.get, left.asInstanceOf[Literal]))
      }
    }
    None
  }

  def isNotEqualTo(expr: Expression): Boolean = {
    expr.isInstanceOf[Not] && expr.asInstanceOf[Not].child.isInstanceOf[EqualTo]
  }

  def isReference(expr: Expression, allowCast: Boolean): Boolean = {
    assert(expr != null)
    if (expr.isInstanceOf[AttributeReference]) {
      return true
    }
    if (allowCast) {
      expr match {
        case cast: Cast =>
          return isReference(cast.child, allowCast = false)
        case _ =>
      }
    }
    false
  }

  def isUpperBound(e: Expression): Boolean = {
    if (!e.isInstanceOf[BinaryComparison]) {
      throw new RuntimeException("isUpperBound() require BinaryExpression parameter.")
    }
    if (e.prettyName.equals("lessthan") || e.prettyName.equals("lessthanorequal")) {
      (isReference(e.asInstanceOf[BinaryComparison].left, allowCast = true)
          && e.asInstanceOf[BinaryComparison].right.isInstanceOf[Literal])
    } else if (e.prettyName.equals("greaterthan") || e.prettyName.equals("greaterthanorequal")) {
      (isReference(e.asInstanceOf[BinaryComparison].right, allowCast = true)
          && e.asInstanceOf[BinaryComparison].left.isInstanceOf[Literal])
    } else {
      false
    }
  }

  def isLowerBound(e: Expression): Boolean = {
    if (!e.isInstanceOf[BinaryComparison]) {
      throw new RuntimeException("isLowerBound() require BinaryExpression parameter.")
    }
    if (e.prettyName.equals("lessthan") || e.prettyName.equals("lessthanorequal")) {
      (isReference(e.asInstanceOf[BinaryComparison].right, allowCast = true)
          && e.asInstanceOf[BinaryComparison].left.isInstanceOf[Literal])
    } else if (e.prettyName.equals("greaterthan") || e.prettyName.equals("greaterthanorequal")) {
      (isReference(e.asInstanceOf[BinaryComparison].left, allowCast = true)
          && e.asInstanceOf[BinaryComparison].right.isInstanceOf[Literal])
    } else {
      false
    }
  }

  def isConstant(expr: Expression): Boolean = {
    assert(expr != null)
    expr.isInstanceOf[Literal]
  }

  @tailrec
  def isAlwaysTrue(expr: Expression): Boolean = {
    expr match {
      case Literal(value: Any, dataType: DataType) =>
        if (dataType.isInstanceOf[BooleanType]) {
          value.asInstanceOf[Boolean]
        } else {
          false
        }
      case IsNotNull(child: Expression) =>
        !child.nullable
      case Not(child: Expression) =>
        isAlwaysFalse(child)
      case Cast(child: Expression, _, _) =>
        isAlwaysTrue(child)
      case _ =>
        false
    }
  }

  @tailrec
  def isAlwaysFalse(expr: Expression): Boolean = {
    expr match {
      case Literal(value: Any, dataType: DataType) =>
        if (dataType.isInstanceOf[BooleanType]) {
          !value.asInstanceOf[Boolean]
        } else {
          false
        }
      case IsNull(child: Expression) =>
        !child.nullable
      case Not(child: Expression) =>
        isAlwaysTrue(child)
      case Cast(child: Expression, _, _) =>
        isAlwaysFalse(child)
      case _ =>
        false
    }
  }

  @tailrec
  def isNull(expr: Expression): Boolean = {
    expr match {
      case literal@Literal(_, _) =>
        literal.value == null
      case Cast(child: Expression, _, _) =>
        isNull(child)
      case _ =>
        false
    }
  }

  def andNot(expr: Expression, notTerms: mutable.Buffer[Expression]): Expression = {
    // If "e" is of the form "x = literal", remove all "x = otherLiteral"
    // terms from notTerms.
    if (expr.isInstanceOf[EqualTo]
        && expr.asInstanceOf[EqualTo].right.isInstanceOf[Literal]) {
      val toBeDeleted: ArrayBuffer[Int] = ArrayBuffer()
      for (i <- notTerms.indices) {
        print()
        if (notTerms(i).isInstanceOf[EqualTo]
            && expr.asInstanceOf[EqualTo].left.sql
            .equals(notTerms(i).asInstanceOf[EqualTo].left.sql)
            && notTerms(i).asInstanceOf[EqualTo].left.isInstanceOf[Literal]) {
          toBeDeleted.+=(i)
        }
      }
      for (i <- toBeDeleted.size - 1 until(-1, -1)) {
        notTerms.remove(i)
      }
    }
    // negate the term in notTerms
    val negateTerms: mutable.Buffer[Expression] = negateBufferTerms(notTerms)
    negateTerms.insert(0, expr)
    composeConjunctions(negateTerms, false)
  }

  def negateBufferTerms(notTerms: mutable.Buffer[Expression]): mutable.Buffer[Expression] = {
    val negateTerms: mutable.Buffer[Expression] = notTerms
        .map((e: Expression) =>
          if (negate(e) != e) {
            negate(e)
          } else if (negateNullSafe(e) != e) {
            negateNullSafe(e)
          } else if (isAlwaysTrue(e)) {
            Literal.FalseLiteral
          } else if (isAlwaysFalse(e)) {
            Literal.TrueLiteral
          } else if (e.isInstanceOf[Not]) {
            e.asInstanceOf[Not].child
          } else {
            Not(e)
          }
        )
    negateTerms
  }
}

case class Comparison(ref: AttributeReference,
    kind: Expression, literal: Literal)

case class EquivalenceClasses() {
  private val nodeToEquivalenceClass:
    mutable.HashMap[ExpressionEqual, mutable.Set[ExpressionEqual]] = mutable.HashMap()
  private var cacheEquivalenceClassesMap: Map[ExpressionEqual, mutable.Set[ExpressionEqual]] = Map()
  private var cacheEquivalenceClasses: List[mutable.Set[ExpressionEqual]] = List()

  def addEquivalenceClass(p: ExpressionEqual, p2: ExpressionEqual): Unit = {
    // Clear cache
    cacheEquivalenceClassesMap = null
    cacheEquivalenceClasses = null

    var p1 = p
    var c1 = nodeToEquivalenceClass.get(p1)
    val c2 = nodeToEquivalenceClass.get(p2)
    if (c1.isDefined && c2.isDefined) {
      // Both present, we need to merge
      if (c1.get.size < c2.get.size) {
        // We swap them to merge
        c1 = c2
        p1 = p2
      }
      val e1 = c1.get
      for (newRef <- c2.get) {
        e1 += newRef
        nodeToEquivalenceClass.put(newRef, e1)
      }
    } else if (c1.isDefined) {
      val e1 = c1.get
      e1 += p2
      nodeToEquivalenceClass.put(p2, e1)
    } else if (c2.isDefined) {
      val e2 = c2.get
      e2 += p1
      nodeToEquivalenceClass.put(p1, e2)
    } else {
      val equivalenceClass: mutable.Set[ExpressionEqual] = mutable.Set[ExpressionEqual]()
      equivalenceClass += p1
      equivalenceClass += p2
      nodeToEquivalenceClass.put(p1, equivalenceClass)
      nodeToEquivalenceClass.put(p2, equivalenceClass)
    }
  }

  def getEquivalenceClassesMap: Map[ExpressionEqual, mutable.Set[ExpressionEqual]] = {
    if (cacheEquivalenceClassesMap == null) {
      cacheEquivalenceClassesMap = nodeToEquivalenceClass.toMap
    }
    cacheEquivalenceClassesMap
  }

  def getEquivalenceClasses: List[mutable.Set[ExpressionEqual]] = {
    if (cacheEquivalenceClasses == null) {
      val visited: mutable.Set[ExpressionEqual] = mutable.Set()
      val builder: mutable.MutableList[mutable.Set[ExpressionEqual]] = mutable.MutableList()
      for (set <- nodeToEquivalenceClass.values) {
        if ((visited & set).isEmpty) {
          visited ++= set
          builder.+=(set)
        }
      }
      cacheEquivalenceClasses = builder.toList
    }
    cacheEquivalenceClasses
  }
}

object EquivalenceClasses {
  def copy(ec: EquivalenceClasses): EquivalenceClasses = {
    val newEc: EquivalenceClasses = EquivalenceClasses()
    for (e <- ec.nodeToEquivalenceClass.iterator) {
      newEc.nodeToEquivalenceClass.put(e._1, e._2.clone())
    }
    newEc.cacheEquivalenceClassesMap = null
    newEc.cacheEquivalenceClasses = null
    newEc
  }
}
