package org.apache.spark.sql.execution.ndp

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils.ceil
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.{ColumnStatsMap, FilterEstimation}
import org.apache.spark.sql.types.{BinaryType, BooleanType, DateType, NumericType, StringType, TimestampType}

import scala.collection.immutable.HashSet
import scala.collection.mutable

case class NdpFilterEstimation(filterEstimation: FilterEstimation) extends Logging {

  /* 1 character corresponds to 3 ascii code values,
   * and double have 15 significant digits,
   * so MAX_LEN = 15 / 3
  */
  private val MAX_LEN = 5

  private val childStats = filterEstimation.plan.child.stats

  private val colStatsMap = ColumnStatsMap(childStats.attributeStats)

  def calculateFilterSelectivity(condition: Expression, update: Boolean = true): Option[Double] = {
    condition match {
      case And(cond1, cond2) =>
        val percent1 = calculateFilterSelectivity(cond1, update).getOrElse(1.0)
        val percent2 = calculateFilterSelectivity(cond2, update).getOrElse(1.0)
        Some(percent1 * percent2)

      case Or(cond1, cond2) =>
        val percent1 = calculateFilterSelectivity(cond1, update = false).getOrElse(1.0)
        val percent2 = calculateFilterSelectivity(cond2, update = false).getOrElse(1.0)
        Some(percent1 + percent2 - (percent1 * percent2))

      // Not-operator pushdown
      case Not(And(cond1, cond2)) =>
        calculateFilterSelectivity(Or(Not(cond1), Not(cond2)), update = false)

      // Not-operator pushdown
      case Not(Or(cond1, cond2)) =>
        calculateFilterSelectivity(And(Not(cond1), Not(cond2)), update = false)

      // Collapse two consecutive Not operators which could be generated after Not-operator pushdown
      case Not(Not(cond)) =>
        calculateFilterSelectivity(cond, update = false)

      // The foldable Not has been processed in the ConstantFolding rule
      // This is a top-down traversal. The Not could be pushed down by the above two cases.
      case Not(l@Literal(null, _)) =>
        calculateSingleCondition(l, update = false)

      case Not(cond) =>
        calculateFilterSelectivity(cond, update = false) match {
          case Some(percent) => Some(1.0 - percent)
          case None => None
        }

      case _ =>
        calculateSingleCondition(condition, update)
    }
  }

  def calculateSingleCondition(condition: Expression, update: Boolean): Option[Double] = {
    condition match {
      case l: Literal =>
        filterEstimation.evaluateLiteral(l)

      // For evaluateBinary method, we assume the literal on the right side of an operator.
      // So we will change the order if not.

      // EqualTo/EqualNullSafe does not care about the order
      case Equality(ar: Attribute, l: Literal) =>
        filterEstimation.evaluateEquality(ar, l, update)
      case Equality(l: Literal, ar: Attribute) =>
        filterEstimation.evaluateEquality(ar, l, update)

      case op@LessThan(ar: Attribute, l: Literal) =>
        evaluateBinary(op, ar, l, update)
      case op@LessThan(l: Literal, ar: Attribute) =>
        evaluateBinary(GreaterThan(ar, l), ar, l, update)

      case op@LessThanOrEqual(ar: Attribute, l: Literal) =>
        evaluateBinary(op, ar, l, update)
      case op@LessThanOrEqual(l: Literal, ar: Attribute) =>
        evaluateBinary(GreaterThanOrEqual(ar, l), ar, l, update)

      case op@GreaterThan(ar: Attribute, l: Literal) =>
        evaluateBinary(op, ar, l, update)
      case op@GreaterThan(l: Literal, ar: Attribute) =>
        evaluateBinary(LessThan(ar, l), ar, l, update)

      case op@GreaterThanOrEqual(ar: Attribute, l: Literal) =>
        evaluateBinary(op, ar, l, update)
      case op@GreaterThanOrEqual(l: Literal, ar: Attribute) =>
        evaluateBinary(LessThanOrEqual(ar, l), ar, l, update)

      case In(ar: Attribute, expList)
        if expList.forall(e => e.isInstanceOf[Literal]) =>
        // Expression [In (value, seq[Literal])] will be replaced with optimized version
        // [InSet (value, HashSet[Literal])] in Optimizer, but only for list.size > 10.
        // Here we convert In into InSet anyway, because they share the same processing logic.
        val hSet = expList.map(e => e.eval())
        filterEstimation.evaluateInSet(ar, HashSet() ++ hSet, update)

      case InSet(ar: Attribute, set) =>
        filterEstimation.evaluateInSet(ar, set, update)

      // In current stage, we don't have advanced statistics such as sketches or histograms.
      // As a result, some operator can't estimate `nullCount` accurately. E.g. left outer join
      // estimation does not accurately update `nullCount` currently.
      // So for IsNull and IsNotNull predicates, we only estimate them when the child is a leaf
      // node, whose `nullCount` is accurate.
      // This is a limitation due to lack of advanced stats. We should remove it in the future.
      case IsNull(ar: Attribute) if filterEstimation.plan.child.isInstanceOf[LeafNode] =>
        filterEstimation.evaluateNullCheck(ar, isNull = true, update)

      case IsNotNull(ar: Attribute) if filterEstimation.plan.child.isInstanceOf[LeafNode] =>
        filterEstimation.evaluateNullCheck(ar, isNull = false, update)

      case op@Equality(attrLeft: Attribute, attrRight: Attribute) =>
        filterEstimation.evaluateBinaryForTwoColumns(op, attrLeft, attrRight, update)

      case op@LessThan(attrLeft: Attribute, attrRight: Attribute) =>
        filterEstimation.evaluateBinaryForTwoColumns(op, attrLeft, attrRight, update)

      case op@LessThanOrEqual(attrLeft: Attribute, attrRight: Attribute) =>
        filterEstimation.evaluateBinaryForTwoColumns(op, attrLeft, attrRight, update)

      case op@GreaterThan(attrLeft: Attribute, attrRight: Attribute) =>
        filterEstimation.evaluateBinaryForTwoColumns(op, attrLeft, attrRight, update)

      case op@GreaterThanOrEqual(attrLeft: Attribute, attrRight: Attribute) =>
        filterEstimation.evaluateBinaryForTwoColumns(op, attrLeft, attrRight, update)

      case _ =>
        // TODO: it's difficult to support string operators without advanced statistics.
        // Hence, these string operators Like(_, _) | Contains(_, _) | StartsWith(_, _)
        // | EndsWith(_, _) are not supported yet
        logDebug("[CBO] Unsupported filter condition: " + condition)
        None
    }
  }

  def evaluateBinary(
                      op: BinaryComparison,
                      attr: Attribute,
                      literal: Literal,
                      update: Boolean): Option[Double] = {
    if (!colStatsMap.contains(attr)) {
      logDebug("[CBO] No statistics for " + attr)
      return None
    }

    attr.dataType match {
      case _: NumericType | DateType | TimestampType | BooleanType =>
        filterEstimation.evaluateBinaryForNumeric(op, attr, literal, update)
      case StringType =>
        evaluateBinaryForString(op, attr, literal, update)
      case BinaryType =>
        // type without min/max and advanced statistics like histogram.
        logDebug("[CBO] No range comparison statistics for Binary type " + attr)
        None
    }
  }

  def evaluateBinaryForString(
                                op: BinaryComparison,
                                attr: Attribute,
                                literal: Literal,
                                update: Boolean): Option[Double] = {

    if (!colStatsMap.hasMinMaxStats(attr) || !colStatsMap.hasDistinctCount(attr)) {
      logDebug("[CBO] No statistics for " + attr)
      return None
    }

    val colStat = colStatsMap(attr)
    if (colStat.min.isEmpty || colStat.max.isEmpty) {
      return None
    }
    val maxStr = colStat.max.get.toString
    val minStr = colStat.min.get.toString
    val literalStr = literal.value.toString
    var maxStrLen = 0
    maxStrLen = Math.max(maxStr.length, maxStrLen)
    maxStrLen = Math.max(minStr.length, maxStrLen)
    maxStrLen = Math.max(literalStr.length, maxStrLen)
    val selectStrLen = Math.min(maxStrLen, MAX_LEN)

    val max = convertInternalVal(maxStr, selectStrLen).toDouble
    val min = convertInternalVal(minStr, selectStrLen).toDouble
    val ndv = colStat.distinctCount.get.toDouble

    // determine the overlapping degree between predicate interval and column's interval
    val numericLiteral = convertInternalVal(literalStr, selectStrLen).toDouble
    val (noOverlap: Boolean, completeOverlap: Boolean) = op match {
      case _: LessThan =>
        (numericLiteral <= min, numericLiteral > max)
      case _: LessThanOrEqual =>
        (numericLiteral < min, numericLiteral >= max)
      case _: GreaterThan =>
        (numericLiteral >= max, numericLiteral < min)
      case _: GreaterThanOrEqual =>
        (numericLiteral > max, numericLiteral <= min)
    }

    var percent = 1.0
    if (noOverlap) {
      percent = 0.0
    } else if (completeOverlap) {
      percent = 1.0
    } else {
      // This is the partial overlap case:


      // Without advanced statistics like histogram, we assume uniform data distribution.
      // We just prorate the adjusted range over the initial range to compute filter selectivity.
      assert(max > min)
      percent = op match {
        case _: LessThan =>
          if (numericLiteral == max) {
            // If the literal value is right on the boundary, we can minus the part of the
            // boundary value (1/ndv).
            1.0 - 1.0 / ndv
          } else {
            (numericLiteral - min) / (max - min)
          }
        case _: LessThanOrEqual =>
          if (numericLiteral == min) {
            // The boundary value is the only satisfying value.
            1.0 / ndv
          } else {
            (numericLiteral - min) / (max - min)
          }
        case _: GreaterThan =>
          if (numericLiteral == min) {
            1.0 - 1.0 / ndv
          } else {
            (max - numericLiteral) / (max - min)
          }
        case _: GreaterThanOrEqual =>
          if (numericLiteral == max) {
            1.0 / ndv
          } else {
            (max - numericLiteral) / (max - min)
          }
      }


      if (update) {
        val newValue = Some(literal.value)
        var newMax = colStat.max
        var newMin = colStat.min

        op match {
          case _: GreaterThan | _: GreaterThanOrEqual =>
            newMin = newValue
          case _: LessThan | _: LessThanOrEqual =>
            newMax = newValue
        }

        val newStats = colStat.copy(distinctCount = Some(ceil(ndv * percent)),
          min = newMin, max = newMax, nullCount = Some(0))

        colStatsMap.update(attr, newStats)
      }
    }
    logDebug("calculate filter selectivity for string:" + percent.toString)
    Some(percent)
  }

  def convertInternalVal(value: String, selectStrLen: Int): String = {
    var calValue = ""
    if (value.length > selectStrLen) {
      calValue = value.substring(0, selectStrLen)
    } else {
      calValue = String.format(s"%-${selectStrLen}s", value)
    }
    val vCharArr = calValue.toCharArray
    val vStr = new mutable.StringBuilder
    for (vc <- vCharArr) {
      val repV = String.format(s"%3s", vc.toInt.toString).replace(" ", "0")
      vStr.append(repV)
    }
    vStr.toString
  }
}
