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

import com.google.common.collect.{ArrayListMultimap, HashBiMap, Multimap}
import com.huawei.boostkit.spark.conf.OmniCachePluginConfig
import scala.collection.mutable

import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation

class RewriteHelper extends PredicateHelper {

  val SESSION_CATALOG_NAME: String = "spark_catalog"

  val EMPTY_BITMAP: HashBiMap[String, String] = HashBiMap.create[String, String]()
  val EMPTY_MAP: Map[ExpressionEqual,
      mutable.Set[ExpressionEqual]] = Map[ExpressionEqual, mutable.Set[ExpressionEqual]]()
  val EMPTY_MULTIMAP: Multimap[Int, Int] = ArrayListMultimap.create[Int, Int]

  def fillQualifier(logicalPlan: LogicalPlan,
      exprIdToQualifier: mutable.HashMap[ExprId, AttributeReference]): LogicalPlan = {
    val newLogicalPlan = logicalPlan.transform {
      case plan =>
        plan.transformExpressions {
          case a: AttributeReference =>
            if (exprIdToQualifier.contains(a.exprId)) {
              exprIdToQualifier(a.exprId)
            } else {
              a
            }
          case a => a
        }
    }
    newLogicalPlan
  }

  def mapTablePlanAttrToQuery(viewTablePlan: LogicalPlan,
      viewQueryPlan: LogicalPlan): LogicalPlan = {
    // map by index
    val topProjectList: Seq[NamedExpression] = viewQueryPlan match {
      case Project(projectList, _) =>
        projectList
      case Aggregate(_, aggregateExpressions, _) =>
        aggregateExpressions
    }
    val exprIdToQualifier = mutable.HashMap[ExprId, AttributeReference]()
    for ((project, column) <- topProjectList.zip(viewTablePlan.output)) {
      project match {
        // only map attr
        case _@Alias(attr@AttributeReference(_, _, _, _), _) =>
          exprIdToQualifier += (column.exprId -> attr)
        case a@AttributeReference(_, _, _, _) =>
          exprIdToQualifier += (column.exprId -> a)
        // skip function
        case _ =>
      }
    }
    fillQualifier(viewTablePlan, exprIdToQualifier)
  }

  def extractTopProjectList(logicalPlan: LogicalPlan): Seq[Expression] = {
    val topProjectList: Seq[Expression] = logicalPlan match {
      case Project(projectList, _) => projectList
      case Aggregate(_, aggregateExpressions, _) => aggregateExpressions
    }
    topProjectList
  }

  def extractTables(logicalPlan: LogicalPlan): (LogicalPlan, Set[TableEqual]) = {
    // tableName->duplicateIndex,start from 0
    val qualifierToIdx = mutable.HashMap.empty[String, Int]
    // logicalPlan->(tableName,duplicateIndex)
    val tablePlanToIdx = mutable.HashMap.empty[LogicalPlan, (String, Int, String)]
    // exprId->AttributeReference,use this to replace LogicalPlan's attr
    val exprIdToAttr = mutable.HashMap.empty[ExprId, AttributeReference]

    val addIdxAndAttrInfo = (catalogTable: CatalogTable, logicalPlan: LogicalPlan,
        attrs: Seq[AttributeReference]) => {
      val table = catalogTable.identifier.toString()
      val idx = qualifierToIdx.getOrElse(table, -1) + 1
      qualifierToIdx += (table -> idx)
      tablePlanToIdx += (logicalPlan -> (table,
          idx, Seq(SESSION_CATALOG_NAME, catalogTable.database,
        catalogTable.identifier.table, String.valueOf(idx)).mkString(".")))
      attrs.foreach { attr =>
        val newAttr = attr.copy()(exprId = attr.exprId, qualifier =
          Seq(SESSION_CATALOG_NAME, catalogTable.database,
            catalogTable.identifier.table, String.valueOf(idx)))
        exprIdToAttr += (attr.exprId -> newAttr)
      }
    }

    logicalPlan.foreachUp {
      case h@HiveTableRelation(tableMeta, _, _, _, _) =>
        addIdxAndAttrInfo(tableMeta, h, h.output)
      case h@LogicalRelation(_, _, catalogTable, _) =>
        if (catalogTable.isDefined) {
          addIdxAndAttrInfo(catalogTable.get, h, h.output)
        }
      case _ =>
    }

    val mappedTables = tablePlanToIdx.keySet.map { tablePlan =>
      val (tableName, idx, qualifier) = tablePlanToIdx(tablePlan)
      TableEqual(tableName, "%s.%d".format(tableName, idx),
        qualifier, fillQualifier(tablePlan, exprIdToAttr))
    }.toSet
    val mappedQuery = fillQualifier(logicalPlan, exprIdToAttr)
    (mappedQuery, mappedTables)
  }
}

object RewriteHelper {
  def canonicalize(expression: Expression): Expression = {
    val canonicalizedChildren = expression.children.map(RewriteHelper.canonicalize)
    expressionReorder(expression.withNewChildren(canonicalizedChildren))
  }

  /** Collects adjacent commutative operations. */
  private def gatherCommutative(
      e: Expression,
      f: PartialFunction[Expression, Seq[Expression]]): Seq[Expression] = e match {
    case c if f.isDefinedAt(c) => f(c).flatMap(gatherCommutative(_, f))
    case other => other :: Nil
  }

  /** Orders a set of commutative operations by their hash code. */
  private def orderCommutative(
      e: Expression,
      f: PartialFunction[Expression, Seq[Expression]]): Seq[Expression] =
    gatherCommutative(e, f).sortBy(_.hashCode())

  /** Rearrange expressions that are commutative or associative. */
  private def expressionReorder(e: Expression): Expression = e match {
    case a@Add(_, _, f) =>
      orderCommutative(a, { case Add(l, r, _) => Seq(l, r) }).reduce(Add(_, _, f))
    case m@Multiply(_, _, f) =>
      orderCommutative(m, { case Multiply(l, r, _) => Seq(l, r) }).reduce(Multiply(_, _, f))

    case o: Or =>
      orderCommutative(o, { case Or(l, r) if l.deterministic && r.deterministic => Seq(l, r) })
          .reduce(Or)
    case a: And =>
      orderCommutative(a, { case And(l, r) if l.deterministic && r.deterministic => Seq(l, r) })
          .reduce(And)

    case o: BitwiseOr =>
      orderCommutative(o, { case BitwiseOr(l, r) => Seq(l, r) }).reduce(BitwiseOr)
    case a: BitwiseAnd =>
      orderCommutative(a, { case BitwiseAnd(l, r) => Seq(l, r) }).reduce(BitwiseAnd)
    case x: BitwiseXor =>
      orderCommutative(x, { case BitwiseXor(l, r) => Seq(l, r) }).reduce(BitwiseXor)

    case EqualTo(l, r) if l.hashCode() > r.hashCode() => EqualTo(r, l)
    case EqualNullSafe(l, r) if l.hashCode() > r.hashCode() => EqualNullSafe(r, l)

    case GreaterThan(l, r) if l.hashCode() > r.hashCode() => LessThan(r, l)
    case LessThan(l, r) if l.hashCode() > r.hashCode() => GreaterThan(r, l)

    case GreaterThanOrEqual(l, r) if l.hashCode() > r.hashCode() => LessThanOrEqual(r, l)
    case LessThanOrEqual(l, r) if l.hashCode() > r.hashCode() => GreaterThanOrEqual(r, l)

    // Note in the following `NOT` cases, `l.hashCode() <= r.hashCode()` holds. The reason is that
    // canonicalization is conducted bottom-up -- see [[Expression.canonicalized]].
    case Not(GreaterThan(l, r)) => LessThanOrEqual(l, r)
    case Not(LessThan(l, r)) => GreaterThanOrEqual(l, r)
    case Not(GreaterThanOrEqual(l, r)) => LessThan(l, r)
    case Not(LessThanOrEqual(l, r)) => GreaterThan(l, r)

    // order the list in the In operator
    case In(value, list) if list.length > 1 => In(value, list.sortBy(_.hashCode()))

    case g: Greatest =>
      val newChildren = orderCommutative(g, { case Greatest(children) => children })
      Greatest(newChildren)
    case l: Least =>
      val newChildren = orderCommutative(l, { case Least(children) => children })
      Least(newChildren)

    case _ => e
  }

  def extractAllAttrsFromExpression(expressions: Seq[Expression]): Set[AttributeReference] = {
    var attrs = Set[AttributeReference]()
    expressions.foreach { e =>
      e.foreach {
        case a@AttributeReference(_, _, _, _) =>
          attrs += a
        case _ =>
      }
    }
    attrs
  }

  def containsMV(logicalPlan: LogicalPlan): Boolean = {
    logicalPlan.foreachUp {
      case _@HiveTableRelation(tableMeta, _, _, _, _) =>
        if (OmniCachePluginConfig.isMV(tableMeta)) {
          return true
        }
      case _@LogicalRelation(_, _, catalogTable, _) =>
        if (catalogTable.isDefined) {
          if (OmniCachePluginConfig.isMV(catalogTable.get)) {
            return true
          }
        }
      case _ =>
    }
    false
  }
}

case class ExpressionEqual(expression: Expression) {
  // like org.apache.spark.sql.catalyst.expressions.EquivalentExpressions.Expr
  lazy val realExpr: Expression = RewriteHelper.canonicalize(extractRealExpr(expression))
  lazy val sql: String = realExpr.sql

  override def equals(obj: Any): Boolean = obj match {
    case e: ExpressionEqual => sql == e.sql
    case _ => false
  }

  override def hashCode(): Int = sql.hashCode()

  def extractRealExpr(expression: Expression): Expression = expression match {
    case Alias(child, _) => extractRealExpr(child)
    case other => other
  }
}

case class TableEqual(tableName: String, tableNameWithIdx: String,
    qualifier: String, logicalPlan: LogicalPlan) {

  override def equals(obj: Any): Boolean = obj match {
    case other: TableEqual => tableNameWithIdx == other.tableNameWithIdx
    case _ => false
  }

  override def hashCode(): Int = tableNameWithIdx.hashCode()
}
