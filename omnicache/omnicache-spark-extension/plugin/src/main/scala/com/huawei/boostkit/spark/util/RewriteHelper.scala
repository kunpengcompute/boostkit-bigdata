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

import com.google.common.collect.{ArrayListMultimap, BiMap, HashBiMap, Multimap}
import com.huawei.boostkit.spark.conf.OmniCachePluginConfig
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.optimizer.rules.RewriteTime
import org.apache.spark.sql.catalyst.plans.{Cross, FullOuter, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf


trait RewriteHelper extends PredicateHelper with RewriteLogger {

  type ViewMetadataPackageType = (String, LogicalPlan, LogicalPlan)

  val SESSION_CATALOG_NAME: String = "spark_catalog"

  val EMPTY_BIMAP: HashBiMap[String, String] = HashBiMap.create[String, String]()
  val EMPTY_MAP: Map[ExpressionEqual,
      mutable.Set[ExpressionEqual]] = Map[ExpressionEqual, mutable.Set[ExpressionEqual]]()
  val EMPTY_MULTIMAP: Multimap[Int, Int] = ArrayListMultimap.create[Int, Int]()

  /**
   * merge expressions by and
   */
  def mergeConjunctiveExpressions(exprs: Seq[Expression]): Expression = {
    if (exprs.isEmpty) {
      return Literal.TrueLiteral
    }
    if (exprs.size == 1) {
      return exprs.head
    }
    exprs.reduce { (a, b) =>
      And(a, b)
    }
  }

  /**
   * fill attr's qualifier
   */
  def fillQualifier(plan: LogicalPlan,
      exprIdToQualifier: mutable.HashMap[ExprId, AttributeReference]): LogicalPlan = {
    val newLogicalPlan = plan.transform {
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

  /**
   * fill viewTablePlan's attr's qualifier by viewQueryPlan
   */
  def mapTablePlanAttrToQuery(viewTablePlan: LogicalPlan,
      viewQueryPlan: LogicalPlan): LogicalPlan = {
    // map by index
    var topProjectList: Seq[NamedExpression] = viewQueryPlan match {
      case Project(projectList, _) =>
        projectList
      case Aggregate(_, aggregateExpressions, _) =>
        aggregateExpressions
      case other =>
        other.output
    }
    val exprIdToQualifier = mutable.HashMap[ExprId, AttributeReference]()
    for ((project, column) <- topProjectList.zip(viewTablePlan.output)) {
      project match {
        // only map attr
        case a@Alias(attr@AttributeReference(_, _, _, _), _) =>
          exprIdToQualifier += (column.exprId ->
              attr.copy(name = a.name)(exprId = attr.exprId, qualifier = attr.qualifier))
        case a@AttributeReference(_, _, _, _) =>
          exprIdToQualifier += (column.exprId -> a)
        // skip function
        case _ =>
      }
    }
    fillQualifier(viewTablePlan, exprIdToQualifier)
  }


  /**
   * extract logicalPlan output expressions
   */
  def extractTopProjectList(plan: LogicalPlan): Seq[Expression] = {
    val topProjectList: Seq[Expression] = plan match {
      case Project(projectList, _) => projectList
      case Aggregate(_, aggregateExpressions, _) => aggregateExpressions
      case e => extractTables(Project(e.output, e))._1.output
    }
    topProjectList
  }

  /**
   * generate (alias_exprId,alias_child_expression)
   */
  def generateOrigins(plan: LogicalPlan): Map[ExprId, Expression] = {
    var origins = Map.empty[ExprId, Expression]
    plan.transformAllExpressions {
      case a@Alias(child, _) =>
        origins += (a.exprId -> child)
        a
      case e => e
    }
    origins
  }

  /**
   * find aliased_attr's original expression
   */
  def findOriginExpression(plan: LogicalPlan): LogicalPlan = {
    val origins = generateOrigins(plan)
    findOriginExpression(origins, plan)
  }

  /**
   * find aliased_attr's original expression
   */
  def findOriginExpression(origins: Map[ExprId, Expression], plan: LogicalPlan): LogicalPlan = {
    plan.transformAllExpressions {
      case a: Alias =>
        a.copy(child = findOriginExpression(origins, a.child))(exprId = ExprId(0),
          qualifier = a.qualifier,
          explicitMetadata = a.explicitMetadata,
          nonInheritableMetadataKeys = a.nonInheritableMetadataKeys)
      case expr =>
        findOriginExpression(origins, expr)
    }
  }

  /**
   * find aliased_attr's original expression
   */
  def findOriginExpression(origins: Map[ExprId, Expression],
      expression: Expression): Expression = {
    def dfs(expr: Expression): Expression = {
      expr.transform {
        case attr: AttributeReference =>
          if (origins.contains(attr.exprId)) {
            origins(attr.exprId)
          } else {
            attr
          }
        case e => e
      }
    }

    dfs(expression)
  }

  /**
   * flag for which condition to extract
   */
  val FILTER_CONDITION: Int = 1
  val INNER_JOIN_CONDITION: Int = 1 << 1
  val OUTER_JOIN_CONDITION: Int = 1 << 2
  val COMPENSABLE_CONDITION: Int = FILTER_CONDITION | INNER_JOIN_CONDITION
  val ALL_JOIN_CONDITION: Int = INNER_JOIN_CONDITION | OUTER_JOIN_CONDITION
  val ALL_CONDITION: Int = INNER_JOIN_CONDITION | OUTER_JOIN_CONDITION | FILTER_CONDITION

  /**
   * extract condition from (join and filter),
   * then transform attr's qualifier by tableMappings
   */
  def extractPredictExpressions(
      plan: LogicalPlan,
      tableMappings: BiMap[String, String]): (
      EquivalenceClasses, Seq[ExpressionEqual], Seq[ExpressionEqual]) = {
    extractPredictExpressions(plan, tableMappings, COMPENSABLE_CONDITION)
  }

  /**
   * extract condition from plan by flag,
   * then transform attr's qualifier by tableMappings
   */
  def extractPredictExpressions(plan: LogicalPlan,
      tableMappings: BiMap[String, String], conditionFlag: Int): (
      EquivalenceClasses, Seq[ExpressionEqual], Seq[ExpressionEqual]) = {
    var conjunctivePredicates: Seq[Expression] = Seq()
    var equiColumnsPreds: mutable.Buffer[Expression] = ArrayBuffer()
    val rangePreds: mutable.Buffer[ExpressionEqual] = ArrayBuffer()
    val residualPreds: mutable.Buffer[ExpressionEqual] = ArrayBuffer()
    val normalizedPlan = plan
    normalizedPlan foreach {
      case Filter(condition, _) =>
        if ((conditionFlag & FILTER_CONDITION) > 0) {
          conjunctivePredicates ++= splitConjunctivePredicates(condition)
        }
      case Join(_, _, joinType, condition, _) =>
        joinType match {
          case Cross =>
          case Inner =>
            if (condition.isDefined & ((conditionFlag & INNER_JOIN_CONDITION) > 0)) {
              conjunctivePredicates ++= splitConjunctivePredicates(condition.get)
            }
          case LeftOuter | RightOuter | FullOuter | LeftSemi | LeftAnti =>
            if (condition.isDefined & ((conditionFlag & OUTER_JOIN_CONDITION) > 0)) {
              conjunctivePredicates ++= splitConjunctivePredicates(condition.get)
            }
          case _ =>
        }
      case _ =>
    }

    val origins = generateOrigins(plan)
    for (src <- conjunctivePredicates) {
      val e = findOriginExpression(origins, src)
      if (e.isInstanceOf[EqualTo]) {
        val left = e.asInstanceOf[EqualTo].left
        val right = e.asInstanceOf[EqualTo].right
        if (ExprOptUtil.isReference(left, allowCast = false)
            && ExprOptUtil.isReference(right, allowCast = false)) {
          equiColumnsPreds += e
        } else if ((ExprOptUtil.isReference(left, allowCast = false)
            && ExprOptUtil.isConstant(right))
            || (ExprOptUtil.isReference(right, allowCast = false)
            && ExprOptUtil.isConstant(left))) {
          rangePreds += ExpressionEqual(e)
        } else {
          residualPreds += ExpressionEqual(e)
        }
      } else if (e.isInstanceOf[LessThan] || e.isInstanceOf[GreaterThan]
          || e.isInstanceOf[LessThanOrEqual] || e.isInstanceOf[GreaterThanOrEqual]) {
        val left = e.asInstanceOf[BinaryComparison].left
        val right = e.asInstanceOf[BinaryComparison].right
        if ((ExprOptUtil.isReference(left, allowCast = false)
            && ExprOptUtil.isConstant(right))
            || (ExprOptUtil.isReference(right, allowCast = false)
            && ExprOptUtil.isConstant(left))
            || (left.isInstanceOf[CaseWhen]
            && ExprOptUtil.isConstant(right))
            || (right.isInstanceOf[CaseWhen]
            && ExprOptUtil.isConstant(left))
        ) {
          rangePreds += ExpressionEqual(e)
        } else {
          residualPreds += ExpressionEqual(e)
        }
      } else if (e.isInstanceOf[Or] || e.isInstanceOf[IsNull] || e.isInstanceOf[In]) {
        rangePreds += ExpressionEqual(e)
      } else {
        residualPreds += ExpressionEqual(e)
      }
    }
    equiColumnsPreds = swapTableReferences(equiColumnsPreds, tableMappings)
    val equivalenceClasses: EquivalenceClasses = EquivalenceClasses()
    for (i <- equiColumnsPreds.indices) {
      val left = equiColumnsPreds(i).asInstanceOf[EqualTo].left
      val right = equiColumnsPreds(i).asInstanceOf[EqualTo].right
      equivalenceClasses.addEquivalenceClass(ExpressionEqual(left), ExpressionEqual(right))
    }
    (equivalenceClasses, rangePreds, residualPreds)
  }

  /**
   * extract used tables from logicalPlan
   * and fill attr's qualifier
   *
   * @return (used tables,filled qualifier plan)
   */
  def extractTables(plan: LogicalPlan): (LogicalPlan, Set[TableEqual]) = {
    // tableName->duplicateIndex,start from 0
    val qualifierToIdx = mutable.HashMap.empty[String, Int]
    // logicalPlan->(tableName,duplicateIndex)
    val tablePlanToIdx = mutable.HashMap.empty[LogicalPlan, (String, Int, String, Long)]
    // exprId->AttributeReference,use this to replace LogicalPlan's attr
    val exprIdToAttr = mutable.HashMap.empty[ExprId, AttributeReference]

    val addIdxAndAttrInfo = (catalogTable: CatalogTable, logicalPlan: LogicalPlan,
        attrs: Seq[AttributeReference], seq: Long) => {
      val table = catalogTable.identifier.toString()
      val idx = qualifierToIdx.getOrElse(table, -1) + 1
      qualifierToIdx += (table -> idx)
      tablePlanToIdx += (logicalPlan -> (table,
          idx, Seq(SESSION_CATALOG_NAME, catalogTable.database,
        catalogTable.identifier.table, String.valueOf(idx)).mkString("."),
          seq))
      attrs.foreach { attr =>
        val newAttr = attr.copy()(exprId = attr.exprId, qualifier =
          Seq(SESSION_CATALOG_NAME, catalogTable.database,
            catalogTable.identifier.table, String.valueOf(idx)))
        exprIdToAttr += (attr.exprId -> newAttr)
      }
    }

    var seq = 0L
    plan.foreachUp {
      case h@HiveTableRelation(tableMeta, _, _, _, _) =>
        seq += 1
        addIdxAndAttrInfo(tableMeta, h, h.output, seq)
      case h@LogicalRelation(_, _, catalogTable, _) =>
        seq += 1
        if (catalogTable.isDefined) {
          addIdxAndAttrInfo(catalogTable.get, h, h.output, seq)
        }
      case _ =>
    }

    plan.transformAllExpressions {
      case a@Alias(child, name) =>
        child match {
          case attr: AttributeReference =>
            if (exprIdToAttr.contains(attr.exprId)) {
              val d = exprIdToAttr(attr.exprId)
              exprIdToAttr += (a.exprId -> d
                  .copy(name = name)(exprId = a.exprId, qualifier = d.qualifier))
            }
          case _ =>
        }
        a
      case e => e
    }

    val mappedTables = tablePlanToIdx.keySet.map { tablePlan =>
      val (tableName, idx, qualifier, seq) = tablePlanToIdx(tablePlan)
      TableEqual(tableName, "%s.%d".format(tableName, idx),
        qualifier, fillQualifier(tablePlan, exprIdToAttr), seq)
    }.toSet
    val mappedQuery = fillQualifier(plan, exprIdToAttr)
    (mappedQuery, mappedTables)
  }

  /**
   * extract used tables from logicalPlan
   *
   * @return used tables
   */
  def extractTablesOnly(plan: LogicalPlan): mutable.Set[String] = {
    val tables = mutable.Set[String]()
    plan.foreachUp {
      case HiveTableRelation(tableMeta, _, _, _, _) =>
        tables += tableMeta.identifier.toString()
      case h@LogicalRelation(_, _, catalogTable, _) =>
        if (catalogTable.isDefined) {
          tables += catalogTable.get.identifier.toString()
        }
      case p =>
        p.transformAllExpressions {
          case e: SubqueryExpression =>
            tables ++= extractTablesOnly(e.plan)
            e
          case e => e
        }
    }
    tables
  }

  /**
   * transform plan's attr by tableMapping then columnMapping
   */
  def swapTableColumnReferences[T <: Iterable[Expression]](expressions: T,
      tableMapping: BiMap[String, String],
      columnMapping: Map[ExpressionEqual,
          mutable.Set[ExpressionEqual]]): T = {
    var result: T = expressions
    if (!tableMapping.isEmpty) {
      result = result.map { expr =>
        expr.transform {
          case a: AttributeReference =>
            val key = a.qualifier.mkString(".")
            if (tableMapping.containsKey(key)) {
              val newQualifier = tableMapping.get(key).split('.').toSeq
              a.copy()(exprId = a.exprId, qualifier = newQualifier)
            } else {
              a
            }
          case e => e
        }
      }.asInstanceOf[T]
    }
    if (columnMapping.nonEmpty) {
      result = result.map { expr =>
        expr.transform {
          case e: NamedExpression =>
            val expressionEqual = ExpressionEqual(e)
            if (columnMapping.contains(expressionEqual)) {
              val newAttr = columnMapping(expressionEqual)
                  .head.expression.asInstanceOf[NamedExpression]
              newAttr
            } else {
              e
            }
          case e => e
        }
      }.asInstanceOf[T]
    }
    result
  }

  /**
   * transform plan's attr by columnMapping then tableMapping
   */
  def swapColumnTableReferences[T <: Iterable[Expression]](expressions: T,
      tableMapping: BiMap[String, String],
      columnMapping: Map[ExpressionEqual,
          mutable.Set[ExpressionEqual]]): T = {
    var result = swapTableColumnReferences(expressions, EMPTY_BIMAP, columnMapping)
    result = swapTableColumnReferences(result, tableMapping, EMPTY_MAP)
    result
  }

  /**
   * transform plan's attr by tableMapping
   */
  def swapTableReferences[T <: Iterable[Expression]](expressions: T,
      tableMapping: BiMap[String, String]): T = {
    swapTableColumnReferences(expressions, tableMapping, EMPTY_MAP)
  }

  /**
   * transform plan's attr by columnMapping
   */
  def swapColumnReferences[T <: Iterable[Expression]](expressions: T,
      columnMapping: Map[ExpressionEqual,
          mutable.Set[ExpressionEqual]]): T = {
    swapTableColumnReferences(expressions, EMPTY_BIMAP, columnMapping)
  }

  /**
   * generate string for simplifiedPlan
   *
   * @param plan plan
   * @param jt   joinType
   * @return string for simplifiedPlan
   */
  def simplifiedPlanString(plan: LogicalPlan, jt: Int): String = {
    val EMPTY_STRING = ""
    RewriteHelper.canonicalize(ExprSimplifier.simplify(plan)).collect {
      case Join(_, _, joinType, condition, hint) =>
        joinType match {
          case Inner =>
            if ((INNER_JOIN_CONDITION & jt) > 0) {
              joinType.toString + condition.getOrElse(Literal.TrueLiteral).sql + hint.toString()
            } else {
              EMPTY_STRING
            }
          case LeftOuter | RightOuter | FullOuter | LeftSemi | LeftAnti =>
            if ((OUTER_JOIN_CONDITION & jt) > 0) {
              joinType.toString + condition.getOrElse(Literal.TrueLiteral).sql + hint.toString()
            } else {
              EMPTY_STRING
            }
          case _ =>
            EMPTY_STRING
        }
      case Filter(condition: Expression, _) =>
        if ((FILTER_CONDITION & jt) > 0) {
          condition.sql
        } else {
          EMPTY_STRING
        }
      case HiveTableRelation(tableMeta, _, _, _, _) =>
        tableMeta.identifier.toString()
      case LogicalRelation(_, _, catalogTable, _) =>
        if (catalogTable.isDefined) {
          catalogTable.get.identifier.toString()
        } else {
          EMPTY_STRING
        }
      case _ =>
        EMPTY_STRING
    }.mkString(EMPTY_STRING)
  }

  /**
   * check attr in viewTableAttrs
   *
   * @param expression     expression
   * @param viewTableAttrs viewTableAttrs
   * @return true:in ;false:not in
   */
  def isValidExpression(expression: Expression, viewTableAttrs: Set[Attribute]): Boolean = {
    expression.foreach {
      case attr: AttributeReference =>
        if (!viewTableAttrs.contains(attr)) {
          return false
        }
      case _ =>
    }
    true
  }

  /**
   * partitioned mv columns differ to mv query projectList, sort mv query projectList
   */
  def sortProjectListForPartition(plan: LogicalPlan, catalogTable: CatalogTable): LogicalPlan = {
    if (catalogTable.partitionColumnNames.isEmpty) {
      return plan
    }
    val partitionColumnNames = catalogTable.partitionColumnNames.toSet
    plan match {
      case Project(projectList, child) =>
        var newProjectList = projectList.filter(x => !partitionColumnNames.contains(x.name))
        val projectMap = projectList.map(x => (x.name, x)).toMap
        newProjectList = newProjectList ++ partitionColumnNames.map(x => projectMap(x))
        Project(newProjectList, child)
      case Aggregate(groupingExpressions, aggregateExpressions, child) =>
        var newProjectList = aggregateExpressions
            .filter(x => !partitionColumnNames.contains(x.name))
        val projectMap = aggregateExpressions.map(x => (x.name, x)).toMap
        newProjectList = newProjectList ++ partitionColumnNames.map(x => projectMap(x))
        Aggregate(groupingExpressions, newProjectList, child)
      case p => p
    }
  }

  /**
   * use all tables to fetch views(may match) from ViewMetaData
   *
   * @param tableNames tableNames in query sql
   * @return Seq[(viewName, viewTablePlan, viewQueryPlan)]
   */
  def getApplicableMaterializations(tableNames: Set[String]): Seq[ViewMetadataPackageType] = {
    // viewName, viewTablePlan, viewQueryPlan
    var viewPlans = Seq.empty[(String, LogicalPlan, LogicalPlan)]

    ViewMetadata.viewToContainsTables.forEach { (viewName, tableEquals) =>
      // 1.add plan info
      if (tableEquals.map(_.tableName).subsetOf(tableNames)) {
        val viewQueryPlan = ViewMetadata.viewToViewQueryPlan.get(viewName)
        val viewTablePlan = ViewMetadata.viewToTablePlan.get(viewName)
        viewPlans +:= (viewName, viewTablePlan, viewQueryPlan)
      }
    }
    resortMaterializations(viewPlans)
  }

  /**
   * resort materializations by priority
   */
  def resortMaterializations(candidateViewPlans: Seq[(String,
      LogicalPlan, LogicalPlan)]): Seq[(String, LogicalPlan, LogicalPlan)] = {
    val tuples = candidateViewPlans.sortWith((c1, c2) =>
      ViewMetadata.viewPriority.getOrDefault(c1._1, 0) >
          ViewMetadata.viewPriority.getOrDefault(c2._1, 0)
    )
    tuples
  }
}

object RewriteHelper extends PredicateHelper with RewriteLogger {

  private val secondsInAYear = 31536000L
  private val daysInTenYear = 3650

  /**
   * Rewrite [[EqualTo]] and [[EqualNullSafe]] operator to keep order. The following cases will be
   * equivalent:
   * 1. (a = b), (b = a);
   * 2. (a <=> b), (b <=> a).
   */
  private def rewriteEqual(condition: Expression): Expression = condition match {
    case eq@EqualTo(l: Expression, r: Expression) =>
      if (l.isInstanceOf[AttributeReference] && r.isInstanceOf[Literal]) {
        eq
      } else if (l.isInstanceOf[Literal] && r.isInstanceOf[AttributeReference]) {
        EqualTo(r, l)
      } else {
        Seq(l, r).sortBy(exprHashCode).reduce(EqualTo)
      }
    case eq@EqualNullSafe(l: Expression, r: Expression) =>
      if (l.isInstanceOf[AttributeReference] && r.isInstanceOf[Literal]) {
        eq
      } else if (l.isInstanceOf[Literal] && r.isInstanceOf[AttributeReference]) {
        EqualNullSafe(r, l)
      } else {
        Seq(l, r).sortBy(exprHashCode).reduce(EqualNullSafe)
      }
    case _ => condition // Don't reorder.
  }

  private def reSortOrs(condition: Expression): Expression = {
    splitDisjunctivePredicates(condition).map(rewriteEqual).sortBy(exprHashCode).reduce(Or)
  }

  private def exprHashCode(_ar: Expression): Int = {
    // See http://stackoverflow.com/questions/113511/hash-code-implementation
    _ar.sql.hashCode
  }

  /**
   * Normalizes plans:
   * - Filter the filter conditions that appear in a plan. For instance,
   * ((expr 1 && expr 2) && expr 3), (expr 1 && expr 2 && expr 3), (expr 3 && (expr 1 && expr 2)
   * etc., will all now be equivalent.
   * - Sample the seed will replaced by 0L.
   * - Join conditions will be resorted by hashCode.
   *
   * we use new hash function to avoid `ar.qualifier` from alias affect the final order.
   *
   */
  def normalizePlan(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case Filter(condition: Expression, child: LogicalPlan) =>
        Filter(splitConjunctivePredicates(condition).map(reSortOrs)
            .map(rewriteEqual).sortBy(exprHashCode).reduce(And), child)
      case sample: Sample =>
        sample.copy(seed = 0L)
      case Join(left, right, joinType, condition, hint) if condition.isDefined =>
        val newCondition =
          splitConjunctivePredicates(condition.get).map(reSortOrs)
              .map(rewriteEqual).sortBy(exprHashCode).reduce(And)
        Join(left, right, joinType, Some(newCondition), hint)
    }
  }

  def canonicalize(expression: Expression): Expression = {
    RewriteTime.withTimeStat("canonicalize") {
      val canonicalizedChildren = expression.children.map(RewriteHelper.canonicalize)
      expressionReorder(expression.withNewChildren(canonicalizedChildren))
    }
  }

  def canonicalize(plan: LogicalPlan): LogicalPlan = {
    RewriteTime.withTimeStat("canonicalize") {
      plan transform {
        case filter@Filter(condition: Expression, child: LogicalPlan) =>
          filter.copy(canonicalize(condition), child)
        case join@Join(left: LogicalPlan, right: LogicalPlan, joinType: JoinType,
        condition: Option[Expression], hint: JoinHint) =>
          if (condition.isDefined) {
            join.copy(left, right, joinType, Option(canonicalize(condition.get)), hint)
          } else {
            join
          }
        case e =>
          e
      }
    }
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
    gatherCommutative(e, f).sortBy(exprHashCode)

  /** Rearrange expressions that are commutative or associative. */
  private def expressionReorder(e: Expression): Expression = e match {
    case a@Add(_, _, f) =>
      orderCommutative(a, { case Add(l, r, _) => Seq(l, r) }).reduce(Add(_, _, f))
    case m@Multiply(_, _, f) =>
      orderCommutative(m, { case Multiply(l, r, _) => Seq(l, r) }).reduce(Multiply(_, _, f))

    case o: Or =>
      val s = splitDisjunctivePredicates(o).map(expressionReorder).sortBy(exprHashCode)
      s.reduce(Or)
    case a: And =>
      val s = splitConjunctivePredicates(a).map(expressionReorder).sortBy(exprHashCode)
      s.reduce(And)

    case o: BitwiseOr =>
      orderCommutative(o, { case BitwiseOr(l, r) => Seq(l, r) }).reduce(BitwiseOr)
    case a: BitwiseAnd =>
      orderCommutative(a, { case BitwiseAnd(l, r) => Seq(l, r) }).reduce(BitwiseAnd)
    case x: BitwiseXor =>
      orderCommutative(x, { case BitwiseXor(l, r) => Seq(l, r) }).reduce(BitwiseXor)

    case EqualTo(l, r) if exprHashCode(l) > exprHashCode(r) => EqualTo(r, l)
    case EqualNullSafe(l, r) if exprHashCode(l) > exprHashCode(r) => EqualNullSafe(r, l)

    case GreaterThan(l, r) if exprHashCode(l) > exprHashCode(r) => LessThan(r, l)
    case LessThan(l, r) if exprHashCode(l) > exprHashCode(r) => GreaterThan(r, l)

    case GreaterThanOrEqual(l, r) if exprHashCode(l) > exprHashCode(r) => LessThanOrEqual(r, l)
    case LessThanOrEqual(l, r) if exprHashCode(l) > exprHashCode(r) => GreaterThanOrEqual(r, l)

    // Note in the following `NOT` cases, `l.hashCode() <= r.hashCode()` holds. The reason is that
    // canonicalization is conducted bottom-up -- see [[Expression.canonicalized]].
    case Not(GreaterThan(l, r)) => LessThanOrEqual(l, r)
    case Not(LessThan(l, r)) => GreaterThanOrEqual(l, r)
    case Not(GreaterThanOrEqual(l, r)) => LessThan(l, r)
    case Not(LessThanOrEqual(l, r)) => GreaterThan(l, r)

    // order the list in the In operator
    case In(value, list) if list.length > 1 => In(value, list.sortBy(exprHashCode))

    case g: Greatest =>
      val newChildren = orderCommutative(g, { case Greatest(children) => children })
      Greatest(newChildren)
    case l: Least =>
      val newChildren = orderCommutative(l, { case Least(children) => children })
      Least(newChildren)

    case _ => e
  }

  /**
   * extract all attrs used in expressions
   */
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

  /**
   * check if logicalPlan use mv
   */
  def containsMV(plan: LogicalPlan): Boolean = {
    plan.foreachUp {
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

  def enableCachePlugin(): Unit = {
    SQLConf.get.setConfString("spark.sql.omnicache.enable", "true")
  }

  def disableCachePlugin(): Unit = {
    SQLConf.get.setConfString("spark.sql.omnicache.enable", "false")
  }

  /**
   * check if plan's input attrs satisfy used attrs
   */
  def checkAttrsValid(plan: LogicalPlan): Boolean = {
    logDetail(s"checkAttrsValid for plan:$plan")
    plan.foreachUp {
      case _: LeafNode =>
      case _: Expand =>
      case plan =>
        val attributeSets = plan.expressions.map { expression =>
          AttributeSet.fromAttributeSets(
            expression.collect {
              case s: SubqueryExpression =>
                var res = s.references
                s.plan.transformAllExpressions {
                  case e@OuterReference(ar) =>
                    res ++= AttributeSet(ar.references)
                    e
                  case e => e
                }
                res
              case e => e.references
            })
        }
        val request = AttributeSet.fromAttributeSets(attributeSets)
        val input = plan.inputSet
        val missing = request -- input
        if (missing.nonEmpty) {
          logBasedOnLevel("checkAttrsValid failed for missing:%s".format(missing))
          return false
        }
    }
    true
  }

  /**
   * use rules to optimize queryPlan and viewQueryPlan
   */
  def optimizePlan(plan: LogicalPlan): LogicalPlan = {
    val rules: Seq[Rule[LogicalPlan]] = Seq(
      SimplifyCasts, ConstantFolding, UnwrapCastInBinaryComparison, ColumnPruning)
    var res = plan
    RewriteTime.withTimeStat("optimizePlan") {
      rules.foreach { rule =>
        res = rule.apply(res)
      }
    }
    res
  }

  def getMVDatabase(MVTablePlan: LogicalPlan): Option[String] = {
    MVTablePlan.foreach {
      case _@HiveTableRelation(tableMeta, _, _, _, _) =>
        return Some(tableMeta.database)
      case _@LogicalRelation(_, _, catalogTable, _) =>
        if (catalogTable.isDefined) {
          return Some(catalogTable.get.database)
        }
      case _: LocalRelation =>
      case _ =>
    }
    None
  }

  def daysToMillisecond(days: Long): Long = {
    if (days > daysInTenYear || days < 0) {
      throw new IllegalArgumentException(
        "The day time cannot be less than 0 days"
            + " or exceed 3650 days.")
    }
    days * 24 * 60 * 60 * 1000
  }

  def secondsToMillisecond(seconds: Long): Long = {
    if (seconds > secondsInAYear || seconds < 0L) {
      throw new IllegalArgumentException(
        "The second time cannot be less than 0 seconds"
            + " or exceed 31536000 seconds.")
    }
    seconds * 1000
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

  def extractRealExpr(expression: Expression): Expression = {
    expression.transform {
      case Alias(child, _) => child
      case Cast(child, _, _) => child
      case other => other
    }
  }

  override def toString: String = s"ExpressionEqual($sql)"
}

case class TableEqual(tableName: String, tableNameWithIdx: String,
    qualifier: String, logicalPlan: LogicalPlan, seq: Long) {

  override def equals(obj: Any): Boolean = obj match {
    case other: TableEqual => tableNameWithIdx == other.tableNameWithIdx
    case _ => false
  }

  override def hashCode(): Int = tableNameWithIdx.hashCode()
}

case class AttributeReferenceEqual(attr: AttributeReference) {
  override def toString: String = attr.sql

  override def equals(obj: Any): Boolean = obj match {
    case attrEqual: AttributeReferenceEqual =>
      attr.name == attrEqual.attr.name && attr.dataType == attrEqual.attr.dataType &&
          attr.nullable == attrEqual.attr.nullable && attr.metadata == attrEqual.attr.metadata &&
          attr.qualifier == attrEqual.attr.qualifier
    //    case attribute: AttributeReference =>
    //      attr.name == attribute.name && attr.dataType == attribute.dataType &&
    //          attr.nullable == attribute.nullable && attr.metadata == attribute.metadata &&
    //          attr.qualifier == attribute.qualifier
    case _ => false
  }

  override def hashCode(): Int = {
    var h = 17
    h = h * 37 + attr.name.hashCode()
    h = h * 37 + attr.dataType.hashCode()
    h = h * 37 + attr.nullable.hashCode()
    h = h * 37 + attr.metadata.hashCode()
    h = h * 37 + attr.qualifier.hashCode()
    h
  }
}
