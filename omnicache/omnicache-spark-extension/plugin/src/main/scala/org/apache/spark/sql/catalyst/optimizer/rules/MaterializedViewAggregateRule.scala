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

package org.apache.spark.sql.catalyst.optimizer.rules

import com.google.common.collect.BiMap
import com.huawei.boostkit.spark.util.{ExpressionEqual, RewriteHelper, TableEqual}
import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.DecimalType


class MaterializedViewAggregateRule(sparkSession: SparkSession)
    extends AbstractMaterializedViewRule(sparkSession: SparkSession) {
  /**
   * check plan if match current rule
   *
   * @param logicalPlan LogicalPlan
   * @return true:matched ; false:unMatched
   */
  override def isValidPlan(logicalPlan: LogicalPlan): Boolean = {
    if (!logicalPlan.isInstanceOf[Aggregate]) {
      return false
    }
    logicalPlan.children.forall(isValidLogicalPlan)
  }

  /**
   * queryTableInfo!=viewTableInfo , need do join compensate
   *
   * @param viewTablePlan  viewTablePlan
   * @param viewQueryPlan  viewQueryPlan
   * @param topViewProject topViewProject
   * @param needTables     needTables
   * @return join compensated viewTablePlan
   */
  override def compensateViewPartial(viewTablePlan: LogicalPlan,
      viewQueryPlan: LogicalPlan,
      topViewProject: Option[Project],
      needTables: Seq[TableEqual]):
  Option[(LogicalPlan, LogicalPlan, Option[Project])] = {
    // newViewTablePlan
    var newViewTablePlan = viewTablePlan
    needTables.foreach { needTable =>
      newViewTablePlan = Join(newViewTablePlan, needTable.logicalPlan,
        Inner, None, JoinHint.NONE)
    }

    // newViewQueryPlan
    val aggregate = viewQueryPlan.asInstanceOf[Aggregate]
    var newViewQueryPlan = aggregate.child
    var newGroupingExpressions = aggregate.groupingExpressions
    var newAggregateExpressions = aggregate.aggregateExpressions
    needTables.foreach { needTable =>
      newViewQueryPlan = Join(newViewQueryPlan, needTable.logicalPlan,
        Inner, None, JoinHint.NONE)
      newGroupingExpressions = newGroupingExpressions ++ needTable.logicalPlan.output
      newAggregateExpressions = newAggregateExpressions ++ needTable.logicalPlan.output
    }
    newViewQueryPlan = Aggregate(newGroupingExpressions, newAggregateExpressions, newViewQueryPlan)

    // newTopViewProject
    val projectList = if (topViewProject.isEmpty) {
      newViewQueryPlan.output
    } else {
      topViewProject.get.projectList ++ needTables.flatMap(t => t.logicalPlan.output)
    }
    val newTopViewProject = Project(projectList, newViewQueryPlan)
    Some(newViewTablePlan, newViewQueryPlan, Some(newTopViewProject))
  }

  /**
   * use viewTablePlan(join compensated) ,query project ,
   * compensationPredicts to rewrite final plan
   *
   * @param viewTablePlan   viewTablePlan(join compensated)
   * @param viewQueryPlan   viewQueryPlan
   * @param queryPlan       queryPlan
   * @param tableMapping    tableMapping
   * @param columnMapping   columnMapping
   * @param viewProjectList viewProjectList
   * @param viewTableAttrs  viewTableAttrs
   * @return final plan
   */
  override def rewriteView(viewTablePlan: LogicalPlan, viewQueryPlan: LogicalPlan,
      queryPlan: LogicalPlan, tableMapping: BiMap[String, String],
      columnMapping: Map[ExpressionEqual, mutable.Set[ExpressionEqual]],
      viewProjectList: Seq[Expression], viewTableAttrs: Seq[Attribute]):
  Option[LogicalPlan] = {
    // 1.1.prepare queryAgg
    val queryAgg = queryPlan.asInstanceOf[Aggregate]
    val queryAggExpressions = queryAgg.aggregateExpressions
    val swappedQueryAggExpressions = swapColumnReferences(queryAggExpressions, columnMapping)

    // queryAgg and Group ExpressionEqual
    val queryAggExpressionEquals = swappedQueryAggExpressions.map(ExpressionEqual)
    val queryGroupExpressionEqualsSeq = swapColumnReferences(queryAgg.groupingExpressions,
      columnMapping)
    val queryGroupExpressionEquals = queryGroupExpressionEqualsSeq.map(ExpressionEqual).toSet

    // 1.2.prepare viewAgg
    val viewAgg = viewQueryPlan.asInstanceOf[Aggregate]
    val viewAggExpressions = viewAgg.aggregateExpressions
    val swappedViewAggExpressions = swapTableColumnReferences(viewAggExpressions,
      tableMapping, columnMapping)

    // viewAgg and Group ExpressionEqual
    val viewAggExpressionEqualsOrdinal = swappedViewAggExpressions
        .map(ExpressionEqual).zipWithIndex.toMap
    val viewAggExpressionEquals = viewAggExpressionEqualsOrdinal.keySet
    val viewGroupExpressionEquals = swapTableColumnReferences(viewAgg.groupingExpressions,
      tableMapping, columnMapping).map(ExpressionEqual).toSet

    // 2.1.if queryGroupColumn not in viewGroupExpressionEquals ,it's invalid
    if (!queryGroupExpressionEquals.subsetOf(viewGroupExpressionEquals)) {
      return None
    }

    // 2.2.subGroupExpressionEquals
    val subGroupExpressionEquals = viewGroupExpressionEquals -- queryGroupExpressionEquals

    // newQueryAggExpressions and newGroupingExpressions
    var newQueryAggExpressions: Seq[NamedExpression] = Seq.empty
    var newGroupingExpressions: Seq[Expression] = queryGroupExpressionEqualsSeq

    // if subGroupExpressionEquals is empty and aggCalls all in viewAggExpressionEquals,
    // final need project not aggregate
    val isJoinCompensated = viewTablePlan.isInstanceOf[Join]
    val projectFlag = subGroupExpressionEquals.isEmpty && !isJoinCompensated

    // 3.1.viewGroupExpressionEquals is same to queryGroupExpressionEquals
    if (projectFlag) {
      queryAggExpressionEquals.foreach { aggCall =>
        var expr = aggCall.expression
        // queryAggCall in viewAggExpressionEquals
        if (viewAggExpressionEquals.contains(aggCall)) {
          // change queryAggCall to viewTableAttr
          val viewTableAttr = viewTableAttrs(viewAggExpressionEqualsOrdinal(aggCall))
              .asInstanceOf[AttributeReference]
          expr = expr match {
            // remain Alias info
            case a: Alias =>
              copyAlias(a, viewTableAttr, viewTableAttr.qualifier)
            // not Alias,just column
            case _ =>
              viewTableAttr
          }
          newGroupingExpressions :+= viewTableAttr
          // queryAggCall not in viewAggExpressionEquals,not support count,it's result always be 1
          // because count info cannot found in view
          // such as max(c1),min(c1),sum(c1),avg(c1),count(distinct c1),
          // if c1 in view,it can support
        } else {
          return None
        }
        newQueryAggExpressions :+= expr.asInstanceOf[NamedExpression]
      }
      // 3.2.subGroupExpressionEquals is not empty,so aggCalls need rollUp
    } else {
      queryAggExpressionEquals.foreach { aggCall =>
        var expr = aggCall.expression
        expr match {
          case Alias(AggregateExpression(_, _, isDistinct, _, _), _) =>
            if (isDistinct) {
              return None
            }
          case _ =>
        }
        // rollUp and use viewTableAttr
        if (viewAggExpressionEquals.contains(aggCall)) {
          val viewTableAttr = viewTableAttrs(viewAggExpressionEqualsOrdinal(aggCall))
              .asInstanceOf[AttributeReference]
          val qualifier = viewTableAttr.qualifier
          expr = expr match {
            case a@Alias(agg@AggregateExpression(Sum(_), _, _, _, _), _) =>
              viewTableAttr match {
                case DecimalType.Expression(prec, scale) =>
                  if (prec - 10 > 0) {
                    copyAlias(a, MakeDecimal(agg.copy(aggregateFunction =
                      Sum(UnscaledValue(viewTableAttr))), prec, scale), qualifier)
                  } else {
                    copyAlias(a, agg.copy(aggregateFunction = Sum(viewTableAttr)), qualifier)
                  }
                case _ =>
                  copyAlias(a, agg.copy(aggregateFunction = Sum(viewTableAttr)), qualifier)
              }
            case a@Alias(agg@AggregateExpression(Min(_), _, _, _, _), _) =>
              copyAlias(a, agg.copy(aggregateFunction = Min(viewTableAttr)), qualifier)
            case a@Alias(agg@AggregateExpression(Max(_), _, _, _, _), _) =>
              copyAlias(a, agg.copy(aggregateFunction = Max(viewTableAttr)), qualifier)
            case a@Alias(agg@AggregateExpression(Count(_), _, _, _, _), _) =>
              copyAlias(a, agg.copy(aggregateFunction = Sum(viewTableAttr)), qualifier)
            case a@Alias(AttributeReference(_, _, _, _), _) =>
              copyAlias(a, viewTableAttr, viewTableAttr.qualifier)
            case AttributeReference(_, _, _, _) => viewTableAttr
            case Literal(_, _) | Alias(Literal(_, _), _) =>
              expr
            // other agg like avg or user_defined udaf not support rollUp
            case _ => return None
          }
        } else {
          return None
        }
        newQueryAggExpressions :+= expr.asInstanceOf[NamedExpression]
      }
    }

    // 4.rewrite and alias queryAggExpressions
    // if the rewrite expression exprId != origin expression exprId,
    // replace by Alias(rewrite expression,origin.name)(exprId=origin.exprId)
    val rewritedQueryAggExpressions = rewriteAndAliasExpressions(newQueryAggExpressions,
      swapTableColumn = true, tableMapping, columnMapping,
      viewProjectList, viewTableAttrs, queryAgg.aggregateExpressions)

    if (rewritedQueryAggExpressions.isEmpty) {
      return None
    }

    // 5.add project
    val res =
      if (projectFlag) {
        // 5.1.not need agg,just project
        Some(Project(rewritedQueryAggExpressions.get, viewTablePlan))
      } else {
        // 5.2.need agg,rewrite GroupingExpressions and new agg
        val rewritedGroupingExpressions = rewriteAndAliasExpressions(newGroupingExpressions,
          swapTableColumn = true, tableMapping, columnMapping,
          viewProjectList, viewTableAttrs,
          newGroupingExpressions.map(_.asInstanceOf[NamedExpression]))
        if (rewritedGroupingExpressions.isEmpty) {
          return None
        }
        Some(Aggregate(rewritedGroupingExpressions.get,
          rewritedQueryAggExpressions.get, viewTablePlan))
      }
    if (!RewriteHelper.checkAttrsValid(res.get)) {
      return None
    }
    res
  }

  def copyAlias(alias: Alias, child: Expression, qualifier: Seq[String]): Alias = {
    alias.copy(child = child)(exprId = alias.exprId,
      qualifier = qualifier, explicitMetadata = alias.explicitMetadata,
      nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys)
  }
}
