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

import com.huawei.boostkit.spark.conf.OmniCachePluginConfig
import com.huawei.boostkit.spark.util._
import com.huawei.boostkit.spark.util.ViewMetadata._
import scala.collection.mutable
import scala.reflect.runtime.{universe => ru}
import scala.util.control.Breaks

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, _}
import org.apache.spark.sql.execution.datasources.LogicalRelation


class MaterializedViewOutJoinRule(sparkSession: SparkSession) {

  /**
   * check plan if match current rule
   *
   * @param logicalPlan LogicalPlan
   * @return true:matched ; false:unMatched
   */
  def isValidPlan(logicalPlan: LogicalPlan): Boolean = {
    logicalPlan.foreach {
      case _: LogicalRelation =>
      case _: HiveTableRelation =>
      case _: Project =>
      case _: Filter =>
      case j: Join =>
        j.joinType match {
          case _: Inner.type =>
          case _: LeftOuter.type =>
          case _: RightOuter.type =>
          case _: FullOuter.type =>
          case _: LeftSemi.type =>
          case _: LeftAnti.type =>
          case _ => return false
        }
      case _: SubqueryAlias =>
      case _ => return false
    }
    true
  }

  /**
   * try match the queryPlan and viewPlan ,then rewrite by viewPlan
   *
   * @param topProject queryTopProject
   * @param plan       queryPlan
   * @param usingMvs   usingMvs
   * @return performedPlan
   */
  def perform(plan: LogicalPlan,
      usingMvs: mutable.Set[String]): LogicalPlan = {
    var finalPlan = plan

    if (ViewMetadata.status == ViewMetadata.STATUS_LOADING) {
      return finalPlan
    }
    RewriteTime.withTimeStat("viewMetadata") {
      ViewMetadata.init(sparkSession)
    }
    // 1.check query sql is match current rule
    if (ViewMetadata.isEmpty || !plan.children.forall(isValidPlan)) {
      return finalPlan
    }

    // 2.extract tablesInfo from queryPlan and replace the AttributeReference
    // in plan using tableAttr
    var (queryExpr, queryTables) = extractTables(finalPlan)

    // 3.use all tables to fetch views(may match) from ViewMetaData
    val candidateViewPlans = RewriteTime.withTimeStat("getApplicableMaterializations") {
      getApplicableMaterializations(queryTables.map(t => t.tableName))
          .filter(x => !OmniCachePluginConfig.isMVInUpdate(x._2))
    }
    if (candidateViewPlans.isEmpty) {
      return finalPlan
    }

    // continue for curPlanLoop,mappingLoop
    val curPlanLoop = new Breaks

    // 4.iterate views,try match and rewrite
    for ((viewName, viewTablePlan, viewQueryPlan) <- candidateViewPlans) {
      curPlanLoop.breakable {
        // 4.1.check view query sql is match current rule
        if (!isValidPlan(viewQueryPlan)) {
          curPlanLoop.break()
        }

        OmniCachePluginConfig.getConf.setCurMatchMV(viewName)

        // 4.3.extract tablesInfo from viewPlan
        val viewTables = ViewMetadata.viewToContainsTables.get(viewName)

        // 4.4.compute the relation of viewTableInfo and queryTableInfo
        // 4.4.1.queryTableInfo containsAll viewTableInfo
        if (!viewTables.subsetOf(queryTables)) {
          curPlanLoop.break()
        }

        // find the Join on viewQueryPlan top.
        val viewQueryTopJoin = viewQueryPlan.find(node => node.isInstanceOf[Join])
        if (viewQueryTopJoin.isEmpty) {
          curPlanLoop.break()
        }

        // extract AttributeReference in queryPlan.
        val queryAttrs = extractAttributeReference(queryExpr)

        // replace exprId in viewTablePlan and viewQueryPlan with exprId in queryExpr.
        replaceExprId(viewTablePlan, queryAttrs)
        replaceExprId(viewQueryPlan, queryAttrs)

        // check relation.
        if (!checkPredicatesRelation(queryExpr, viewQueryPlan)) {
          curPlanLoop.break()
        }

        // rewrite logical plan.
        val viewQueryStr = RewriteHelper.normalizePlan(viewQueryTopJoin.get).toString()
        val normalizedQueryPlan = RewriteHelper.normalizePlan(queryExpr)
        val optPlan = normalizedQueryPlan.transformDown {
          case curPlan: Join =>
            val planStr = curPlan.toString()
            if (!viewQueryStr.equals(planStr)) {
              curPlan
            } else {
              viewTablePlan
            }
        }
        if (RewriteHelper.checkAttrsValid(optPlan)) {
          queryExpr = optPlan
          finalPlan = optPlan
        }
      }
    }
    finalPlan
  }

  /**
   * Check if viewPredict predicates is a subset of queryPredict predicates.
   *
   * @param queryPlan query plan
   * @param viewPlan  view plan
   * @return
   */
  def checkPredicatesRelation(queryPlan: LogicalPlan, viewPlan: LogicalPlan): Boolean = {
    // extract AttributeReference in viewQueryPlan.
    val viewAttrs = extractAttributeReference(viewPlan)

    // function to filter AttributeReference
    def attrFilter(e: ExpressionEqual): Boolean = {
      var contains = true;
      e.realExpr.foreach {
        case attr: AttributeReference =>
          if (!viewAttrs.contains(AttributeReferenceEqual(attr))) {
            contains = false
          }
        case _ =>
      }
      contains
    }

    // extract predicates
    val queryPredicates = RewriteTime.withTimeStat("extractPredictExpressions") {
      extractPredictExpressions(queryPlan, EMPTY_BIMAP)
    }
    val viewPredicates = RewriteTime.withTimeStat("extractPredictExpressions") {
      extractPredictExpressions(viewPlan, EMPTY_BIMAP)
    }
    // equivalence predicates
    val queryEquivalence = queryPredicates._1.getEquivalenceClassesMap
    val viewEquivalence = viewPredicates._1.getEquivalenceClassesMap
    if (!viewEquivalence.keySet.subsetOf(queryEquivalence.keySet)) {
      return false
    }
    for (i <- queryEquivalence.keySet) {
      if (viewEquivalence.contains(i) && !viewEquivalence(i).subsetOf(queryEquivalence(i))) {
        return false
      }
    }

    // range predicates
    val queryRangeSeq = queryPredicates._2.filter(attrFilter).map(_.realExpr)
    val viewRangeSeq = viewPredicates._2.filter(attrFilter).map(_.realExpr)
    if (viewRangeSeq.nonEmpty) {
      if (queryRangeSeq.isEmpty) {
        return false
      }
      val queryRange =
        if (queryRangeSeq.size == 1) queryRangeSeq.head else queryRangeSeq.reduce(And)
      val viewRange =
        if (viewRangeSeq.size == 1) viewRangeSeq.head else viewRangeSeq.reduce(And)
      val simplifyQueryRange = ExprSimplifier.simplify(queryRange)
      val simplifyViewRange = ExprSimplifier.simplify(viewRange)
      val union = ExprSimplifier.simplify(And(simplifyViewRange, simplifyQueryRange))
      if (simplifyQueryRange.sql != union.sql) {
        return false
      }
    }


    // residual predicates
    val queryResidualSeq = queryPredicates._3.filter(attrFilter).map(_.realExpr)
    val viewResidualSeq = viewPredicates._3.filter(attrFilter).map(_.realExpr)
    if ((queryResidualSeq.isEmpty && viewResidualSeq.nonEmpty)
        || (queryResidualSeq.nonEmpty && viewResidualSeq.isEmpty)) {
      return false
    } else if (queryResidualSeq.nonEmpty || viewResidualSeq.nonEmpty) {
      val queryResidual =
        if (queryResidualSeq.size == 1) queryResidualSeq.head else queryResidualSeq.reduce(And)
      val viewResidual =
        if (viewResidualSeq.size == 1) viewResidualSeq.head else viewResidualSeq.reduce(And)
      val simplifyQueryResidual = ExprSimplifier.simplify(queryResidual)
      val simplifyViewResidual = ExprSimplifier.simplify(viewResidual)
      if (simplifyViewResidual.sql != simplifyQueryResidual.sql) {
        return false
      }
    }
    true
  }

  /**
   * Extract AttributeReferences in plan.
   *
   * @param plan LogicalPlan to be extracted.
   * @return Extracted AttributeReference
   */
  def extractAttributeReference(plan: LogicalPlan)
  : mutable.HashMap[AttributeReferenceEqual, ExprId] = {
    val res = mutable.HashMap[AttributeReferenceEqual, ExprId]()
    plan.foreach {
      // TODO 改成RewriteHelper.fillQualifier这种遍历方式
      logicalPlan: LogicalPlan =>
        val allAttr = logicalPlan.references.toSeq ++
            logicalPlan.output ++ logicalPlan.inputSet.toSeq
        allAttr.foreach {
          case attr: AttributeReference =>
            // select AttributeReference
            // which changed by RewriteHelper.fillQualifier(qualifier.size = 4)
            if (attr.qualifier.size == 4) {
              val attrEqual = AttributeReferenceEqual(attr)
              if (res.contains(attrEqual)) {
                // FIXME 这里应该不会有同一个变量值存在多个exprid的情况，先看下
                assert(res(attrEqual).equals(attrEqual.attr.exprId))
              }
              res.put(attrEqual, attr.exprId)
            }
        }
    }
    res
  }

  /**
   * Replace exprId in plan with the exprId in attrs.
   *
   * @param plan  LogicalPlan to be replaced.
   * @param attrs replace with the elements in this map.
   */
  def replaceExprId(plan: LogicalPlan,
      attrs: mutable.HashMap[AttributeReferenceEqual, ExprId]): Unit = {
    val termName = "exprId"
    val m = ru.runtimeMirror(this.getClass.getClassLoader)
    val exprIdTermSymb = ru.typeOf[AttributeReference].decl(ru.TermName(termName)).asTerm
    plan.foreach {
      logicalPlan: LogicalPlan =>
        // TODO 改成RewriteHelper.fillQualifier这种遍历方式
        val allAttr = logicalPlan.output ++ logicalPlan.inputSet.toSeq ++
            logicalPlan.references.toSeq
        allAttr.foreach {
          case attr: AttributeReference =>
            if (attr.qualifier.size == 4) {
              val attrEqual = AttributeReferenceEqual(attr)
              if (!attrs.contains(attrEqual)) {
                // TODO 防止id重复，看下会不会影响结果
                attrs.put(attrEqual, NamedExpression.newExprId)
              }
              val exprIdFieldMirror = m.reflect(attr).reflectField(exprIdTermSymb)
              exprIdFieldMirror.set(attrs(attrEqual))
            }
        }
    }
  }

  /**
   * use all tables to fetch views(may match) from ViewMetaData
   *
   * @param tableNames tableNames in query sql
   * @return Seq[(viewName, viewTablePlan, viewQueryPlan)]
   */
  def getApplicableMaterializations(tableNames: Set[String]): Seq[(String,
      LogicalPlan, LogicalPlan)] = {
    // viewName, viewTablePlan, viewQueryPlan
    var viewPlans = Seq.empty[(String, LogicalPlan, LogicalPlan)]
    val viewNames = mutable.Set.empty[String]
    // 1.topological iterate graph
    tableNames.foreach { tableName =>
      if (ViewMetadata.tableToViews.containsKey(tableName)) {
        viewNames ++= ViewMetadata.tableToViews.get(tableName)
      }
    }
    viewNames.foreach { viewName =>
      // 4.add plan info
      val viewQueryPlan = ViewMetadata.viewToViewQueryPlan.get(viewName)
      val viewTablePlan = ViewMetadata.viewToTablePlan.get(viewName)
      viewPlans +:= (viewName, viewTablePlan, viewQueryPlan)
    }
    viewPlans
  }
}
