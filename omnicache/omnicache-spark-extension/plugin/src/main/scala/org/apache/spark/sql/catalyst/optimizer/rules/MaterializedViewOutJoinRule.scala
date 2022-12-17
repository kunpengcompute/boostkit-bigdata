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
import com.huawei.boostkit.spark.util._
import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.PushDownPredicates
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, _}

class MaterializedViewOutJoinRule(sparkSession: SparkSession)
    extends AbstractMaterializedViewRule(sparkSession: SparkSession) {

  /**
   * check plan if match current rule
   *
   * @param logicalPlan LogicalPlan
   * @return true:matched ; false:unMatched
   */
  def isValidPlan(logicalPlan: LogicalPlan): Boolean = {
    isValidOutJoinLogicalPlan(logicalPlan)
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
    Some(viewTablePlan, viewQueryPlan, None)
  }

  /**
   * extract filter condition
   *
   * @param plan          logicalPlan
   * @param tableMappings tableMappings
   * @return PredictExpressions
   */
  override def extractPredictExpressions(plan: LogicalPlan,
      tableMappings: BiMap[String, String])
  : (EquivalenceClasses, Seq[ExpressionEqual], Seq[ExpressionEqual]) = {
    extractPredictExpressions(plan, tableMappings, FILTER_CONDITION)
  }

  /**
   * We map every table in the query to a table with the same qualified
   * name (all query tables are contained in the view, thus this is equivalent
   * to mapping every table in the query to a view table).
   *
   * @param queryTables queryTables
   * @return
   */
  override def generateTableMappings(queryTables: Set[TableEqual]): Seq[BiMap[String, String]] = {
    // skipSwapTable
    Seq(EMPTY_BIMAP)
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

    // queryProjectList
    val queryProjectList = extractTopProjectList(queryPlan).map(_.asInstanceOf[NamedExpression])
    val origins = generateOrigins(queryPlan)
    val originQueryProjectList = queryProjectList.map(x => findOriginExpression(origins, x))
    val swapQueryProjectList = swapColumnReferences(originQueryProjectList, columnMapping)
    var simplifiedQueryPlanString = simplifiedPlanString(findOriginExpression(origins, queryPlan))

    val viewTableAttrsSet = viewTableAttrs.toSet
    val viewOrigins = generateOrigins(viewQueryPlan)
    val originViewProjectList = viewProjectList.map(x => findOriginExpression(viewOrigins, x))
    val simplifiedViewPlanString =
      simplifiedPlanString(findOriginExpression(viewOrigins, viewQueryPlan))

    if (simplifiedQueryPlanString == simplifiedViewPlanString) {
      // rewrite and alias queryProjectList
      // if the rewrite expression exprId != origin expression exprId,
      // replace by Alias(rewrite expression,origin.name)(exprId=origin.exprId)
      val rewritedQueryProjectList = rewriteAndAliasExpressions(swapQueryProjectList,
        swapTableColumn = true, tableMapping, columnMapping,
        originViewProjectList, viewTableAttrs, queryProjectList)

      val res = Project(rewritedQueryProjectList.get
          .map(_.asInstanceOf[NamedExpression]), viewTablePlan)
      // add project
      return Some(res)
    }

    var filter: Option[Filter] = None
    var flag = false
    var res = queryPlan.transform {
      case curPlan: Join =>
        simplifiedQueryPlanString = simplifiedPlanString(findOriginExpression(origins, curPlan))
        if (simplifiedQueryPlanString == simplifiedViewPlanString) {
          val (curProject: Project, _) = extractTables(Project(curPlan.output, curPlan))
          val curProjectList = curProject.projectList
              .map(x => findOriginExpression(origins, x).asInstanceOf[NamedExpression])
          val swapCurProjectList = swapColumnReferences(curProjectList, columnMapping)
          val rewritedQueryProjectList = rewriteAndAliasExpressions(swapCurProjectList,
            swapTableColumn = true, tableMapping, columnMapping,
            originViewProjectList, viewTableAttrs, curProjectList)

          flag = true
          val projectChild = viewTablePlan match {
            case f@Filter(_, child) =>
              filter = Some(f)
              child
            case _ =>
              viewTablePlan
          }
          Project(rewritedQueryProjectList.get
              .filter(x => isValidExpression(x, viewTableAttrsSet))
              ++ viewTableAttrs.map(_.asInstanceOf[NamedExpression])
            , projectChild)
        } else {
          curPlan
        }
      case p => p
    }
    if (flag) {
      if (filter.isDefined) {
        val queryProject = res.asInstanceOf[Project]
        res = queryProject.withNewChildren(Seq(
          filter.get.withNewChildren(Seq(queryProject.child))))
      }
      Some(res)
    } else {
      None
    }
  }
}
