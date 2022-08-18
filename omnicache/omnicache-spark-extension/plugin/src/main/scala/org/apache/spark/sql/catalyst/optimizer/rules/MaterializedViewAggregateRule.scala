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
import com.huawei.boostkit.spark.util.{ExpressionEqual, TableEqual}
import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, LogicalPlan, Project}


class MaterializedViewAggregateRule(sparkSession: SparkSession)
    extends AbstractMaterializedViewRule(sparkSession: SparkSession) {
  /**
   * cehck plan if match current rule
   *
   * @param logicalPlan LogicalPlan
   * @return true:matched ; false:unMatched
   */
  override def isValidPlan(logicalPlan: LogicalPlan): Boolean = {
    isValidLogicalPlan(logicalPlan)
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
      needTables: Set[TableEqual]):
  Option[(LogicalPlan, LogicalPlan, Option[Project])] = {
    // newViewTablePlan
    var newViewTablePlan = viewTablePlan
    needTables.foreach { needTable =>
      newViewTablePlan = Join(newViewTablePlan, needTable.logicalPlan,
        Inner, None, JoinHint.NONE)
    }
    // newViewQueryPlan
    var newViewQueryPlan = if (topViewProject.isEmpty) {
      viewQueryPlan
    } else {
      topViewProject.get
    }

    needTables.foreach { needTable =>
      newViewQueryPlan = Join(newViewQueryPlan, needTable.logicalPlan,
        Inner, None, JoinHint.NONE)
    }
    Some(newViewTablePlan, viewQueryPlan, None)
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
    val swapQueryProjectList = swapColumnReferences(queryProjectList, columnMapping)

    // rewrite and alias queryProjectList
    // if the rewrite expression exprId != origin expression exprId,
    // replace by Alias(rewrite expression,origin.name)(exprId=origin.exprId)
    val rewritedQueryProjectList = rewriteAndAliasExpressions(swapQueryProjectList,
      swapTableColumn = true, tableMapping, columnMapping,
      viewProjectList, viewTableAttrs, queryProjectList)

    if (rewritedQueryProjectList.isEmpty) {
      return None
    }

    // add project
    Some(Project(rewritedQueryProjectList.get, viewTablePlan))
  }
}
