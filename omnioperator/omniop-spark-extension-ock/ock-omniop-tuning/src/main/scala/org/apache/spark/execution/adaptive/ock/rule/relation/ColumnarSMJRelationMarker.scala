/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package org.apache.spark.sql.execution.adaptive.ock.rule.relation

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{ColumnarSortMergeJoinExec, SortMergeJoinExec}

object ColumnarSMJRelationMarker extends RelationMarker {

  override def solve(plan: SparkPlan): SparkPlan = plan.transformUp {
    case csmj @ ColumnarSortMergeJoinExec(_, _, _, _, left, right, _, _) =>
      SMJRelationMarker.solveDepAndWorkGroupOfSMJExec(left, right)
      csmj
    case smj @ SortMergeJoinExec(_, _, _, _, left, right, _) =>
      SMJRelationMarker.solveDepAndWorkGroupOfSMJExec(left, right)
      smj
  }
}