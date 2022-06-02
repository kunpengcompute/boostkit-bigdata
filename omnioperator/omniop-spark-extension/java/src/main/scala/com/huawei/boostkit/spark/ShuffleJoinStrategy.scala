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

package com.huawei.boostkit.spark

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{joins, SparkPlan}

object ShuffleJoinStrategy extends Strategy
  with PredicateHelper
  with JoinSelectionHelper
  with SQLConfHelper {

  private val columnarPreferShuffledHashJoin =
    ColumnarPluginConfig.getConf.columnarPreferShuffledHashJoin

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, nonEquiCond, left, right, hint)
      if columnarPreferShuffledHashJoin =>
      val enable = getBroadcastBuildSide(left, right, joinType, hint, true, conf).isEmpty &&
        !hintToSortMergeJoin(hint) &&
        getShuffleHashJoinBuildSide(left, right, joinType, hint, true, conf).isEmpty &&
        !hintToShuffleReplicateNL(hint) &&
        getBroadcastBuildSide(left, right, joinType, hint, false, conf).isEmpty
      if (enable) {
        var buildLeft = false
        var buildRight = false
        var joinCountLeft = 0
        var joinCountRight = 0
        left.foreach(x => {
          if (x.isInstanceOf[Join]) {
            joinCountLeft = joinCountLeft + 1
          }
        })
        right.foreach(x => {
          if (x.isInstanceOf[Join]) {
            joinCountRight = joinCountRight + 1
          }
        })
        if ((joinCountLeft > 0) && (joinCountRight == 0)) {
          buildLeft = true
        }
        if ((joinCountRight > 0) && (joinCountLeft == 0)) {
          buildRight = true
        }

        getBuildSide(
          canBuildShuffledHashJoinLeft(joinType) && buildLeft,
          canBuildShuffledHashJoinRight(joinType) && buildRight,
          left,
          right
        ).map {
          buildSide =>
            Seq(joins.ShuffledHashJoinExec(
              leftKeys,
              rightKeys,
              joinType,
              buildSide,
              nonEquiCond,
              planLater(left),
              planLater(right)))
        }.getOrElse(Nil)
      } else {
        Nil
      }

    case _ => Nil
  }

  private def getBuildSide(
    canBuildLeft: Boolean,
    canBuildRight: Boolean,
    left: LogicalPlan,
    right: LogicalPlan): Option[BuildSide] = {
    if (canBuildLeft && canBuildRight) {
      // returns the smaller side base on its estimated physical size, if we want to build the
      // both sides.
      Some(getSmallerSide(left, right))
    } else if (canBuildLeft) {
      Some(BuildLeft)
    } else if (canBuildRight) {
      Some(BuildRight)
    } else {
      None
    }
  }
}
