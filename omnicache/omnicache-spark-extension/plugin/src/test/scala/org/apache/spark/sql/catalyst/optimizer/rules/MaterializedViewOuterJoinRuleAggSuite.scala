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

import com.huawei.boostkit.spark.util.RewriteHelper.{disableCachePlugin, enableCachePlugin}

class MaterializedViewOuterJoinRuleAggSuite extends OuterJoinSuite {

  test("create_agg_outJoin_view_0") {
    def $1(joinType: String, viewNumber: Int): Unit = {
      val joinName = joinType.replace(" ", "_")
      spark.sql(
        s"""
           |DROP MATERIALIZED VIEW IF EXISTS ${joinName}_${viewNumber};
           |""".stripMargin
      )
      spark.sql(
        s"""
           |CREATE MATERIALIZED VIEW IF NOT EXISTS ${joinName}_${viewNumber}
           |AS
           |SELECT e.empid, count(e.salary)
           |FROM emps e
           |${joinType} (select * from depts where deptno > 5 or deptno < 1) d ON e.deptno=d.deptno
           |where e.deptno >= 2 OR e.deptno < 40
           |group by e.empid, e.locationid
           |""".stripMargin
      )
    }

    runOuterJoinFunc($1)(0)
  }

  test("agg_outJoin_group_diff_0_0") {
    def $1(joinType: String, viewNumber: Int): Unit = {
      // is same to view but group by is different.
      val joinName = joinType.replace(" ", "_")
      val sql =
        s"""
           |SELECT e.empid, count(e.salary)
           |FROM emps e
           | ${joinType} (select * from depts where deptno > 5 or deptno < 1) d ON e.deptno=d.deptno
           |where e.deptno >= 2 OR e.deptno < 40
           |group by e.empid
           |""".stripMargin
      comparePlansAndRows(sql, "default", s"${joinName}_${viewNumber}", noData = true)
    }

    runOuterJoinFunc($1)(0)
  }

  // It is not currently supported, but will be supported later.
  test("agg_outJoin_group_diff_0_1") {
    def $1(joinType: String, viewNumber: Int): Unit = {
      // group by is different and query condition is subset of view condition.
      val joinName = joinType.replace(" ", "_")
      val sql =
        s"""
           |SELECT e.empid, count(e.salary)
           |FROM emps e
           | ${joinType} (select * from depts where deptno > 5) d ON e.deptno=d.deptno
           |where e.deptno >= 2
           |group by e.empid
           |""".stripMargin
      compareNotRewriteAndRows(sql, noData = true)
    }

    runOuterJoinFunc($1)(0)
  }

  test("agg_outJoin_group_same_0_0") {
    def $1(joinType: String, viewNumber: Int): Unit = {
      // is same to view but group by is different.
      val joinName = joinType.replace(" ", "_")
      val sql =
        s"""
           |SELECT e.empid, count(e.salary)
           |FROM emps e
           | ${joinType} (select * from depts where deptno > 5 or deptno < 1) d ON e.deptno=d.deptno
           |where e.deptno >= 2 OR e.deptno < 40
           |group by e.empid, e.locationid
           |""".stripMargin
      comparePlansAndRows(sql, "default", s"${joinName}_${viewNumber}", noData = true)
    }

    runOuterJoinFunc($1)(0)
  }

  test("clean_agg_outJoin_view_0") {
    def $1(joinType: String, viewNumber: Int): Unit = {
      val joinName = joinType.replace(" ", "_")
      spark.sql(
        s"""
           |DROP MATERIALIZED VIEW IF EXISTS ${joinName}_${viewNumber};
           |""".stripMargin
      )
    }

    runOuterJoinFunc($1)(0)
  }
}
