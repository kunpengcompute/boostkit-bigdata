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

import org.apache.spark.sql.catalyst.optimizer.rules.RewriteSuite._

class MaterializedViewOuterJoinRuleProjectSuite extends OuterJoinSuite {

  test("create_project_outJoin_view_0") {
    def $1(joinType: String, viewNumber: Int): Unit = {
      val joinName = joinType.replace(" ", "_")
      var leftTable = "(select * from depts where deptno > 50 or deptno < 5) d"
      var rightTable = "emps e"
      joinType match {
        case "RIGHT JOIN" =>
          leftTable = "emps e"
          rightTable = "(select * from depts where deptno > 50 or deptno < 5) d"
        case "LEFT JOIN" | "SEMI JOIN" | "ANTI JOIN" =>
        case _ =>
      }
      spark.sql(
        s"""
           |DROP MATERIALIZED VIEW IF EXISTS ${joinName}_${viewNumber};
           |""".stripMargin
      )
      spark.sql(
        s"""
           |CREATE MATERIALIZED VIEW IF NOT EXISTS ${joinName}_${viewNumber}
           |AS
           |SELECT d.deptno
           |FROM ${leftTable}
           |${joinType} ${rightTable}
           |ON e.deptno=d.deptno
           |where d.deptno >= 40 OR d.deptno < 2;
           |""".stripMargin
      )
    }

    runOuterJoinFunc($1)(0)
  }

  test("project_outJoin_filterCondition_compensate_0_0") {
    def $1(joinType: String, viewNumber: Int): Unit = {
      val joinName = joinType.replace(" ", "_")
      var leftTable = "(select * from depts where deptno > 50 or deptno < 5) d"
      var rightTable = "emps e"
      joinType match {
        case "RIGHT JOIN" =>
          leftTable = "emps e"
          rightTable = "(select * from depts where deptno > 50 or deptno < 5) d"
        case "LEFT JOIN" | "SEMI JOIN" | "ANTI JOIN" =>
        case _ =>
      }
      val sql =
        s"""
           |SELECT d.deptno
           |FROM ${leftTable}
           |${joinType} ${rightTable}
           |ON e.deptno=d.deptno
           |where d.deptno >= 40;
           |""".stripMargin
      comparePlansAndRows(sql, "default", s"${joinName}_${viewNumber}", noData = true)
    }

    runOuterJoinFunc($1)(0)
  }

  test("project_outJoin_innerCondition_compensate_0_1") {
    def $1(joinType: String, viewNumber: Int): Unit = {
      val joinName = joinType.replace(" ", "_")
      var leftTable = "(select * from depts where deptno > 50) d"
      var rightTable = "emps e"
      joinType match {
        case "RIGHT JOIN" =>
          leftTable = "emps e"
          rightTable = "(select * from depts where deptno > 50) d"
        case "LEFT JOIN" | "SEMI JOIN" | "ANTI JOIN" =>
        case _ =>
      }
      val sql =
        s"""
           |SELECT d.deptno
           |FROM ${leftTable}
           |${joinType} ${rightTable}
           |ON e.deptno=d.deptno
           |where d.deptno >= 40 OR d.deptno < 2;
           |""".stripMargin
      comparePlansAndRows(sql, "default", s"${joinName}_${viewNumber}", noData = true)
    }

    runOuterJoinFunc($1)(0)
  }

  test("project_outJoin_same_0_2") {
    def $1(joinType: String, viewNumber: Int): Unit = {
      // is same to view.
      val joinName = joinType.replace(" ", "_")
      var leftTable = "(select * from depts where deptno > 50 or deptno < 5) d"
      var rightTable = "emps e"
      joinType match {
        case "RIGHT JOIN" =>
          leftTable = "emps e"
          rightTable = "(select * from depts where deptno > 50 or deptno < 5) d"
        case "LEFT JOIN" | "SEMI JOIN" | "ANTI JOIN" =>
        case _ =>
      }
      val sql =
        s"""
           |SELECT d.deptno
           |FROM ${leftTable}
           |${joinType} ${rightTable}
           |ON e.deptno=d.deptno
           |where d.deptno >= 40 OR d.deptno < 2;
           |""".stripMargin
      comparePlansAndRows(sql, "default", s"${joinName}_${viewNumber}", noData = true)
    }

    runOuterJoinFunc($1)(0)
  }

  test("clean_project_outJoin_view_0") {
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

  test("create_project_outJoin_view_1") {
    def $1(joinType: String, viewNumber: Int): Unit = {
      val joinName = joinType.replace(" ", "_")
      var leftTable = "(select * from depts where deptno > 50 or deptno < 5) d"
      var rightTable = "emps e"
      joinType match {
        case "RIGHT JOIN" =>
          leftTable = "emps e"
          rightTable = "(select * from depts where deptno > 50 or deptno < 5) d"
        case "LEFT JOIN" | "SEMI JOIN" | "ANTI JOIN" =>
        case _ =>
      }
      val leftAlias = leftTable.split(" ").last
      spark.sql(
        s"""
           |DROP MATERIALIZED VIEW IF EXISTS ${joinName}_${viewNumber};
           |""".stripMargin
      )
      spark.sql(
        s"""
           |CREATE MATERIALIZED VIEW IF NOT EXISTS ${joinName}_${viewNumber}
           |AS
           |SELECT d.deptno
           |FROM  locations l JOIN
           |${leftTable} ON l.locationid = ${leftAlias}.deptno
           |${joinType} ${rightTable}
           |ON e.deptno=d.deptno or ${leftAlias}.deptno is not null
           |where d.deptno >= 40 OR d.deptno < 2;
           |""".stripMargin
      )
    }

    runOuterJoinFunc($1)(1)
  }

  /**
   * The join of the view and the join of the query must match from scratch.
   * Positive example:
   * view:  select * from a left join b join c right join d join e where ...
   * query: select * from a left join b join c right join d where ...
   *
   * Bad example:
   * view:  select * from a left join b join c right join d join e where ...
   * query: select * from b join c right join d where ...
   */
  test("project_outJoin_MatchFromHead_1_0") {
    def $1(joinType: String, viewNumber: Int): Unit = {
      val joinName = joinType.replace(" ", "_")
      var leftTable = "(select * from depts where deptno > 50 or deptno < 5) d"
      var rightTable = "emps e"
      joinType match {
        case "RIGHT JOIN" =>
          leftTable = "emps e"
          rightTable = "(select * from depts where deptno > 50 or deptno < 5) d"
        case "LEFT JOIN" | "SEMI JOIN" | "ANTI JOIN" =>
        case _ =>
      }
      val leftAlias = leftTable.split(" ").last
      val sql =
        s"""
           |SELECT d.deptno
           |FROM ${leftTable}
           |${joinType} ${rightTable}
           |ON e.deptno=d.deptno or ${leftAlias}.deptno is not null
           |where d.deptno >= 40 OR d.deptno < 2;
           |""".stripMargin
      compareNotRewriteAndRows(sql, noData = true)
    }

    runOuterJoinFunc($1)(1)
  }

  // At present, the out join condition needs to be consistent,
  // and the support with inconsistent condition may be carried out in the future
  test("project_outJoin_OutJoinCondition_diff_1_1") {
    def $1(joinType: String, viewNumber: Int): Unit = {
      val joinName = joinType.replace(" ", "_")
      var leftTable = "(select * from depts where deptno > 50 or deptno < 5) d"
      var rightTable = "emps e"
      joinType match {
        case "RIGHT JOIN" =>
          leftTable = "emps e"
          rightTable = "(select * from depts where deptno > 50 or deptno < 5) d"
        case "LEFT JOIN" | "SEMI JOIN" | "ANTI JOIN" =>
        case _ =>
      }
      val leftAlias = leftTable.split(" ").last
      val sql =
        s"""
           |SELECT d.deptno
           |FROM  locations l JOIN
           |${leftTable} ON l.locationid = ${leftAlias}.deptno
           |${joinType} ${rightTable}
           |ON e.deptno=d.deptno
           |where d.deptno >= 40 OR d.deptno < 2;
           |""".stripMargin
      compareNotRewriteAndRows(sql, noData = true)
    }

    runOuterJoinFunc($1)(1)
  }

  test("clean_project_outJoin_view_1") {
    def $1(joinType: String, viewNumber: Int): Unit = {
      val joinName = joinType.replace(" ", "_")
      spark.sql(
        s"""
           |DROP MATERIALIZED VIEW IF EXISTS ${joinName}_${viewNumber};
           |""".stripMargin
      )
    }

    runOuterJoinFunc($1)(1)
  }
}
