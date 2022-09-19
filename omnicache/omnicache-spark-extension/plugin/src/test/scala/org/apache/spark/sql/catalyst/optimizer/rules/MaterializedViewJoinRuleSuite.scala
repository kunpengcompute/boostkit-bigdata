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

import com.huawei.boostkit.spark.util.RewriteHelper

class MaterializedViewJoinRuleSuite extends RewriteSuite {

  test("mv_join1") {
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_join1;
        |""".stripMargin
    )
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_join1
        |AS
        |SELECT e.*,d.deptname
        |FROM emps e JOIN depts d
        |ON e.deptno=d.deptno;
        |""".stripMargin
    )
  }

  test("mv_join1_1") {
    // is same to view
    val sql =
      """
        |SELECT e.*,d.deptname
        |FROM emps e JOIN depts d
        |ON e.deptno=d.deptno;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_join1", noData = false)
  }

  test("mv_join1_2") {
    // is same to view, join order different
    val sql =
      """
        |SELECT e.*,d.deptname
        |FROM depts d JOIN emps e
        |ON e.deptno=d.deptno;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_join1", noData = false)
  }

  test("mv_join1_3") {
    // view tables is subset of query
    val sql =
      """
        |SELECT e.*,d.deptname,l.locationid
        |FROM emps e JOIN depts d JOIN locations l
        |ON e.deptno=d.deptno AND e.locationid=l.locationid;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_join1", noData = false)
  }

  test("mv_join1_4") {
    // view tables is subset of query, join order different
    val sql =
      """
        |SELECT e.*,d.deptname,l.locationid
        |FROM depts d JOIN locations l JOIN emps e
        |ON e.deptno=d.deptno AND e.locationid=l.locationid;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_join1", noData = false)
  }

  test("mv_join1_5") {
    // view tables is subset of query, join with subquery
    val sql =
      """
        |SELECT v1.*,l.locationid
        |FROM
        |(SELECT e.*,d.deptname
        |FROM emps e JOIN depts d
        |ON e.deptno=d.deptno
        |) v1
        |JOIN locations l
        |ON v1.locationid=l.locationid;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_join1", noData = false)
  }

  test("mv_join1_6") {
    // view tables is subset of query, join with subquery2
    val sql =
      """
        |SELECT v1.*,d.deptname
        |FROM
        |(SELECT e.*,l.locationid
        |FROM emps e JOIN locations l
        |ON e.locationid=l.locationid
        |) v1
        |JOIN depts d
        |ON v1.deptno=d.deptno;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_join1", noData = false)
  }

  test("mv_join1_7") {
    // view tables is same to query, equal columns
    val sql =
      """
        |SELECT d.deptno
        |FROM emps e JOIN depts d
        |ON e.deptno=d.deptno;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_join1", noData = false)
  }

  test("mv_join1_disable") {
    val sql = "ALTER MATERIALIZED VIEW mv_join1 DISABLE REWRITE;"
    spark.sql(sql).show()
  }

  test("mv_join2") {
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_join2;
        |""".stripMargin
    )
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_join2
        |AS
        |SELECT e.*,c1.stringtype
        |FROM emps e JOIN column_type c1
        |ON e.deptno=c1.deptno
        |AND c1.deptno=1;
        |""".stripMargin
    )
  }

  test("mv_join2_1") {
    // view tables is same to query, equal tables
    val sql =
      """
        |SELECT e.*,c2.stringtype
        |FROM emps e JOIN column_type c1 JOIN column_type c2
        |ON e.deptno=c1.deptno AND e.deptno=c2.deptno
        |AND c1.deptno!=2
        |AND c2.deptno=1;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_join2", noData = false)
    RewriteHelper.enableCachePlugin()
    comparePlansAndRows(sql, "default", "mv_join2", noData = false)
  }

  test("mv_join2_disable") {
    val sql = "ALTER MATERIALIZED VIEW mv_join2 DISABLE REWRITE;"
    spark.sql(sql).show()
  }
}
