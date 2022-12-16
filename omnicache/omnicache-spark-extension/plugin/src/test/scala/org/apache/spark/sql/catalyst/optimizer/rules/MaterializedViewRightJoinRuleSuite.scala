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

class MaterializedViewRightJoinRuleSuite extends RewriteSuite {

  test("mv_right_join") {
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_right_join;
        |""".stripMargin
    )
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_right_join
        |AS
        |SELECT e.*,d.deptname
        |FROM depts d RIGHT JOIN emps e
        |ON e.deptno=d.deptno where e.deptno >= 2;
        |""".stripMargin
    )
  }

  test("mv_right_join_1") {
    // is same to view
    val sql =
      """
        |SELECT e.*,d.deptname
        |FROM depts d RIGHT JOIN emps e
        |ON e.deptno=d.deptno where e.deptno >= 2;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_right_join", noData = false)
  }

  test("mv_right_join_2") {
    // view tables is subset of query
    val sql =
      """
        |SELECT e.*,d.deptname, l.locationid
        |FROM depts d RIGHT JOIN emps e ON e.deptno=d.deptno JOIN locations l
        |ON e.locationid=l.locationid where e.deptno >= 2;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_right_join", noData = false)
  }

  test("mv_right_join_3") {
    // view tables is subset of query
    val sql =
      """
        |SELECT e.*,d.deptname, l.locationid
        |FROM depts d RIGHT JOIN emps e ON e.deptno=d.deptno JOIN locations l
        |ON e.locationid=l.locationid where e.deptno = 5;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_right_join", noData = true)
  }

  test("mv_right_join_cannot_rewrite") {
    val sql =
      """
        |SELECT e1.*,d.deptname,e2.*
        |FROM depts d RIGHT JOIN emps e1 ON e1.deptno=d.deptno JOIN emps e2
        |on d.deptno = e2.deptno where e1.deptno >= 2;
        |""".stripMargin
    val df = spark.sql(sql)
    val optPlan = df.queryExecution.optimizedPlan
    disableCachePlugin()
    val df2 = spark.sql(sql)
    val srcPlan = df2.queryExecution.optimizedPlan
    enableCachePlugin()
    assert(optPlan.toString().replaceAll("#\\d+", "")
        .equals(srcPlan.toString().replaceAll("#\\d+", "")))
  }

  test("mv_right_join_4") {
    // view tables is subset of query, join with subquery
    val sql =
      """
        |SELECT v1.*,l.locationid
        |FROM
        |(SELECT e.*,d.deptname
        |FROM depts d RIGHT JOIN emps e
        |ON e.deptno=d.deptno where e.deptno >= 2
        |) v1
        |JOIN locations l
        |ON v1.locationid=l.locationid;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_right_join", noData = false)
  }

  test("mv_right_join_5") {
    // view tables is same to query, equal columns
    val sql =
      """
        |SELECT d.deptname
        |FROM depts d RIGHT JOIN emps e
        |ON e.deptno=d.deptno where e.deptno >= 2;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_right_join", noData = false)
  }

  test("right_join_range1") {
    // where的条件范围比视图大，不能重写
    val sql =
      """
        |SELECT e.*,d.deptname, l.locationid
        |FROM depts d RIGHT JOIN emps e ON e.deptno=d.deptno JOIN locations l
        |ON e.locationid=l.locationid where e.deptno > 0;
        |""".stripMargin
    val df = spark.sql(sql)
    val optPlan = df.queryExecution.optimizedPlan
    disableCachePlugin()
    val df2 = spark.sql(sql)
    val srcPlan = df2.queryExecution.optimizedPlan
    enableCachePlugin()
    assert(optPlan.toString().replaceAll("#\\d+", "")
        .equals(srcPlan.toString().replaceAll("#\\d+", "")))
  }

  test("right_join_range2") {
    // where的条件范围比视图小，可以重写
    val sql =
      """
        |SELECT e.*,d.deptname, l.locationid
        |FROM depts d RIGHT JOIN emps e ON e.deptno=d.deptno JOIN locations l
        |ON e.locationid=l.locationid where e.deptno > 2;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_right_join", noData = true)
  }

  test("clean_env") {
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_right_join;
        |""".stripMargin
    )
  }
}
