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

package org.apache.spark.sql.catalyst.optimizer.rule

class MaterializedViewAggregateRuleSuite extends RewriteSuite {

  test("mv_agg1") {
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg1;
        |""".stripMargin
    )
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg1
        |AS
        |SELECT c.empid,c.deptno,c.locationid,sum(c.integertype) as _sum,
        |max(c.longtype) as _max,min(c.floattype) as _min,
        |count(c.doubletype) as _count,count(distinct c.datetype) as _count_distinct,
        |avg(c.decimaltype) as _avg
        |FROM column_type c
        |GROUP BY c.empid,c.deptno,c.locationid;
        |""".stripMargin
    )
  }

  test("mv_agg1_1") {
    // group by column,agg is same to view,additional agg on view column
    val sql =
      """
        |SELECT c.empid,c.deptno,c.locationid,sum(c.integertype) as _sum,
        |max(c.longtype) as _max,min(c.floattype) as _min,
        |count(c.doubletype) as _count,count(distinct c.datetype) as _count_distinct,
        |avg(c.decimaltype) as _avg,
        |max(c.deptno) as _max2,min(c.deptno) as _min2,
        |count(c.deptno) as _count2,count(distinct c.deptno) as _count_distinct2,
        |avg(c.deptno) as _avg2
        |FROM column_type c
        |GROUP BY c.empid,c.deptno,c.locationid;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_agg1", noData = false)
  }

  test("mv_agg1_2") {
    // group by column is same to view,additional agg on column not in view
    val sql =
      """
        |SELECT c.empid,c.deptno,c.locationid,sum(c.bytetype) as _sum,
        |FROM column_type c
        |GROUP BY c.empid,c.deptno,c.locationid;
        |""".stripMargin
    compareNotRewriteAndRows(sql, noData = false)
  }

  test("mv_agg1_disable") {
    val sql = "ALTER MATERIALIZED VIEW mv_agg1 DISABLE REWRITE;"
    spark.sql(sql).show()
  }

  test("mv_agg2") {
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg2;
        |""".stripMargin
    )
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg2
        |AS
        |SELECT sum(c.integertype) as _sum,
        |max(c.longtype) as _max,min(c.floattype) as _min,
        |count(c.doubletype) as _count,count(distinct c.datetype) as _count_distinct,
        |avg(c.decimaltype) as _avg
        |FROM column_type c;
        |""".stripMargin
    )
  }

  test("mv_agg2_1") {
    // group by column(is empty),agg is same to view
    val sql =
      """
        |SELECT sum(c.integertype) as _sum,
        |max(c.longtype) as _max,min(c.floattype) as _min,
        |count(c.doubletype) as _count,count(distinct c.datetype) as _count_distinct,
        |avg(c.decimaltype) as _avg
        |FROM column_type c;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_agg2", noData = false)
  }

  test("mv_agg2_disable") {
    val sql = "ALTER MATERIALIZED VIEW mv_agg2 DISABLE REWRITE;"
    spark.sql(sql).show()
  }

  test("mv_agg3") {
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg3;
        |""".stripMargin
    )
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg3
        |AS
        |SELECT c.empid,c.deptno,c.locationid,sum(c.integertype) as _sum,
        |max(c.longtype) as _max,min(c.floattype) as _min,
        |count(c.doubletype) as _count,count(distinct c.datetype) as _count_distinct,
        |avg(c.decimaltype) as _avg
        |FROM column_type c
        |GROUP BY c.empid,c.deptno,c.locationid;
        |""".stripMargin
    )
  }

  test("mv_agg3_1") {
    // group by column,agg is subset of view,additional agg on view column
    val sql =
      """
        |SELECT c.empid,c.deptno,sum(c.integertype) as _sum,
        |max(c.longtype) as _max,min(c.floattype) as _min,
        |count(c.doubletype) as _count,
        |max(c.deptno) as _max2,min(c.deptno) as _min2,
        |count(c.deptno) as _count2,count(distinct c.deptno) as _count_distinct2,
        |avg(c.deptno) as _avg2
        |FROM column_type c
        |GROUP BY c.empid,c.deptno;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_agg3", noData = false)
  }

  test("mv_agg3_2") {
    // group by column is subset of view,using non-rollup agg
    val sql =
      """
        |SELECT c.empid,c.deptno,avg(c.decimaltype) as _avg
        |FROM column_type c
        |GROUP BY c.empid,c.deptno;
        |""".stripMargin
    compareNotRewriteAndRows(sql, noData = false)
  }

  test("mv_agg3_3") {
    // group by column(is empty) is subset of view,,additional agg on view column
    val sql =
      """
        |SELECT sum(c.integertype) as _sum,
        |max(c.longtype) as _max,min(c.floattype) as _min,
        |count(c.doubletype) as _count,
        |max(c.deptno) as _max2,min(c.deptno) as _min2,
        |count(c.deptno) as _count2,count(distinct c.deptno) as _count_distinct2,
        |avg(c.deptno) as _avg2
        |FROM column_type c ;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_agg3", noData = false)
  }

  test("mv_agg3_disable") {
    val sql = "ALTER MATERIALIZED VIEW mv_agg3 DISABLE REWRITE;"
    spark.sql(sql).show()
  }


  test("mv_agg4") {
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg4;
        |""".stripMargin
    )
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg4
        |AS
        |SELECT c.empid,c.deptno,c.locationid,sum(c.integertype) as _sum,
        |max(c.longtype) as _max,min(c.floattype) as _min,
        |count(c.doubletype) as _count,count(distinct c.datetype) as _count_distinct,
        |avg(c.decimaltype) as _avg
        |FROM column_type c JOIN emps e
        |ON c.empid=e.empid
        |AND c.empid=1
        |GROUP BY c.empid,c.deptno,c.locationid;
        |""".stripMargin
    )
  }

  test("mv_agg4_1") {
    // group by column,agg is same to view,additional agg on view column
    val sql =
      """
        |SELECT c.empid,c.deptno,c.locationid,sum(c.integertype) as _sum,
        |max(c.longtype) as _max,min(c.floattype) as _min,
        |count(c.doubletype) as _count,count(distinct c.datetype) as _count_distinct,
        |avg(c.decimaltype) as _avg,
        |max(c.deptno) as _max2,min(c.deptno) as _min2,
        |count(c.deptno) as _count2,count(distinct c.deptno) as _count_distinct2,
        |avg(c.deptno) as _avg2
        |FROM column_type c JOIN emps e
        |ON c.empid=e.empid
        |AND c.empid=1
        |GROUP BY c.empid,c.deptno,c.locationid;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_agg4", noData = false)
  }

  test("mv_agg4_2") {
    // group by column,agg is subset of view,additional agg on view column
    val sql =
      """
        |SELECT c.empid,c.deptno,sum(c.integertype) as _sum,
        |max(c.longtype) as _max,min(c.floattype) as _min,
        |count(c.doubletype) as _count,
        |max(c.deptno) as _max2,min(c.deptno) as _min2,
        |count(c.deptno) as _count2,count(distinct c.deptno) as _count_distinct2,
        |avg(c.deptno) as _avg2
        |FROM column_type c JOIN emps e
        |ON c.empid=e.empid
        |AND c.empid=1
        |GROUP BY c.empid,c.deptno;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_agg4", noData = false)
  }

  test("mv_agg4_disable") {
    val sql = "ALTER MATERIALIZED VIEW mv_agg4 DISABLE REWRITE;"
    spark.sql(sql).show()
  }
}
