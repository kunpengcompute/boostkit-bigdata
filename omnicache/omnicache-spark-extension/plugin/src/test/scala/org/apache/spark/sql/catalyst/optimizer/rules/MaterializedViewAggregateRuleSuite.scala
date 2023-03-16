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
        |avg(c.decimaltype) as _avg
        |FROM column_type c
        |GROUP BY c.empid,c.deptno,c.locationid;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_agg1", noData = false)
  }

  test("mv_agg1_2") {
    // group by column is same to view,additional agg on column in view
    val sql =
      """
        |SELECT c.empid,c.deptno,c.locationid,sum(c.empid) as _sum
        |FROM column_type c
        |GROUP BY c.empid,c.deptno,c.locationid;
        |""".stripMargin
    compareNotRewriteAndRows(sql, noData = false)
  }

  test("mv_agg1_3") {
    // group by column is same to view,additional agg on column not in view
    val sql =
      """
        |SELECT c.empid,c.deptno,c.locationid,sum(c.bytetype) as _sum
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
        |count(c.doubletype) as _count
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
    // group by column(is empty) is subset of view,additional agg on view column
    val sql =
      """
        |SELECT sum(c.integertype) as _sum,
        |max(c.longtype) as _max,min(c.floattype) as _min,
        |count(c.doubletype) as _count
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
        |avg(c.decimaltype) as _avg
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
        |count(c.doubletype) as _count
        |FROM column_type c JOIN emps e
        |ON c.empid=e.empid
        |AND c.empid=1
        |GROUP BY c.empid,c.deptno;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_agg4", noData = false)
  }

  test("mv_agg4_3") {
    // group by column,agg is same to view, join more
    val sql =
      """
        |SELECT c.empid,c.deptno,c.locationid,sum(c.integertype) as _sum,
        |max(c.longtype) as _max,min(c.floattype) as _min,
        |count(c.doubletype) as _count,count(distinct c.datetype) as _count_distinct,
        |avg(c.decimaltype) as _avg
        |FROM column_type c JOIN emps e JOIN locations l
        |ON c.empid=e.empid
        |AND c.locationid=l.locationid
        |AND c.empid=1
        |GROUP BY c.empid,c.deptno,c.locationid;
        |""".stripMargin
    compareNotRewriteAndRows(sql, noData = false)
  }

  test("mv_agg4_disable") {
    val sql = "ALTER MATERIALIZED VIEW mv_agg4 DISABLE REWRITE;"
    spark.sql(sql).show()
  }

  test("mv_agg5") {
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg5;
        |""".stripMargin
    )
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg5
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

  test("mv_agg5_1") {
    val sql =
      """
        |SELECT c.empid,c.deptno,sum(c.integertype) as _sum,
        |max(c.longtype) as _max,min(c.floattype) as _min,
        |count(c.doubletype) as _count
        |FROM column_type c JOIN emps e
        |ON c.empid=e.empid
        |AND c.empid=1
        |GROUP BY c.empid,c.deptno;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_agg5", noData = false)
  }

  test("mv_agg5_2") {
    val sql =
      """
        |SELECT c.empid,c.deptno,count(distinct c.datetype) as _count_distinct
        |FROM column_type c JOIN emps e
        |ON c.empid=e.empid
        |AND c.empid=1
        |GROUP BY c.empid,c.deptno;
        |""".stripMargin
    compareNotRewriteAndRows(sql, noData = false)
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg5;
        |""".stripMargin
    )
  }

  test("mv_agg6") {
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg6;
        |""".stripMargin
    )
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg6
        |AS
        |SELECT c.empid,c.deptno,c.locationid,sum(distinct c.integertype) as _sum,
        |max(distinct c.longtype) as _max,min(distinct c.floattype) as _min,
        |count(distinct c.doubletype) as _count
        |FROM column_type c
        |GROUP BY c.empid,c.deptno,c.locationid;
        |""".stripMargin
    )

    val sql =
      """
        |SELECT c.empid,c.deptno,sum(distinct c.integertype) as _sum,
        |max(distinct c.longtype) as _max,min(distinct c.floattype) as _min,
        |count(distinct c.doubletype) as _count
        |FROM column_type c
        |GROUP BY c.empid,c.deptno;
        |""".stripMargin
    compareNotRewriteAndRows(sql, noData = false)
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg6;
        |""".stripMargin
    )
  }

  test("mv_agg7_1") {
    // join compensate,no rollUp
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg7;
        |""".stripMargin
    )
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg7
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

    val sql =
      """
        |SELECT c.empid,c.deptno,c.locationid,sum(c.integertype) as _sum,
        |max(c.longtype) as _max,min(c.floattype) as _min,
        |count(c.doubletype) as _count
        |FROM column_type c JOIN emps e JOIN locations l
        |ON c.empid=e.empid AND c.locationid=l.locationid
        |AND c.empid=1
        |GROUP BY c.empid,c.deptno,c.locationid;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_agg7", noData = false)
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg7;
        |""".stripMargin
    )
  }

  test("mv_agg7_2") {
    // join compensate, has distinct agg
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg7;
        |""".stripMargin
    )
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg7
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

    val sql =
      """
        |SELECT c.empid,c.deptno,c.locationid,sum(c.integertype) as _sum,
        |max(c.longtype) as _max,min(c.floattype) as _min,
        |count(c.doubletype) as _count,count(distinct c.datetype) as _count_distinct
        |FROM column_type c JOIN emps e JOIN locations l
        |ON c.empid=e.empid AND c.locationid=l.locationid
        |AND c.empid=1
        |GROUP BY c.empid,c.deptno,c.locationid;
        |""".stripMargin
    compareNotRewriteAndRows(sql, noData = false)
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg7;
        |""".stripMargin
    )
  }

  test("mv_agg7_3") {
    // join compensate, has cannot rollUp agg
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg7;
        |""".stripMargin
    )
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg7
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

    val sql =
      """
        |SELECT c.empid,c.deptno,c.locationid,sum(c.integertype) as _sum,
        |max(c.longtype) as _max,min(c.floattype) as _min,
        |count(c.doubletype) as _count,avg(c.decimaltype) as _avg
        |FROM column_type c JOIN emps e JOIN locations l
        |ON c.empid=e.empid AND c.locationid=l.locationid
        |AND c.empid=1
        |GROUP BY c.empid,c.deptno,c.locationid;
        |""".stripMargin
    compareNotRewriteAndRows(sql, noData = false)
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg7;
        |""".stripMargin
    )
  }

  test("mv_agg8_1") {
    // Aggregation hence(The group by field is different):
    // min(distinct ) / max(distinct )
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg8_1;
        |""".stripMargin
    )
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg8_1
        |AS
        |SELECT
        |c.deptno,
        |c.locationid,
        |max(c.longtype) as _max,
        |min(c.floattype) as _min
        |FROM column_type c
        |GROUP BY c.empid,c.deptno,c.locationid;
        |""".stripMargin
    )
    val sql =
      """
        |SELECT
        |max(c.longtype) as _max,
        |min(c.floattype) as _min
        |FROM column_type c
        |GROUP BY c.deptno,c.locationid;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_agg8_1", noData = false)
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg8_1;
        |""".stripMargin
    )
  }

  test("mv_agg8_2") {
    // Aggregation hence(The group by field is different):
    // avg()
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg8_2;
        |""".stripMargin
    )
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg8_2
        |AS
        |SELECT
        |c.deptno,
        |c.locationid,
        |avg(c.longtype) as _avg,
        |count(c.longtype) as _count
        |FROM column_type c
        |GROUP BY c.empid,c.deptno,c.locationid;
        |""".stripMargin
    )
    val sql =
      """
        |SELECT
        |avg(c.longtype) as _avg
        |FROM column_type c
        |GROUP BY c.deptno,c.locationid;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_agg8_2", noData = false)
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg8_2;
        |""".stripMargin
    )
  }

  // min(distinct)/max(distinct)/avg() enhance
  test("mv_agg9") {
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg9;
        |""".stripMargin
    )
    spark.sql(
      """
        |CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg9
        |AS
        |SELECT c.empid,c.deptno,c.locationid,
        |min(distinct c.integertype) as _min_dist,
        |max(distinct c.longtype) as _max_dist,
        |count(c.decimaltype) as _count,
        |avg(c.decimaltype) as _avg
        |FROM column_type c JOIN emps e
        |ON c.empid=e.empid
        |AND c.empid=1
        |GROUP BY c.empid,c.deptno,c.locationid;
        |""".stripMargin
    )
  }

  test("mv_agg9_1") {
    val sql =
      """
        |SELECT c.empid,c.deptno,
        |min(distinct c.integertype) as _min_dist,
        |max(distinct c.longtype) as _max_dist,
        |count(c.decimaltype) as _count,
        |avg(c.decimaltype) as _avg
        |FROM column_type c JOIN emps e
        |ON c.empid=e.empid
        |AND c.empid=1
        |GROUP BY c.empid,c.deptno;
        |""".stripMargin
    comparePlansAndRows(sql, "default", "mv_agg9", noData = true)
  }

  test("drop_mv_agg9") {
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg9;
        |""".stripMargin
    )
  }

  test("drop all mv") {
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg1;
        |""".stripMargin
    )
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg2;
        |""".stripMargin
    )
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg3;
        |""".stripMargin
    )
    spark.sql(
      """
        |DROP MATERIALIZED VIEW IF EXISTS mv_agg4;
        |""".stripMargin
    )
  }
}
